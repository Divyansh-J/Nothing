import json
import uuid
import xml.etree.ElementTree as ET
import os
import re
import logging
from typing import Dict, List, Set, Tuple, Union, Optional
import argparse
from concurrent.futures import ThreadPoolExecutor
import glob
from datetime import datetime
import csv
import sys
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("converter.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("twb_to_bim")

def generate_lineage_tag() -> str:
    """Generate a unique lineage tag for Power BI model elements."""
    return str(uuid.uuid4())

def get_format_string(measure_name: str, dax_formula: str) -> str:
    """
    Determine appropriate format string based on measure characteristics.
    
    Args:
        measure_name: The name of the measure
        dax_formula: The DAX formula of the measure
        
    Returns:
        A Power BI format string
    """
    name_lower = measure_name.lower()
    
    # Check for percentage-related measures
    if any(word in name_lower for word in ['ratio', 'efficiency', 'percentage', 'percent', '%']):
        return "0.00\\%"
        
    # Check for averages
    if any(word in name_lower for word in ['avg', 'average', 'mean']):
        return "0.00"
        
    # Check for date/time measures
    if 'date' in name_lower or 'time' in name_lower:
        return "General Date"
        
    # Check for currency
    if any(word in name_lower for word in ['cost', 'price', 'revenue', 'sales', 'amount', '$', 'dollar']):
        return "\\$#,0.00;(\\$#,0.00);\\$#,0.00"
        
    # Check for counts
    if any(word in name_lower for word in ['count', 'num', 'quantity', 'qty']):
        return "#,0"
        
    # Check formula for functions that suggest formatting
    if "DIVIDE" in dax_formula or "/" in dax_formula:
        return "0.00"
        
    # Default to regular number format
    return "0"

def fix_table_references(dax_formula: str) -> str:
    """
    Fix common table reference issues in DAX formulas.
    
    Args:
        dax_formula: The original DAX formula
        
    Returns:
        Corrected DAX formula with proper table references
    """
    # Fix known typos and standardize table names
    replacements = {
        "project_assignents": "project_assignments",
        "Projects": "projects",
        "pro[": "project_assignments[", 
        "YourTable": "project_assignments",
        "'YourTable'": "'project_assignments'",
    }
    
    for old, new in replacements.items():
        dax_formula = dax_formula.replace(old, new)
    
    # Fix common syntax errors in table references
    if "projects]" in dax_formula and "[projects]" not in dax_formula:
        dax_formula = dax_formula.replace("projects]", "[projects]")
    
    # Fix naked column references (missing table name)
    naked_columns_pattern = r'(?<!\w)(?<!\')(?<!\[)\[([^\]]+)\](?!\])'
    if re.search(naked_columns_pattern, dax_formula):
        dax_formula = re.sub(naked_columns_pattern, r"'project_assignments'[\1]", dax_formula)
    
    # Fix cases where table name is mentioned but bracket syntax is wrong
    if re.search(r"[a-zA-Z_]+\[", dax_formula):
        dax_formula = re.sub(r"([a-zA-Z_]+)\[", r"'\1'[", dax_formula)
        
    return dax_formula

def clean_dax_expression(dax_formula: str) -> str:
    """
    Clean and fix common DAX formula issues.
    
    Args:
        dax_formula: The original DAX formula
        
    Returns:
        Cleaned and corrected DAX formula
    """
    if not dax_formula:
        return ""
        
    # Fix table references
    dax_formula = fix_table_references(dax_formula)
    
    # Fix DIVIDE syntax - ensure it has 3 arguments (typically missing the 0 for division by zero)
    if "DIVIDE(" in dax_formula:
        # Count arguments in DIVIDE function
        divide_pattern = r'DIVIDE\s*\(([^,]+),([^,\)]+)(?:,([^,\)]+))?\)'
        matches = re.findall(divide_pattern, dax_formula)
        
        for match in matches:
            if len(match) == 2 or not match[2].strip():  # Missing third argument
                original = f"DIVIDE({match[0]},{match[1]})"
                replacement = f"DIVIDE({match[0]},{match[1]}, 0)"
                dax_formula = dax_formula.replace(original, replacement)
    
    # Fix reference to Budget column
    if "projects]" in dax_formula and "projects[Budget]" not in dax_formula:
        dax_formula = dax_formula.replace("projects]", "projects[Budget]")
    
    # Fix common formatting issues
    dax_formula = dax_formula.replace("  ", " ")  # Remove double spaces
    
    return dax_formula

def extract_table_names_from_twb(root: ET.Element) -> List[str]:
    """
    Extract unique table names from Tableau workbook XML.
    
    Args:
        root: The XML root element
        
    Returns:
        List of unique table names
    """
    table_names = set()  # Use set to ensure uniqueness
    
    # Look for table names in relations
    for relation in root.findall(".//relation[@name]"):
        table_name = relation.get("name")
        if table_name:
            # Remove file extension if present
            table_name = os.path.splitext(table_name)[0]
            table_names.add(table_name)
    
    # Also look in column references
    for column in root.findall(".//column"):
        table_refs = column.findall(".//relation-ref")
        for ref in table_refs:
            name = ref.get("name")
            if name:
                table_name = os.path.splitext(name)[0]
                table_names.add(table_name)
    
    return list(table_names)

def extract_columns_from_twb(root: ET.Element, table_name: str) -> List[Dict]:
    """
    Extract column definitions from a specific table with proper metadata.
    
    Args:
        root: The XML root element
        table_name: The name of the table to extract columns from
        
    Returns:
        List of column definitions with Power BI metadata
    """
    columns = []
    seen_columns = set()  # Track columns we've already processed
    
    # Find column elements for this table
    for column_element in root.findall(f".//relation[@name='{table_name}.csv']/columns/column"):
        name = column_element.get("name")
        
        # Skip if we've already processed this column
        if name in seen_columns:
            continue
            
        seen_columns.add(name)
        datatype = column_element.get("datatype", "").lower()
        
        # Map Tableau datatypes to Power BI datatypes and format strings
        pb_datatype = "string"  # Default type
        format_string = None
        summarize_by = "none"
        
        # Determine type based on column name and datatype
        if datatype == "integer" or name.lower().endswith('id') or "id_" in name.lower():
            pb_datatype = "int64"
            format_string = "0"
            summarize_by = "none" if any(id_pattern in name.lower() for id_pattern in ["id", "key", "code"]) else "sum"
        elif datatype == "real" or any(word in name.lower() for word in ["amount", "price", "cost", "budget", "rating", "value"]):
            pb_datatype = "double"
            format_string = "0.00"
            summarize_by = "sum"
        elif datatype == "date" or any(word in name.lower() for word in ["date", "time", "day", "month", "year"]):
            pb_datatype = "dateTime"
            format_string = "Long Date"
            summarize_by = "none"
        elif datatype == "boolean" or name.lower() in ["active", "enabled", "status", "flag"]:
            pb_datatype = "boolean"
            summarize_by = "none"
            
        # Create column with proper metadata
        column = {
            "name": name,
            "dataType": pb_datatype,
            "sourceColumn": name,
            "lineageTag": generate_lineage_tag(),
            "summarizeBy": summarize_by,
            "annotations": [
                {
                    "name": "SummarizationSetBy",
                    "value": "Automatic"
                }
            ]
        }
        
        # Add format string if needed
        if format_string:
            column["formatString"] = format_string
            
        # For date columns, add variations for date hierarchies
        if pb_datatype == "dateTime":
            date_table_id = generate_lineage_tag().replace('-', '_')
            column["variations"] = [
                {
                    "name": "Variation",
                    "relationship": generate_lineage_tag(),
                    "defaultHierarchy": {
                        "table": f"LocalDateTable_{date_table_id}",
                        "hierarchy": "Date Hierarchy"
                    },
                    "isDefault": True
                }
            ]
            column["annotations"].append({
                "name": "UnderlyingDateTimeDataType",
                "value": "Date"
            })
            
        columns.append(column)
    
    # If we couldn't find columns, try to infer them from calculations or parameters
    if not columns:
        columns = infer_columns_from_calculations(root, table_name)
    
    return columns

def infer_columns_from_calculations(root: ET.Element, table_name: str) -> List[Dict]:
    """
    Infer columns from calculations when direct column info is not available.
    
    Args:
        root: The XML root element
        table_name: The table name to associate columns with
        
    Returns:
        List of inferred column definitions
    """
    columns = []
    seen_columns = set()
    
    # Look for column references in calculations
    for calc in root.findall(".//calculation"):
        formula = calc.get("formula", "")
        if not formula:
            continue
            
        # Find column references [column_name]
        col_matches = re.findall(r'\[([^\]]+)\]', formula)
        
        for col_name in col_matches:
            if col_name in seen_columns:
                continue
                
            seen_columns.add(col_name)
            
            # Try to determine the data type based on name
            pb_datatype = "string"  # Default type
            format_string = None
            summarize_by = "none"
            
            name_lower = col_name.lower()
            if name_lower.endswith('id') or "id_" in name_lower:
                pb_datatype = "int64"
                format_string = "0"
            elif any(word in name_lower for word in ["amount", "price", "cost", "budget"]):
                pb_datatype = "double"
                format_string = "0.00"
                summarize_by = "sum"
            elif any(word in name_lower for word in ["date", "time"]):
                pb_datatype = "dateTime"
                format_string = "Long Date"
            
            column = {
                "name": col_name,
                "dataType": pb_datatype,
                "sourceColumn": col_name,
                "lineageTag": generate_lineage_tag(),
                "summarizeBy": summarize_by,
                "annotations": [
                    {
                        "name": "SummarizationSetBy",
                        "value": "Automatic"
                    }
                ]
            }
            
            if format_string:
                column["formatString"] = format_string
                
            columns.append(column)
    
    return columns

def extract_relationships_from_twb(root: ET.Element, table_names: List[str]) -> List[Dict]:
    """
    Extract relationship definitions from Tableau workbook.
    
    Args:
        root: The XML root element
        table_names: List of table names to help with relationship inference
        
    Returns:
        List of relationship definitions
    """
    relationships = []
    seen_relationships = set()  # Track unique relationships
    
    try:
        # First try to find explicit relationships in joins
        for relationship in root.findall(".//relation[@type='join']"):
            try:
                clause = relationship.get("join", "")
                if not clause:
                    continue
                
                parts = clause.split("=")
                if len(parts) != 2:
                    continue
                
                left_part = parts[0].strip()
                right_part = parts[1].strip()
                
                # Extract table and column names
                left_match = re.match(r"\[([^\]]+)\]\.\[([^\]]+)\]", left_part)
                right_match = re.match(r"\[([^\]]+)\]\.\[([^\]]+)\]", right_part)
                
                if left_match and right_match:
                    from_table = os.path.splitext(left_match.group(1))[0]
                    from_column = left_match.group(2)
                    to_table = os.path.splitext(right_match.group(1))[0]
                    to_column = right_match.group(2)
                    
                    # Skip self-referencing relationships
                    if from_table == to_table:
                        continue
                    
                    rel_key = f"{from_table}.{from_column}-{to_table}.{to_column}"
                    if rel_key not in seen_relationships:
                        seen_relationships.add(rel_key)
                        relationships.append({
                            "fromTable": from_table,
                            "fromColumn": from_column,
                            "toTable": to_table,
                            "toColumn": to_column
                        })
            except Exception as e:
                logger.warning(f"Could not process relationship: {e}")
                continue
        
        # If no relationships found, try to infer from column names
        if not relationships:
            # Try to infer from foreign key naming conventions
            inferred_relationships = infer_relationships_from_names(table_names, root)
            for rel in inferred_relationships:
                rel_key = f"{rel['fromTable']}.{rel['fromColumn']}-{rel['toTable']}.{rel['toColumn']}"
                if rel_key not in seen_relationships:
                    seen_relationships.add(rel_key)
                    relationships.append(rel)
                
    except Exception as e:
        logger.error(f"Error extracting relationships: {e}")
    
    return relationships

def infer_relationships_from_names(table_names: List[str], root: ET.Element) -> List[Dict]:
    """
    Infer relationships based on column naming conventions.
    
    Args:
        table_names: List of table names
        root: XML root element for extraction
        
    Returns:
        List of inferred relationships
    """
    relationships = []
    
    # Build a dictionary of tables and their columns
    table_columns = {}
    for table in table_names:
        columns = extract_columns_from_twb(root, table)
        table_columns[table] = [col["name"] for col in columns]
    
    # Look for foreign key patterns
    for from_table, from_columns in table_columns.items():
        for to_table in table_names:
            if from_table == to_table:
                continue
                
            # Check for <table>_id or <table>id columns
            for from_col in from_columns:
                # Check if this column references another table
                if to_table.lower() + "_id" == from_col.lower() or to_table.lower() + "id" == from_col.lower():
                    # Look for matching primary key in target table
                    for to_col in table_columns.get(to_table, []):
                        if to_col.lower() == "id":
                            relationships.append({
                                "fromTable": from_table,
                                "fromColumn": from_col,
                                "toTable": to_table,
                                "toColumn": to_col
                            })
                            break
    
    # Look for common ID columns
    for i, table1 in enumerate(table_names):
        cols1 = table_columns.get(table1, [])
        for table2 in table_names[i+1:]:
            cols2 = table_columns.get(table2, [])
            
            # Look for ID-based relationships
            for col1 in cols1:
                if "id" not in col1.lower():
                    continue
                    
                for col2 in cols2:
                    if col1 == col2 and "id" in col1.lower():
                        # Determine which is the "one" side (likely the shorter table name)
                        if len(table1) <= len(table2):
                            # table1 is likely the primary table
                            relationships.append({
                                "fromTable": table2,
                                "fromColumn": col2,
                                "toTable": table1,
                                "toColumn": col1
                            })
                        else:
                            relationships.append({
                                "fromTable": table1,
                                "fromColumn": col1,
                                "toTable": table2,
                                "toColumn": col2
                            })
                        break
    
    return relationships

def extract_calculations_from_twb(root: ET.Element) -> List[Dict]:
    """
    Extract calculated fields from Tableau workbook.
    
    Args:
        root: The XML root element
        
    Returns:
        List of calculation definitions
    """
    calculations = []
    seen_names = set()  # To avoid duplicates
    
    for column in root.findall(".//column"):
        calc = column.find("calculation")
        if calc is None:
            continue
            
        formula = calc.get("formula")
        name = column.get("caption", column.get("name", "Unnamed Calculation"))
        
        # Skip if we've seen this calculation before or if it has no formula
        if not formula or name in seen_names:
            continue
            
        seen_names.add(name)
        
        # Convert Tableau formula to DAX
        dax_formula = convert_tableau_to_dax(formula)
        
        # Determine if this is a measure or calculated column
        calc_type = determine_calculation_type(formula)
        
        calculations.append({
            "name": name,
            "expression": dax_formula,
            "type": calc_type
        })
    
    return calculations

def determine_calculation_type(formula: str) -> str:
    """
    Determine if a calculation is a measure or calculated column.
    
    Args:
        formula: The calculation formula
        
    Returns:
        'measure' or 'calculatedColumn'
    """
    # Look for aggregation functions that indicate it's a measure
    agg_functions = ["SUM(", "AVG(", "COUNT(", "MIN(", "MAX(", "COUNTD("]
    if any(agg.upper() in formula.upper() for agg in agg_functions):
        return "measure"
    else:
        return "calculatedColumn"

def convert_tableau_to_dax(formula: str) -> str:
    """
    Convert Tableau formula to DAX formula.
    
    Args:
        formula: The Tableau formula
        
    Returns:
        Converted DAX formula
    """
    dax = formula
    
    # Handle FIXED expressions
    if "{FIXED" in dax:
        # Convert FIXED to CALCULATE with ALLEXCEPT
        matches = re.findall(r"\{FIXED ([^:]+): ([^}]+)\}", dax)
        for match in matches:
            dimensions = match[0]
            expression = match[1]
            
            # Try to extract table and column names
            try:
                table_name = dimensions.split('[')[0].strip("'")
                column_name = dimensions.split('[')[1].strip(']')
                original = f"{{FIXED {dimensions}: {expression}}}"
                replacement = f"CALCULATE({expression}, ALLEXCEPT('{table_name}', '{table_name}'[{column_name}]))"
                dax = dax.replace(original, replacement)
            except:
                # If parsing fails, use a simpler approach
                original = f"{{FIXED {dimensions}: {expression}}}"
                replacement = f"CALCULATE({expression}, ALL())"
                dax = dax.replace(original, replacement)
    
    # Basic function replacements
    replacements = {
        "SUM(": "SUM(",
        "AVG(": "AVERAGE(",
        "COUNT(": "COUNT(",
        "COUNTD(": "DISTINCTCOUNT(",
        "MIN(": "MIN(",
        "MAX(": "MAX(",
        "MEDIAN(": "MEDIAN(",
        "ATTR(": "FIRSTNONBLANK(",
        "ZN(": "IFERROR(",
    }
    
    for tableau, dax_func in replacements.items():
        dax = dax.replace(tableau, dax_func)
    
    # Fix IF statements (Tableau uses THEN/ELSE/END, DAX just uses commas)
    if "IF " in dax:
        dax = re.sub(r"IF\s+([^\s]+)\s+THEN\s+([^\s]+)\s+ELSE\s+([^\s]+)\s+END", r"IF(\1, \2, \3)", dax)
    
    # Fix field references - this is a simplified approach
    dax = fix_table_references(dax)
    
    # Add DIVIDE third parameter if missing
    if "DIVIDE(" in dax:
        dax = clean_dax_expression(dax)
    
    # Replace IFERROR that's missing second parameter
    if "IFERROR(" in dax and not re.search(r"IFERROR\([^,]+,[^)]+\)", dax):
        dax = re.sub(r"IFERROR\(([^)]+)\)", r"IFERROR(\1, 0)", dax)
    
    return dax

def create_date_tables(tables_with_dates: List[Dict]) -> List[Dict]:
    """
    Create date tables for each date column.
    
    Args:
        tables_with_dates: List of table and column info for date columns
        
    Returns:
        List of date table definitions
    """
    date_tables = []
    
    # Create the template date table
    template_lineage_tag = generate_lineage_tag()
    template_table = {
        "name": f"DateTableTemplate_{template_lineage_tag.replace('-', '_')}",
        "isHidden": True,
        "isPrivate": True,
        "lineageTag": generate_lineage_tag(),
        "columns": [
            {
                "type": "calculatedTableColumn",
                "name": "Date",
                "dataType": "dateTime",
                "isNameInferred": True,
                "isHidden": True,
                "sourceColumn": "[Date]",
                "formatString": "General Date",
                "lineageTag": generate_lineage_tag(),
                "dataCategory": "PaddedDateTableDates",
                "summarizeBy": "none",
                "annotations": [
                    {
                        "name": "SummarizationSetBy",
                        "value": "User"
                    }
                ]
            },
            # Year column
            {
                "type": "calculated",
                "name": "Year",
                "dataType": "int64",
                "isHidden": True,
                "expression": "YEAR([Date])",
                "formatString": "0",
                "lineageTag": generate_lineage_tag(),
                "dataCategory": "Years",
                "summarizeBy": "none",
                "annotations": [
                    {
                        "name": "SummarizationSetBy",
                        "value": "User"
                    },
                    {
                        "name": "TemplateId",
                        "value": "Year"
                    }
                ]
            },
            # MonthNo column
            {
                "type": "calculated",
                "name": "MonthNo",
                "dataType": "int64",
                "isHidden": True,
                "expression": "MONTH([Date])",
                "formatString": "0",
                "lineageTag": generate_lineage_tag(),
                "dataCategory": "MonthOfYear",
                "summarizeBy": "none",
                "annotations": [
                    {
                        "name": "SummarizationSetBy",
                        "value": "User"
                    },
                    {
                        "name": "TemplateId",
                        "value": "MonthNumber"
                    }
                ]
            },
            # Month column
            {
                "type": "calculated",
                "name": "Month",
                "dataType": "string",
                "isHidden": True,
                "expression": "FORMAT([Date], \"MMMM\")",
                "sortByColumn": "MonthNo",
                "lineageTag": generate_lineage_tag(),
                "dataCategory": "Months",
                "summarizeBy": "none",
                "annotations": [
                    {
                        "name": "SummarizationSetBy",
                        "value": "User"
                    },
                    {
                        "name": "TemplateId",
                        "value": "Month"
                    }
                ]
            },
            # QuarterNo column
            {
                "type": "calculated",
                "name": "QuarterNo",
                "dataType": "int64",
                "isHidden": True,
                "expression": "INT(([MonthNo] + 2) / 3)",
                "formatString": "0",
                "lineageTag": generate_lineage_tag(),
                "dataCategory": "QuarterOfYear",
                "summarizeBy": "none",
                "annotations": [
                    {
                        "name": "SummarizationSetBy",
                        "value": "User"
                    },
                    {
                        "name": "TemplateId",
                        "value": "QuarterNumber"
                    }
                ]
            },
            # Quarter column
            {
                "type": "calculated",
                "name": "Quarter",
                "dataType": "string",
                "isHidden": True,
                "expression": "\"Qtr \" & [QuarterNo]",
                "sortByColumn": "QuarterNo",
                "lineageTag": generate_lineage_tag(),
                "dataCategory": "Quarters",
                "summarizeBy": "none",
                "annotations": [
                    {
                        "name": "SummarizationSetBy",
                        "value": "User"
                    },
                    {
                        "name": "TemplateId",
                        "value": "Quarter"
                    }
                ]
            },
            # Day column
            {
                "type": "calculated",
                "name": "Day",
                "dataType": "int64",
                "isHidden": True,
                "expression": "DAY([Date])",
                "formatString": "0",
                "lineageTag": generate_lineage_tag(),
                "dataCategory": "DayOfMonth",
                "summarizeBy": "none",
                "annotations": [
                    {
                        "name": "SummarizationSetBy",
                        "value": "User"
                    },
                    {
                        "name": "TemplateId",
                        "value": "Day"
                    }
                ]
            }
        ],
        "partitions": [
            {
                "name": f"DateTableTemplate_{template_lineage_tag.replace('-', '_')}",
                "mode": "import",
                "source": {
                    "type": "calculated",
                    "expression": "CALENDAR(DATE(2015,1,1), DATE(2030,12,31))"
                }
            }
        ],
        "hierarchies": [
            {
                "name": "Date Hierarchy",
                "lineageTag": generate_lineage_tag(),
                "levels": [
                    {
                        "name": "Year",
                        "ordinal": 0,
                        "column": "Year",
                        "lineageTag": generate_lineage_tag()
                    },
                    {
                        "name": "Quarter",
                        "ordinal": 1,
                        "column": "Quarter",
                        "lineageTag": generate_lineage_tag()
                    },
                    {
                        "name": "Month",
                        "ordinal": 2,
                        "column": "Month",
                        "lineageTag": generate_lineage_tag()
                    },
                    {
                        "name": "Day",
                        "ordinal": 3,
                        "column": "Day",
                        "lineageTag": generate_lineage_tag()
                    }
                ],
                "annotations": [
                    {
                        "name": "TemplateId",
                        "value": "DateHierarchy"
                    }
                ]
            }
        ],
        "annotations": [
            {
                "name": "__PBI_TemplateDateTable",
                "value": "true"
            },
            {
                "name": "DefaultItem",
                "value": "DateHierarchy"
            }
        ]
    }
    
    date_tables.append(template_table)
    
    # Create local date tables for each date column
    for table_info in tables_with_dates:
        table_name = table_info["table"]
        column_name = table_info["column"]
        local_date_table_id = table_info["date_table_id"]
        
        local_date_table = {
            "name": f"LocalDateTable_{local_date_table_id}",
            "isHidden": True,
            "showAsVariationsOnly": True,
            "lineageTag": generate_lineage_tag(),
            "columns": template_table["columns"].copy(),  # Copy columns from template
            "partitions": [
                {
                    "name": f"LocalDateTable_{local_date_table_id}",
                    "mode": "import",
                    "source": {
                        "type": "calculated",
                        "expression": f"Calendar(Date(Year(MIN('{table_name}'[{column_name}])), 1, 1), Date(Year(MAX('{table_name}'[{column_name}])), 12, 31))"
                    }
                }
            ],
            "hierarchies": template_table["hierarchies"].copy(),  # Copy hierarchies from template
            "annotations": [
                {
                    "name": "__PBI_LocalDateTable",
                    "value": "true"
                }
            ]
        }
        
        date_tables.append(local_date_table)
    
    return date_tables

def create_date_relationships(tables_with_dates: List[Dict]) -> List[Dict]:
    """
    Create relationships between date columns and their date tables.
    
    Args:
        tables_with_dates: List of table and column info for date columns
        
    Returns:
        List of relationship definitions
    """
    relationships = []
    
    for table_info in tables_with_dates:
        relationship = {
            "name": table_info["variation_relationship"],
            "fromTable": table_info["table"],
            "fromColumn": table_info["column"],
            "toTable": f"LocalDateTable_{table_info['date_table_id']}",
            "toColumn": "Date",
            "joinOnDateBehavior": "datePartOnly"
        }
        relationships.append(relationship)
    
    return relationships

def create_table_partition(table_name: str, columns: List[Dict], csv_path: str, embed_data: bool = False) -> Dict:
    """
    Create a table partition with file reference approach only.
    Data embedding has been removed as it's not a priority.
    
    Args:
        table_name: The name of the table
        columns: List of column definitions
        csv_path: Path to the CSV file
        embed_data: Parameter kept for backward compatibility but no longer used
        
    Returns:
        Partition definition with file reference
    """
    # Log what we're doing
    logger.info(f"Creating file reference partition for table {table_name}")
    logger.info(f"CSV path: {csv_path}")
    
    # Always use file reference approach
    csv_path_escaped = csv_path.replace("\\", "\\\\")
    
    return {
        "name": table_name,
        "mode": "import",
        "source": {
            "type": "m",
            "expression": [
                "let",
                f'    Source = Csv.Document(File.Contents("{csv_path_escaped}"),[Delimiter=",", Columns={len(columns)}, Encoding=65001, QuoteStyle=QuoteStyle.None]),',
                '    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true])',
                "in",
                '    #"Promoted Headers"'
            ]
        }
    }

def create_model_bim(twb_file_path: str, output_bim_path: str, embed_csv_data: bool = True, extract_measures: bool = False) -> bool:
    """
    Create a complete Model.bim file from a Tableau workbook.
    Data embedding has been removed as it's not a priority.
    
    Args:
        twb_file_path: Path to the Tableau workbook file
        output_bim_path: Path to save the generated Model.bim file
        embed_csv_data: Parameter kept for backward compatibility but no longer used
        extract_measures: If True, extract and include measures directly from the TWB file
                         If False, only table structure is created (measures will be added later from DAX file)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Parse the TWB file
        tree = ET.parse(twb_file_path)
        root = tree.getroot()
        
        # Extract table names
        table_names = extract_table_names_from_twb(root)
        logger.info(f"Extracted table names: {table_names}")
        
        # Initialize the model structure
        model = {
            "name": os.path.splitext(os.path.basename(output_bim_path))[0],
            "compatibilityLevel": 1550,
            "model": {
                "culture": "en-US",
                "dataAccessOptions": {
                    "legacyRedirects": True,
                    "returnErrorValuesAsNull": True
                },
                "defaultPowerBIDataSourceVersion": "powerBI_V3",
                "sourceQueryCulture": "en-US",
                "tables": [],
                "relationships": [],
                "annotations": [
                    {
                        "name": "TabularEditor_SerializeOptions",
                        "value": "{\n  \"IgnoreInferredObjects\": true,\n  \"IgnoreInferredProperties\": true,\n  \"IgnoreTimestamps\": true,\n  \"SplitMultilineStrings\": true,\n  \"PrefixFilenames\": false,\n  \"LocalTranslations\": false,\n  \"LocalPerspectives\": false,\n  \"LocalRelationships\": false,\n  \"Levels\": [\"Data Sources\", \"Shared Expressions\", \"Perspectives\", \"Relationships\", \"Roles\", \"Tables\", \"Tables/Columns\", \"Tables/Hierarchies\", \"Tables/Measures\", \"Tables/Partitions\", \"Tables/Calculation Items\", \"Translations\"]\n}"
                    }
                ]
            }
        }
        
        # Create tables without embedding data
        for table_name in table_names:
            # Extract columns for this table
            columns = extract_columns_from_twb(root, table_name)
            
            # Create table structure
            table = {
                "name": table_name,
                "columns": [],
                "partitions": [
                    {
                        "name": f"{table_name} Partition",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": f"let\n    Source = Csv.Document(File.Contents(\"{table_name}.csv\"),[Delimiter=\",\", Columns=25, QuoteStyle=QuoteStyle.Csv]),\n    #\"Promoted Headers\" = Table.PromoteHeaders(Source, [PromoteAllScalars=true])\nin\n    #\"Promoted Headers\""
                        }
                    }
                ],
                "measures": []
            }
            
            # Add columns
            for col in columns:
                column = {
                    "name": col["name"],
                    "dataType": col.get("dataType", "string"),
                    "sourceColumn": col["name"],
                    "lineageTag": generate_lineage_tag()
                }
                table["columns"].append(column)
            
            model["model"]["tables"].append(table)
        
        # Extract relationships
        relationships = extract_relationships_from_twb(root, table_names)
        if relationships:
            model["model"]["relationships"] = relationships
        
        # Extract measures if requested
        if extract_measures:
            calculations = extract_calculations_from_twb(root)
            for calc in calculations:
                if calc["type"] == "measure":
                    # Find the appropriate table for this measure using the improved logic
                    target_table = None
                    measure_base_tables = extract_measure_base_tables(root)
                    
                    # Try to get table from measure base tables
                    if calc["name"] in measure_base_tables:
                        base_table = measure_base_tables[calc["name"]]
                        target_table = next((t for t in model["model"]["tables"] if t["name"] == base_table), None)
                    
                    # If not found, try to infer from expression
                    if target_table is None:
                        for table in model["model"]["tables"]:
                            if table["name"] in calc["expression"]:
                                target_table = table
                                break
                    
                    # If still not found, use the first non-hidden table
                    if target_table is None and model["model"]["tables"]:
                        target_table = next((t for t in model["model"]["tables"] 
                                           if not t.get("isHidden", False)), model["model"]["tables"][0])
                    
                    if target_table:
                        # Add the measure to the table
                        measure = {
                            "name": calc["name"],
                            "expression": clean_dax_expression(calc["expression"]),
                            "formatString": get_format_string(calc["name"], calc["expression"]),
                            "lineageTag": generate_lineage_tag()
                        }
                        target_table["measures"].append(measure)
        
        # Write the model to file
        with open(output_bim_path, 'w') as f:
            json.dump(model, f, indent=2)
        
        return True
    except Exception as e:
        logger.error(f"Error creating model: {e}")
        return False

def extract_measure_base_tables(root: ET.Element) -> Dict[str, str]:
    """
    Extract measures and their base tables from a Tableau workbook.
    Enhanced with improved table extraction logic from tableExtraction.py.
    
    Args:
        root: The root element of the Tableau workbook XML
        
    Returns:
        Dictionary mapping measure names to their base table names
    """
    measures = {}
    
    # Find the datasource
    datasource = root.find('.//datasource')
    if datasource is None:
        logger.warning("No datasource found in workbook")
        return measures

    # Log the datasource we're working with
    logger.info(f"Using datasource: {datasource.get('name', 'unnamed')}")

    # Extract column-to-table mappings from the <cols> section
    cols = datasource.find('connection/cols')
    if cols is None:
        logger.warning("<cols> section not found in datasource")
        return measures

    # Build column to table mapping
    column_to_table = {}
    for map_elem in cols.findall('map'):
        key = map_elem.get('key')  # e.g., '[Age]'
        value = map_elem.get('value')  # e.g., '[sample_data.csv].[Age]'
        
        if key and value:
            # Extract table name from value
            if '].[' in value:
                # Standard format: '[table].[column]'
                table_name = value.split('].[')[0].strip('[')
                # Remove file extension if present
                table_name = os.path.splitext(table_name)[0]
                column_to_table[key] = table_name
            else:
                # Alternative format
                parts = value.split('.')
                if len(parts) >= 2:
                    table_name = parts[0].strip('[')
                    # Remove file extension if present
                    table_name = os.path.splitext(table_name)[0]
                    column_to_table[key] = table_name

    def extract_column_from_formula(formula):
        """Extract the first column name in square brackets from a formula."""
        if not formula:
            return None
        match = re.search(r'\[([^\]]+)\]', formula)
        return match.group(0) if match else None

    # Step 1: Collect measures from the <datasource> with role='measure'
    for column in datasource.findall('column[@role="measure"]'):
        measure_name = column.get('caption') or column.get('name')
        if not measure_name:
            continue
            
        # Check if it's a calculated measure
        calculation = column.find('calculation')
        if calculation is not None:
            formula = calculation.get('formula')
            dependent_column = extract_column_from_formula(formula)
            if dependent_column and dependent_column in column_to_table:
                table = column_to_table[dependent_column]
                measures[measure_name] = table
                logger.debug(f"Found calculated measure '{measure_name}' based on column '{dependent_column}', assigned to table '{table}'")
        else:
            # Direct measure or table measure
            if measure_name in column_to_table:
                table = column_to_table[measure_name]
                measures[measure_name] = table
                logger.debug(f"Found direct measure '{measure_name}', assigned to table '{table}'")
            elif measure_name.startswith('[') and measure_name.endswith(']'):
                # Try with and without brackets
                name_without_brackets = measure_name[1:-1]
                bracket_key = f"[{name_without_brackets}]"
                
                if bracket_key in column_to_table:
                    table = column_to_table[bracket_key]
                    measures[measure_name] = table
                    logger.debug(f"Found bracketed measure '{measure_name}', assigned to table '{table}'")

    # Step 2: Look for table captions as potential measure sources
    for column in datasource.findall('column'):
        if not column.get('role') == 'measure':
            continue
            
        parent_name = column.get('parent-name')
        if parent_name:
            # Remove .csv extension if present
            table_name = os.path.splitext(parent_name)[0]
            measure_name = column.get('caption') or column.get('name')
            if measure_name and measure_name not in measures:
                measures[measure_name] = table_name
                logger.debug(f"Found measure '{measure_name}' with parent-name '{parent_name}', assigned to table '{table_name}'")

    # Step 3: Look for internal tableau measures
    for column in datasource.findall('column'):
        if column.get('name', '').startswith('[__tableau_internal_object_id__]'):
            measure_name = column.get('caption')
            if measure_name and measure_name not in measures:
                # For internals, use the caption as the table name
                # This is often something like "Number of Records"
                source = column.find("./drill-paths/drill-path/field")
                if source is not None:
                    source_table = source.get('value', '').split('.')[0].strip('[')
                    if source_table:
                        measures[measure_name] = source_table
                        logger.debug(f"Found internal tableau measure '{measure_name}', assigned to table '{source_table}'")

    # Step 4: Collect additional measures from worksheets
    for worksheet in root.findall('.//worksheet'):
        dependencies = worksheet.find('table/view/datasource-dependencies')
        if dependencies is not None:
            for column in dependencies.findall('column[@role="measure"]'):
                measure_name = column.get('caption') or column.get('name')
                # Add only if not already collected from datasource
                if measure_name and measure_name not in measures:
                    # Try different ways to find the table
                    if measure_name in column_to_table:
                        table = column_to_table[measure_name]
                        measures[measure_name] = table
                        logger.debug(f"Found worksheet measure '{measure_name}', assigned to table '{table}'")
                    else:
                        # Try to find a base table from the worksheet
                        for dep_column in dependencies.findall('column'):
                            dep_name = dep_column.get('name')
                            if dep_name and dep_name in column_to_table and dep_name != measure_name:
                                table = column_to_table[dep_name]
                                measures[measure_name] = table
                                logger.debug(f"Found worksheet measure '{measure_name}' via dependency '{dep_name}', assigned to table '{table}'")
                                break

    # Step 5: For any remaining measures without tables, try to infer from formula
    for column in datasource.findall('.//column[@role="measure"]'):
        measure_name = column.get('caption') or column.get('name')
        if measure_name and measure_name not in measures:
            calculation = column.find('calculation')
            if calculation is not None:
                formula = calculation.get('formula', '')
                # Try to extract table names from the formula
                table_mentions = []
                for table in set(column_to_table.values()):
                    if table in formula or f"[{table}]" in formula or f"'{table}'" in formula:
                        table_mentions.append(table)
                
                if table_mentions:
                    # Use the most frequently mentioned table
                    from collections import Counter
                    most_common = Counter(table_mentions).most_common(1)[0][0]
                    measures[measure_name] = most_common
                    logger.debug(f"Assigned measure '{measure_name}' to table '{most_common}' based on formula mentions")

    logger.info(f"Successfully extracted {len(measures)} measure base tables")
    return measures

def correct_table_references_in_formula(dax_formula: str, base_table: str, table_names: List[str], model_tables: List[Dict] = None) -> str:
    """
    Correct table references in a DAX formula to use the base table where appropriate.
    
    Args:
        dax_formula: The DAX formula to correct
        base_table: The correct base table for this measure
        table_names: List of all available table names
        model_tables: Optional list of tables with their columns from the model
        
    Returns:
        Corrected DAX formula
    """
    if not dax_formula:
        return dax_formula
    
    # Build a dictionary of columns by table if model_tables is provided
    columns_by_table = {}
    if model_tables:
        for table in model_tables:
            table_name = table.get('name')
            if table_name:
                columns_by_table[table_name] = [col.get('name') for col in table.get('columns', [])]
    
    # Helper function to clean column names from CSV file references
    def clean_column_name(column_name):
        # Remove CSV file references like "Department (projects.csv)"
        return re.sub(r"\s*\([^)]+\.csv\)\s*", "", column_name)
    
    # Regular expression to find table references like 'table_name'[column_name]
    table_pattern = r"'([^']+)'(\[([^\]]+)\])"
    
    def replace_table_ref(match):
        table_ref = match.group(1)
        column_ref = match.group(2)  # includes brackets
        column_name = match.group(3)  # just the name without brackets
        
        # Clean the column name (remove CSV file references)
        clean_name = clean_column_name(column_name)
        
        # Don't modify if already referencing the base table
        if table_ref == base_table:
            return f"'{table_ref}'[{clean_name}]"
        
        # If the referenced table is in our list but isn't the base table
        if table_ref in table_names and table_ref != base_table:
            # Check if column contains base table name or file extension
            if f"({base_table}.csv" in column_name or base_table.lower() in column_name.lower():
                # This is likely a column from the base table referenced with the wrong table
                return f"'{base_table}'[{clean_name}]"
            
            # Check if the column exists in both tables using our column dictionary
            if columns_by_table and base_table in columns_by_table and table_ref in columns_by_table:
                base_columns = columns_by_table[base_table]
                
                # First check for the exact match
                if clean_name in base_columns:
                    logger.debug(f"Column '{clean_name}' found in both {table_ref} and {base_table}, using {base_table}")
                    return f"'{base_table}'[{clean_name}]"
                
                # Also check if any column in base table has the same name after removing file extensions
                for col in base_columns:
                    if clean_column_name(col) == clean_name:
                        logger.debug(f"Column '{clean_name}' (cleaned) found in both {table_ref} and {base_table}, using {base_table}")
                        return f"'{base_table}'[{clean_name}]"
        
        # If we get here, just use the original table but with cleaned column name
        return f"'{table_ref}'[{clean_name}]"
    
    # Apply the correction to all table references
    corrected_formula = re.sub(table_pattern, replace_table_ref, dax_formula)
    
    # Clean up any remaining column references with CSV file extensions
    corrected_formula = re.sub(r"\[([^\]]+) \([^)]+\.csv\)\]", r"[\1]", corrected_formula)
    
    return corrected_formula

def clean_column_name(column_name: str) -> str:
    """
    Clean column names by removing file extensions and CSV file references.
    
    Args:
        column_name: The column name to clean
        
    Returns:
        Cleaned column name
    """
    # Remove CSV file references like "Department (projects.csv)"
    cleaned = re.sub(r"\s*\([^)]+\.csv\)\s*", "", column_name.strip())
    return cleaned

def process_dax_calculations(dax_file_path: str, model_file_path: str, tableau_file_path: str = None, replace_existing: bool = True) -> bool:
    """
    Process DAX calculations and add them to the model with correct table assignments.
    
    Args:
        dax_file_path: Path to the JSON file containing DAX calculations
        model_file_path: Path to the Power BI model (.bim) file
        tableau_file_path: Optional path to original Tableau workbook for measure base table extraction
        replace_existing: Whether to replace existing measures with the same name
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load the model
        with open(model_file_path, 'r', encoding='utf-8') as f:
            model = json.load(f)

        # Load DAX calculations
        with open(dax_file_path, 'r', encoding='utf-8') as f:
            dax_calcs = json.load(f)

        # Get measure base tables if Tableau file is provided
        measure_base_tables = {}
        if tableau_file_path and os.path.exists(tableau_file_path):
            try:
                logger.info(f"Extracting measure base tables from {tableau_file_path}")
                tree = ET.parse(tableau_file_path)
                root = tree.getroot()
                measure_base_tables = extract_measure_base_tables(root)
                logger.info(f"Found {len(measure_base_tables)} measure-to-table mappings")
                
                # Log the mappings for debugging
                for measure, table in measure_base_tables.items():
                    logger.debug(f"Measure '{measure}' -> Table '{table}'")
            except Exception as e:
                logger.warning(f"Error extracting measure base tables: {str(e)}")
                import traceback
                logger.debug(traceback.format_exc())

        # Get existing table names and columns from the model
        tables = model['model']['tables']
        tables_by_name = {t['name']: t for t in tables}
        visible_table_names = [t['name'] for t in tables if not t.get('isHidden', False)]
        
        logger.info(f"Model contains tables: {visible_table_names}")
        
        measures_added = 0
        measures_updated = 0
        measures_failed = 0

        # Process each calculation
        for calc in dax_calcs:
            try:
                # Extract measure name and formula from different possible formats
                measure_name = None
                dax_formula = None
                
                if isinstance(calc, dict):
                    # Format from BI_Convert_Tool
                    if 'name' in calc and ('dax_formula' in calc or 'expression' in calc or 'dax' in calc):
                        measure_name = calc['name']
                        dax_formula = calc.get('dax_formula') or calc.get('expression') or calc.get('dax')
                    elif 'calculatedFieldName' in calc and 'daxFormula' in calc:
                        measure_name = calc['calculatedFieldName']
                        dax_formula = calc['daxFormula']
                    # Handle format from twb_to_bim_converter
                    elif 'type' in calc and calc['type'].lower() == 'measure':
                        measure_name = calc.get('name')
                        dax_formula = calc.get('expression')
                
                if not measure_name or not dax_formula:
                    logger.warning(f"Invalid measure format in DAX file: {calc}")
                    measures_failed += 1
                    continue

                # Clean and validate the DAX expression
                dax_formula = clean_dax_expression(dax_formula)
                
                # NEW: Additional cleanup for CSV file references in column names
                dax_formula = re.sub(r"'([^']+)'\[([^\]]+) \([^)]+\.csv\)\]", r"'\1'[\2]", dax_formula)
                dax_formula = re.sub(r"\[([^\]]+) \([^)]+\.csv\)\]", r"[\1]", dax_formula)
                
                # Determine the base table (IMPROVED TABLE ASSIGNMENT LOGIC)
                base_table = None
                
                # PRIORITY 1: Use base table from Tableau workbook extraction
                if measure_name in measure_base_tables:
                    base_table = measure_base_tables[measure_name]
                    # Verify the table exists in the model
                    if base_table in tables_by_name:
                        logger.info(f"Using Tableau-defined base table '{base_table}' for measure '{measure_name}'")
                    else:
                        logger.warning(f"Tableau-defined base table '{base_table}' for measure '{measure_name}' not found in model")
                        base_table = None

                # PRIORITY 2: If no mapping or table not found, infer from DAX formula
                if not base_table:
                    # Extract all table references from the DAX formula
                    referenced_tables = re.findall(r'\'([^\']+)\'', dax_formula)
                    
                    if referenced_tables:
                        # Count references to each table
                        table_counts = {}
                        for table in referenced_tables:
                            if table in visible_table_names:  # Only count existing tables
                                table_counts[table] = table_counts.get(table, 0) + 1
                                
                        if table_counts:
                            # Use the most referenced table
                            base_table = max(table_counts.items(), key=lambda x: x[1])[0]
                            logger.info(f"Assigned measure '{measure_name}' to table '{base_table}' based on formula references")

                # PRIORITY 3: Try to match based on measure name containing table name
                if not base_table:
                    measure_name_lower = measure_name.lower()
                    for table_name in visible_table_names:
                        if table_name.lower() in measure_name_lower:
                            base_table = table_name
                            logger.info(f"Assigned measure '{measure_name}' to table '{base_table}' based on name similarity")
                            break

                # PRIORITY 4: Default to first visible table as last resort
                if not base_table and visible_table_names:
                    base_table = visible_table_names[0]
                    logger.warning(f"Could not determine base table for measure '{measure_name}'. Using default: '{base_table}'")

                if not base_table:
                    logger.error(f"No tables available for measure '{measure_name}'")
                    measures_failed += 1
                    continue
                
                # NEW: Pass the model tables to the correction function for column lookup
                corrected_dax_formula = correct_table_references_in_formula(dax_formula, base_table, visible_table_names, tables)
                if corrected_dax_formula != dax_formula:
                    logger.info(f"Corrected table references in formula for '{measure_name}'")
                    logger.debug(f"Original: {dax_formula}")
                    logger.debug(f"Corrected: {corrected_dax_formula}")
                    dax_formula = corrected_dax_formula

                # Create the measure object
                measure = {
                    "name": measure_name,
                    "expression": dax_formula,
                    "formatString": get_format_string(measure_name, dax_formula),
                    "lineageTag": generate_lineage_tag()
                }

                # Find the target table
                target_table = tables_by_name.get(base_table)
                if not target_table:
                    logger.error(f"Target table '{base_table}' not found in model")
                    measures_failed += 1
                    continue

                # Initialize measures array if it doesn't exist
                if 'measures' not in target_table:
                    target_table['measures'] = []

                # Check if measure already exists
                existing_measure = next((m for m in target_table['measures'] if m['name'] == measure_name), None)
                if existing_measure:
                    if replace_existing:
                        target_table['measures'].remove(existing_measure)
                        target_table['measures'].append(measure)
                        measures_updated += 1
                        logger.info(f"Updated measure '{measure_name}' in table '{base_table}'")
                    else:
                        logger.info(f"Skipping existing measure '{measure_name}'")
                else:
                    target_table['measures'].append(measure)
                    measures_added += 1
                    logger.info(f"Added measure '{measure_name}' to table '{base_table}'")

            except Exception as e:
                logger.error(f"Error processing measure: {str(e)}")
                measures_failed += 1

        # Save the updated model
        with open(model_file_path, 'w', encoding='utf-8') as f:
            json.dump(model, f, indent=2)

        logger.info(f"DAX processing complete: {measures_added} added, {measures_updated} updated, {measures_failed} failed")
        return True

    except Exception as e:
        logger.error(f"Error processing DAX calculations: {str(e)}")
        return False

def validate_bim_file(bim_path: str) -> Tuple[bool, List[str]]:
    """
    Validate the generated BIM file.
    
    Args:
        bim_path: Path to the BIM file
        
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []
    
    try:
        # Read the BIM file
        with open(bim_path, 'r') as f:
            model = json.load(f)
            
        # Basic structure validation
        if not isinstance(model, dict):
            issues.append("BIM file is not a valid JSON object")
            return False, issues
            
        if 'model' not in model:
            issues.append("Missing 'model' key in BIM file")
            return False, issues
            
        if 'tables' not in model['model']:
            issues.append("Missing 'tables' in model")
            return False, issues
            
        # Check for empty tables
        tables = model['model']['tables']
        if not tables:
            issues.append("No tables found in the model")
        
        # Validate relationships
        relationships = model['model'].get('relationships', [])
        for rel in relationships:
            if 'fromTable' not in rel or 'fromColumn' not in rel or 'toTable' not in rel or 'toColumn' not in rel:
                issues.append(f"Invalid relationship: {rel}")
            else:
                # Check if referenced tables and columns exist
                from_table_exists = any(t['name'] == rel['fromTable'] for t in tables)
                to_table_exists = any(t['name'] == rel['toTable'] for t in tables)
                
                if not from_table_exists:
                    issues.append(f"Relationship references non-existent fromTable: {rel['fromTable']}")
                if not to_table_exists:
                    issues.append(f"Relationship references non-existent toTable: {rel['toTable']}")
        
        # Validate measures
        for table in tables:
            for measure in table.get('measures', []):
                if not measure.get('expression'):
                    issues.append(f"Empty measure expression in {table['name']}: {measure['name']}")
        
        # Check for common Power BI model requirements
        has_date_tables = any('DateTableTemplate' in t['name'] for t in tables)
        if not has_date_tables and any('dateTime' in [c.get('dataType') for t in tables for c in t.get('columns', [])]):
            issues.append("Model has date columns but no date table template")
        
        return len(issues) == 0, issues
        
    except Exception as e:
        issues.append(f"Validation error: {str(e)}")
        return False, issues

def generate_conversion_report(output_dir: str, twb_file: str, bim_file: str, 
                               tables: List[str], measures: List[Dict], 
                               relationships: List[Dict], validation_issues: List[str]) -> str:
    """
    Generate a detailed conversion report.
    
    Args:
        output_dir: Directory to save the report
        twb_file: Path to the source TWB file
        bim_file: Path to the generated BIM file
        tables: List of tables converted
        measures: List of measures converted
        relationships: List of relationships created
        validation_issues: List of validation issues
        
    Returns:
        Path to the generated report
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(output_dir, f"conversion_report_{timestamp}.md")
    
    with open(report_path, 'w') as f:
        # Write report header
        f.write(f"# Tableau to Power BI Conversion Report\n\n")
        f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"**Source File:** {os.path.basename(twb_file)}\n")
        f.write(f"**Output File:** {os.path.basename(bim_file)}\n\n")
        
        # Summary
        f.write("## Conversion Summary\n\n")
        f.write(f"* **Tables:** {len(tables)}\n")
        f.write(f"* **Measures:** {len(measures)}\n")
        f.write(f"* **Relationships:** {len(relationships)}\n")
        f.write(f"* **Validation Issues:** {len(validation_issues)}\n\n")
        
        # Tables
        f.write("## Tables\n\n")
        if tables:
            f.write("| # | Table Name |\n")
            f.write("|---|------------|\n")
            for i, table in enumerate(tables, 1):
                f.write(f"| {i} | {table} |\n")
        else:
            f.write("No tables were converted.\n")
        f.write("\n")
        
        # Measures
        f.write("## Measures\n\n")
        if measures:
            f.write("| # | Table | Measure Name | Expression |\n")
            f.write("|---|-------|-------------|------------|\n")
            for i, measure in enumerate(measures, 1):
                table_name = measure.get('table', 'Unknown')
                name = measure.get('name', 'Unnamed')
                expression = measure.get('expression', '')
                # Truncate expression if too long
                if len(expression) > 50:
                    expression = expression[:47] + "..."
                f.write(f"| {i} | {table_name} | {name} | `{expression}` |\n")
        else:
            f.write("No measures were converted.\n")
        f.write("\n")
        
        # Relationships
        f.write("## Relationships\n\n")
        if relationships:
            f.write("| # | From Table | From Column | To Table | To Column |\n")
            f.write("|---|-----------|------------|----------|----------|\n")
            for i, rel in enumerate(relationships, 1):
                f.write(f"| {i} | {rel.get('fromTable', 'Unknown')} | {rel.get('fromColumn', 'Unknown')} | {rel.get('toTable', 'Unknown')} | {rel.get('toColumn', 'Unknown')} |\n")
        else:
            f.write("No relationships were created.\n")
        f.write("\n")
        
        # Validation Issues
        f.write("## Validation Issues\n\n")
        if validation_issues:
            f.write("| # | Issue |\n")
            f.write("|---|-------|\n")
            for i, issue in enumerate(validation_issues, 1):
                f.write(f"| {i} | {issue} |\n")
        else:
            f.write("No validation issues found. The BIM file appears to be valid.\n")
        f.write("\n")
        
        # Next Steps
        f.write("## Next Steps\n\n")
        f.write("1. Open the generated Model.bim file in Power BI Desktop or Tabular Editor\n")
        f.write("2. Check and fix any validation issues mentioned above\n")
        f.write("3. Add additional measures and calculated columns as needed\n")
        f.write("4. Configure data refresh and security settings\n")
        f.write("5. Create reports and dashboards based on the imported model\n\n")
        
        # Footer
        f.write("*Generated with twb_to_bim_converter*\n")
    
    return report_path

def batch_process_files(source_dir: str, output_dir: str, pattern: str = "*.twb") -> Dict:
    """
    Process multiple Tableau workbook files in batch mode.
    
    Args:
        source_dir: Directory containing TWB files
        output_dir: Directory to save output files
        pattern: File pattern to match
        
    Returns:
        Dictionary with results for each file
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all matching files
    file_paths = glob.glob(os.path.join(source_dir, pattern))
    
    if not file_paths:
        logger.warning(f"No files matching '{pattern}' found in {source_dir}")
        return {}
    
    results = {}
    
    # Process each file
    with ThreadPoolExecutor(max_workers=min(os.cpu_count(), len(file_paths))) as executor:
        futures = {}
        for file_path in file_paths:
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            output_path = os.path.join(output_dir, f"{base_name}_Model.bim")
            dax_file = os.path.join(source_dir, f"{base_name}_dax_calculations.json")
            
            # Check if DAX file exists
            has_dax = os.path.exists(dax_file)
            
            # Submit the task
            futures[executor.submit(process_single_file, file_path, output_path, dax_file if has_dax else None)] = base_name
        
        # Process results as they complete
        for future in futures:
            base_name = futures[future]
            try:
                result = future.result()
                results[base_name] = result
                if result['success']:
                    logger.info(f"Successfully processed {base_name}")
                else:
                    logger.error(f"Failed to process {base_name}: {result['error']}")
            except Exception as e:
                logger.error(f"Error processing {base_name}: {str(e)}")
                results[base_name] = {
                    'success': False,
                    'error': str(e)
                }
    
    return results

def process_single_file(twb_path: str, output_path: str, dax_path: str = None) -> Dict:
    """
    Process a single TWB file.
    
    Args:
        twb_path: Path to the TWB file
        output_path: Path to save the BIM file
        dax_path: Optional path to DAX calculations file
        
    Returns:
        Dictionary with results
    """
    result = {
        'success': False,
        'error': None,
        'tables_count': 0,
        'measures_count': 0,
        'relationships_count': 0,
        'validation_issues': [],
        'output_path': output_path
    }
    
    try:
        # Create the base model
        base_model_created = create_model_bim(twb_path, output_path)
        
        if not base_model_created:
            result['error'] = "Failed to create base model"
            return result
        
        # Process DAX calculations if provided
        if dax_path and os.path.exists(dax_path):
            dax_processed = process_dax_calculations(dax_path, output_path, twb_path)
            if not dax_processed:
                result['error'] = "Failed to process DAX calculations"
                # We continue even if DAX processing fails
        
        # Read the created model to get statistics
        with open(output_path, 'r') as f:
            model = json.load(f)
            
        # Collect statistics
        tables = model['model']['tables']
        non_hidden_tables = [t for t in tables if not t.get('isHidden', False)]
        
        result['tables_count'] = len(non_hidden_tables)
        result['measures_count'] = sum(len(t.get('measures', [])) for t in tables)
        result['relationships_count'] = len(model['model'].get('relationships', []))
        
        # Validate the model
        is_valid, issues = validate_bim_file(output_path)
        result['validation_issues'] = issues
        
        result['success'] = True
    except Exception as e:
        result['error'] = str(e)
    
    return result

def check_data_files_exist(table_names: List[str], data_dir: str) -> List[str]:
    """
    Check if all required data files exist.
    
    Args:
        table_names: List of table names
        data_dir: Directory where data files are expected
        
    Returns:
        List of missing files
    """
    missing_files = []
    
    for table in table_names:
        csv_path = os.path.join(data_dir, f"{table}.csv")
        if not os.path.exists(csv_path):
            missing_files.append(csv_path)
    
    return missing_files

def show_progress(operation: str, current: int, total: int):
    """
    Display a simple progress bar.
    
    Args:
        operation: Name of the operation
        current: Current item being processed
        total: Total items to process
    """
    percent = min(100, int(current * 100 / total))
    bar_length = 40
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    
    sys.stdout.write(f'\r{operation}: [{bar}] {percent}% ({current}/{total}) ')
    sys.stdout.flush()
    
    if current == total:
        sys.stdout.write('\n')

def enhanced_main():
    """Enhanced main function with command line argument support"""
    parser = argparse.ArgumentParser(description='Convert Tableau workbooks to Power BI models.')
    
    # Add arguments
    parser.add_argument('-i', '--input', help='Input TWB file or directory', required=True)
    parser.add_argument('-o', '--output', help='Output directory for BIM files', required=True)
    parser.add_argument('-d', '--dax', help='DAX calculations file (optional)')
    parser.add_argument('-b', '--batch', action='store_true', help='Process all TWB files in input directory')
    parser.add_argument('-r', '--report', action='store_true', help='Generate conversion report')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Configure logging based on verbosity
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Process files
    if args.batch:
        if not os.path.isdir(args.input):
            logger.error(f"Input must be a directory when using batch mode: {args.input}")
            return 1
            
        logger.info(f"Starting batch processing from {args.input} to {args.output}")
        results = batch_process_files(args.input, args.output)
        
        # Summarize results
        success_count = sum(1 for r in results.values() if r['success'])
        logger.info(f"Batch processing complete. {success_count}/{len(results)} files successful.")
        
        if args.report:
            # Generate batch report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            batch_report_path = os.path.join(args.output, f"batch_report_{timestamp}.csv")
            
            with open(batch_report_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['File', 'Success', 'Tables', 'Measures', 'Relationships', 'Validation Issues', 'Error'])
                
                for file, result in results.items():
                    writer.writerow([
                        file, 
                        result['success'],
                        result['tables_count'],
                        result['measures_count'],
                        result['relationships_count'],
                        len(result['validation_issues']),
                        result['error'] or ''
                    ])
            
            logger.info(f"Batch report saved to {batch_report_path}")
            
    else:
        # Single file processing
        if not os.path.isfile(args.input):
            logger.error(f"Input file does not exist: {args.input}")
            return 1
            
        # Ensure output directory exists
        os.makedirs(args.output, exist_ok=True)
        
        # Generate output path
        base_name = os.path.splitext(os.path.basename(args.input))[0]
        output_path = os.path.join(args.output, f"{base_name}_Model.bim")
        
        # Parse the TWB file to get table names
        try:
            tree = ET.parse(args.input)
            root = tree.getroot()
            table_names = extract_table_names_from_twb(root)
            
            # Check for data files
            data_dir = os.path.join(os.path.dirname(args.input), 'data')
            missing_files = check_data_files_exist(table_names, data_dir)
            
            if missing_files:
                logger.warning(f"Missing data files: {', '.join(missing_files)}")
                logger.warning("Model will be created but may not be able to import data.")
        except Exception as e:
            logger.error(f"Error parsing TWB file: {e}")
            return 1
        
        logger.info(f"Processing {args.input} to {output_path}")
        
        # Create the model
        if not create_model_bim(args.input, output_path):
            logger.error("Failed to create model")
            return 1
            
        # Process DAX if provided
        if args.dax and os.path.exists(args.dax):
            logger.info(f"Processing DAX calculations from {args.dax}")
            if not process_dax_calculations(args.dax, output_path, args.input):
                logger.error("Failed to process DAX calculations")
                # Continue despite DAX error
        
        # Validate the model
        logger.info("Validating the generated model...")
        is_valid, issues = validate_bim_file(output_path)
        
        if issues:
            logger.warning(f"Validation found {len(issues)} issues:")
            for issue in issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("Model validation successful")
            
        # Generate report if requested
        if args.report:
            try:
                # Read the model to get statistics
                with open(output_path, 'r') as f:
                    model = json.load(f)
                
                # Collect data for report
                tables = [t['name'] for t in model['model']['tables'] if not t.get('isHidden', False)]
                
                measures = []
                for table in model['model']['tables']:
                    for measure in table.get('measures', []):
                        measures.append({
                            'table': table['name'],
                            'name': measure['name'],
                            'expression': measure.get('expression', '')
                        })
                
                relationships = model['model'].get('relationships', [])
                
                # Generate the report
                report_path = generate_conversion_report(args.output, args.input, output_path, 
                                                        tables, measures, relationships, issues)
                logger.info(f"Conversion report saved to {report_path}")
            except Exception as e:
                logger.error(f"Error generating report: {e}")
        
        logger.info(f"Processing complete. Output saved to {output_path}")
    
    return 0

if __name__ == "__main__":
    exit_code = enhanced_main()
    sys.exit(exit_code)

def main():
    try:
        # Use proper Windows path format
        current_dir = os.path.dirname(os.path.abspath(__file__))
        twb_file_path = os.path.join(current_dir, 'Book1.twb')
        dax_file_path = os.path.join(current_dir, 'Book1_dax_calculations.json')
        output_bim_path = os.path.join(current_dir, 'Generated_Model.bim')
        
        logger.info(f"Working directory: {current_dir}")
        logger.info(f"TWB path: {twb_file_path}")
        logger.info(f"Output path: {output_bim_path}")
        
        # Step 1: Create the initial model structure from TWB
        logger.info("Creating base model from Tableau workbook...")
        if not create_model_bim(twb_file_path, output_bim_path):
            logger.error("Failed to create base model.")
            return
        
        # Step 2: Enhance with DAX calculations
        logger.info("Processing DAX calculations...")
        if not process_dax_calculations(dax_file_path, output_bim_path, twb_file_path):
            logger.error("Failed to process DAX calculations.")
            return
        
        # Verify the file was created
        if os.path.exists(output_bim_path):
            logger.info(f"Model generation complete! Output saved to: {output_bim_path}")
            logger.info(f"File size: {os.path.getsize(output_bim_path)} bytes")
        else:
            logger.error(f"ERROR: Output file was not created at {output_bim_path}")
    except Exception as e:
        logger.error(f"Error occurred during execution: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

