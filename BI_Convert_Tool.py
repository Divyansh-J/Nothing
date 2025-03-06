import tkinter as tk
from tkinter import filedialog, messagebox, ttk, scrolledtext
import os
import sys
import re
import json
import xml.etree.ElementTree as ET
from threading import Thread
import time

# Mapping Tableau mark classes to Power BI visual types
MARK_CLASS_TO_VISUAL_TYPE = {
    "Line": "lineChart",
    "Bar": "barChart",
    "Area": "areaChart",
    "Scatter": "scatterChart",
    "Pie": "pieChart",
    "Multipolygon": "filledMap",
    "Text": "tableEx",
    "Automatic": "columnChart",
    "columnChart": "columnChart"
}

# Function to extract table names from Tableau workbook
def extract_table_names(root):
    """Extract table names from Tableau workbook XML"""
    table_names = []
    
    # Try to extract actual table names from relations
    for relation in root.findall(".//relation[@name]"):
        table_name = relation.get("name")
        if table_name and table_name not in table_names:
            # Remove file extension if present
            table_name = os.path.splitext(table_name)[0]
            table_names.append(table_name)
    
    # If no table names found, try to extract from datasource captions
    if not table_names:
        for datasource in root.findall(".//datasource"):
            caption = datasource.get("caption")
            if caption and caption not in table_names:
                table_names.append(caption.split(" ")[0])
    
    # If still no table names, look for columns with parent-name
    if not table_names:
        for column in root.findall(".//column[@parent-name]"):
            parent_name = column.get("parent-name")
            if parent_name and parent_name not in table_names:
                parent_name = parent_name.replace(".csv", "")
                table_names.append(parent_name)
    
    # Default fallback
    if not table_names:
        table_names = ["YourTable"]
    
    return table_names

# Function to translate Tableau formulas to DAX

def translate_to_dax(tableau_formula, table_name='YourTable'):
    """
    Converts Tableau calculations to Power BI DAX.

    Args:
        tableau_formula (str): The Tableau calculation string.
        table_name (str): The name of the table to use in DAX formulas.

    Returns:
        dict: Dictionary with dax_formula and type.
    """

    def handle_lod_expressions(formula):
        """
        Recursively resolve all LOD expressions (FIXED, INCLUDE, EXCLUDE) in the formula.
        """
        def build_calculate(expression, dimension, context_type="ALLEXCEPT"):
            if context_type == "ALLEXCEPT":
                return f"CALCULATE({expression}, ALLEXCEPT('{table_name}', '{table_name}'[{dimension}]))"
            elif context_type == "ALL":
                return f"CALCULATE({expression}, ALL('{table_name}'[{dimension}]))"
            elif context_type == "REMOVEFILTERS":
                return f"CALCULATE({expression}, REMOVEFILTERS('{table_name}'[{dimension}]))"
            return expression

        while True:
            # Handle FIXED LOD
            fixed_match = re.search(r"\{\s*FIXED\s*\[(.+?)\]:\s*([^\}]+)\}", formula)
            if fixed_match:
                dimension, expression = fixed_match.groups()
                dax_replacement = build_calculate(expression, dimension, "ALLEXCEPT")
                formula = formula.replace(fixed_match.group(0), dax_replacement)

            # Handle INCLUDE LOD
            include_match = re.search(r"\{\s*INCLUDE\s*\[(.+?)\]:\s*([^\}]+)\}", formula)
            if include_match:
                dimension, expression = include_match.groups()
                dax_replacement = build_calculate(expression, dimension, "ALL")
                formula = formula.replace(include_match.group(0), dax_replacement)

            # Handle EXCLUDE LOD
            exclude_match = re.search(r"\{\s*EXCLUDE\s*\[(.+?)\]:\s*([^\}]+)\}", formula)
            if exclude_match:
                dimension, expression = exclude_match.groups()
                dax_replacement = build_calculate(expression, dimension, "REMOVEFILTERS")
                formula = formula.replace(exclude_match.group(0), dax_replacement)

            if not (fixed_match or include_match or exclude_match):
                break

        return formula

    # Tableau to DAX function map with table_name as parameter
    function_map = {
        # Aggregations
        r"SUM\((.+?)\)": lambda match: f"SUM('{table_name}'[{clean_column_name(match.group(1))}])",
        r"AVG\((.+?)\)": lambda match: f"AVERAGE('{table_name}'[{clean_column_name(match.group(1))}])",
        r"COUNT\((.+?)\)": lambda match: f"COUNT('{table_name}'[{clean_column_name(match.group(1))}])",
        r"COUNTD\(IF\s+\[([^\]]+)\]\s+AND\s+\[([^\]]+)\]\s+THEN\s+\[([^\]]+)\]\s+END\)": (
            lambda match: f"CALCULATE(DISTINCTCOUNT('{table_name}'[{clean_column_name(match.group(3))}]), '{table_name}'[{clean_column_name(match.group(1))}] = TRUE, '{table_name}'[{clean_column_name(match.group(2))}] = TRUE)"),
        r"COUNTD\(IF\s+\[([^\]]+)\]\s+THEN\s+\[([^\]]+)\]\s+END\)": (
            lambda match: f"CALCULATE(DISTINCTCOUNT('{table_name}'[{clean_column_name(match.group(2))}]), '{table_name}'[{clean_column_name(match.group(1))}])"),
        r"COUNTD\((.+?)\)": lambda match: f"DISTINCTCOUNT('{table_name}'[{clean_column_name(match.group(1))}])",
        r"MIN\((.+?)\)": lambda match: f"MIN('{table_name}'[{clean_column_name(match.group(1))}])",
        r"MAX\((.+?)\)": lambda match: f"MAX('{table_name}'[{clean_column_name(match.group(1))}])",
        r"(SUM|COUNT|MAX|MIN|AVERAGE)\((.+?)\s*WITH\s*FILTER\s*\((.+?)\)\)": r"CALCULATE(\1(\2), \3)",

        # Division Handling (Dynamic DIVIDE function)
        r"(.+?)\s*/\s*(.+)": (
            lambda match: f"DIVIDE({match.group(1).strip()}, {match.group(2).strip()}, 0)"
                .replace("[[", "[")  # Fix double brackets
                .replace("]]", "]")),
        r"(.+?)\s*-\s*(.+)": (
            lambda match: f"({match.group(1).strip()} - {match.group(2).strip()})"),

        # CASE Logic
       r"CASE\s+\[([^\]]+)\](.*?)\s+END": (
        lambda match: "SWITCH(TRUE(), " +
        match.group(2)
        .replace("WHEN", ",")
        .replace("THEN", ",")
        .replace("ELSE", ",")
        + " BLANK())"),

        r"ZN\((.+?)\)": (
        lambda match: f"IF(ISBLANK({match.group(1).strip()}), 0, {match.group(1).strip()})"), # ZN functions
        # RANK Logic
        r"RANK\((.+?),\s*'(.+?)'\)": (
            lambda match: f"RANKX(ALL('{table_name}'), {match.group(1).strip()}, {match.group(2).upper()})"
        ),

        # Logical Operators
        r"\bOR\b": r"||",
        r"\bAND\b": r"&&",

        # String Functions
        r"LEFT\((.+?),\s*(.+?)\)": f"LEFT('{table_name}'[\\1], \\2)",
        r"RIGHT\((.+?),\s*(.+?)\)": f"RIGHT('{table_name}'[\\1], \\2)",
        r"MID\((.+?),\s*(.+?),\s*(.+?)\)": f"MID('{table_name}'[\\1], \\2, \\3)",
        r"STR\((.+?)\)": r"FORMAT(\1, \"General Number\")",
        r"LEN\((.+?)\)": f"LEN('{table_name}'[\\1])",

        # Date Functions
        r"DATEADD\((.+?),\s*(.+?),\s*(.+?)\)": f"DATEADD('{table_name}'[\\1], \\2, \\3)",
        r"DATEDIFF\((.+?),\s*(.+?),\s*(.+?)\)": f"DATEDIFF('{table_name}'[\\1], '{table_name}'[\\2], \\3)",
        r"YEAR\((.+?)\)": f"YEAR('{table_name}'[\\1])",
        r"MONTH\((.+?)\)": f"MONTH('{table_name}'[\\1])",
        r"DAY\((.+?)\)": f"DAY('{table_name}'[\\1])",
        r"DATETRUNC\((.+?),\s*(.+?)\)": f"TRUNC('{table_name}'[\\2], \\1)",

        # NULL/ISNULL Checks
        r"ISNULL\((.+?)\)": f"ISBLANK('{table_name}'[\\1])",
        

        # Conditional Statements
        r"IIF\s*\(\s*(.+?)\s*,\s*'(.+?)'\s*,\s*'(.+?)'\s*\)": r"IF(\1, \2, \3)",
        r"IF\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+END": (
            lambda match: (
                f"IF({match.group(1).strip()}, {match.group(2).strip()}, {match.group(3).strip()})"
                if "IF" not in match.group(2) and "IF" not in match.group(3)
                else f"SWITCH(TRUE(), {match.group(1).strip()}, {match.group(2).strip()}, {match.group(3).strip()}, BLANK())"
            )
        ),

        # Arithmetic
        r"\[([^\]]+)\]\s*\+\s*\[([^\]]+)\]": lambda match: f"'{table_name}'[{clean_column_name(match.group(1))}] + '{table_name}'[{clean_column_name(match.group(2))}]",
        r"\[([^\]]+)\]\s*\-\s*\[([^\]]+)\]": lambda match: f"'{table_name}'[{clean_column_name(match.group(1))}] - '{table_name}'[{clean_column_name(match.group(2))}]",
        r"\[([^\]]+)\]\s*\*\s*\[([^\]]+)\]": lambda match: f"'{table_name}'[{clean_column_name(match.group(1))}] * '{table_name}'[{clean_column_name(match.group(2))}]",
        r"\[([^\]]+)\]\s*/\s*\[([^\]]+)\]": lambda match: f"DIVIDE('{table_name}'[{clean_column_name(match.group(1))}], '{table_name}'[{clean_column_name(match.group(2))}], 0)",
        
        # Improved date handling
        r"DATEDIFF\('([^']+)',\s*(.+?),\s*(.+?)\)": f"DATEDIFF('\\1', \\2, \\3)",
        
        # NEW: Handle references to columns with table name in them
        r"\[Department \(projects.csv\)\]": f"'{table_name}'[Department]",
    }

    # Handle LOD Expressions
    dax_formula = handle_lod_expressions(tableau_formula)

    # NEW: Function to clean column names by removing file references
    def clean_column_name(column_name):
        # Remove CSV file references like "Department (projects.csv)"
        clean_name = re.sub(r"\s*\([^)]+\.csv\)\s*", "", column_name.strip())
        return clean_name

    # Replace Tableau functions with DAX equivalents
    for tableau_pattern, dax_replacement in function_map.items():
        if callable(dax_replacement):
            # Use regex substitution with a function
            dax_formula = re.sub(tableau_pattern, dax_replacement, dax_formula, flags=re.IGNORECASE)
        else:
            # Use simple string replacement for non-function patterns
            dax_formula = re.sub(tableau_pattern, dax_replacement, dax_formula, flags=re.IGNORECASE)
    
    # Fix common DAX issues
    dax_formula = dax_formula.replace("[[", "[").replace("]]", "]")
    
    # Clean up any remaining column references with CSV file extensions
    dax_formula = re.sub(r"'([^']+)'\[([^\]]+) \([^)]+\.csv\)\]", r"'\1'[\2]", dax_formula)
    dax_formula = re.sub(r"\[([^\]]+) \([^)]+\.csv\)\]", r"[\1]", dax_formula)
    
    # Better detect measure vs calculated column
    if any(agg in tableau_formula.upper() for agg in ["SUM", "AVG", "COUNT", "COUNTD", "MIN", "MAX", "FIXED"]):
        classification = "measure"
    else:
        classification = "calculated_column"

    return {
        "dax_formula": dax_formula.strip(),
        "type": classification
    }

   
# Function to extract calculated fields
def extract_calculated_fields(root, table_name='YourTable'):
    calculated_fields = []
    seen_formulas = set()  # Track seen formulas to avoid duplicates
    
    for column in root.findall(".//column"):
        calc = column.find("calculation")
        if calc is not None:
            formula = calc.get("formula")
            name = column.get("caption", "Unnamed Calculation")
            
            if formula:
                cleaned_tableau_formula = formula.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").strip()
                
                # Skip duplicates
                if (name, cleaned_tableau_formula) in seen_formulas:
                    continue
                    
                seen_formulas.add((name, cleaned_tableau_formula))
                dax_result = translate_to_dax(cleaned_tableau_formula, table_name)
                cleaned_dax_formula = dax_result["dax_formula"].replace("\r\n", " ").replace("\n", " ").replace("\r", " ").strip()
                
                calculated_fields.append({
                    "name": name,
                    "tableau_formula": cleaned_tableau_formula,
                    "dax_formula": cleaned_dax_formula,
                    "type": dax_result["type"]
                })
    return calculated_fields


# Function to extract dashboards and visuals

def extract_dashboards_and_visuals_from_xml(xml_data):
    dashboards_data = {}
    root = ET.fromstring(xml_data)
    return dashboards_data, root

# Improved main function with better error handling
def main(twb_file_path, dax_output_path, table_name='YourTable', progress_callback=None):
    """
    Main function to process Tableau XML and save DAX formulas.
    """
    try:
        if progress_callback:
            progress_callback(0, "Starting conversion process...")
            
        if progress_callback:
            progress_callback(20, "Reading Tableau XML file...")
            
        # Read Tableau XML
        with open(twb_file_path, 'r', encoding='utf-8') as xml_file:
            xml_data = xml_file.read()
            
        if progress_callback:
            progress_callback(40, "Extracting Tableau structure...")
            
        # Extract root element from XML
        _, root = extract_dashboards_and_visuals_from_xml(xml_data)
        
        if progress_callback:
            progress_callback(70, "Translating calculated fields to DAX...")
            
        calculated_fields = extract_calculated_fields(root, table_name)

        if progress_callback:
            progress_callback(90, "Saving DAX formulas...")
            
        # Save the generated DAX calculations
        with open(dax_output_path, "w") as file:
            json.dump(calculated_fields, file, indent=4)

        if progress_callback:
            progress_callback(100, "Conversion complete!")

        return True, ""
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        if progress_callback:
            progress_callback(-1, error_message)
        return False, error_message

# Enhanced GUI with table name selection
class ConverterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("BIConvert - Tableau to DAX")
        self.root.geometry("800x600")
        self.root.resizable(True, True)
        self.root.configure(bg="#f2f2f2")
        
        self.segoe_ui = "Segoe UI"
        self.setup_ui()
        
    def setup_ui(self):
        # Title
        title_label = tk.Label(
            self.root,
            text="Tableau to DAX Formula Converter",
            font=(self.segoe_ui, 18, "bold"),
            bg="#f2f2f2",
            fg="#333333"
        )
        title_label.pack(pady=(20, 30))
        
        # Main frame
        main_frame = tk.Frame(self.root, bg="#f2f2f2")
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20)
        
        # File selection frame
        file_frame = tk.LabelFrame(main_frame, text="Input File", font=(self.segoe_ui, 12), bg="#f2f2f2", fg="#333333", padx=10, pady=10)
        file_frame.pack(fill=tk.X, pady=10)
        
        # File path entry
        self.file_path_var = tk.StringVar()
        file_path_entry = tk.Entry(file_frame, textvariable=self.file_path_var, font=(self.segoe_ui, 12), width=50)
        file_path_entry.pack(side=tk.LEFT, padx=(5, 10), fill=tk.X, expand=True)
        
        # Browse button
        browse_button = ttk.Button(
            file_frame, 
            text="Browse", 
            command=self.browse_file
        )
        browse_button.pack(side=tk.RIGHT, padx=5)
        
        # Options frame
        options_frame = tk.LabelFrame(main_frame, text="Options", font=(self.segoe_ui, 12), bg="#f2f2f2", fg="#333333", padx=10, pady=10)
        options_frame.pack(fill=tk.X, pady=10)
        
        # Table name option with combobox instead of entry
        table_label = tk.Label(options_frame, text="Table name in DAX:", font=(self.segoe_ui, 11), bg="#f2f2f2")
        table_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")
        
        self.table_name_var = tk.StringVar(value="YourTable")
        self.table_combobox = ttk.Combobox(options_frame, textvariable=self.table_name_var, font=(self.segoe_ui, 11), width=20)
        self.table_combobox.grid(row=0, column=1, padx=5, pady=5, sticky="w")
        self.table_combobox['values'] = ["YourTable"]  # Default value
        
        # Progress frame
        progress_frame = tk.Frame(main_frame, bg="#f2f2f2")
        progress_frame.pack(fill=tk.X, pady=10)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X, padx=5, pady=5)
        
        self.status_var = tk.StringVar(value="Ready to convert")
        status_label = tk.Label(progress_frame, textvariable=self.status_var, font=(self.segoe_ui, 10), bg="#f2f2f2")
        status_label.pack(fill=tk.X, padx=5)
        
        # Preview frame for DAX formulas
        preview_frame = tk.LabelFrame(main_frame, text="Formula Preview", font=(self.segoe_ui, 12), bg="#f2f2f2", fg="#333333", padx=10, pady=10)
        preview_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.preview_text = scrolledtext.ScrolledText(preview_frame, font=(self.segoe_ui, 10), height=10)
        self.preview_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Buttons frame
        buttons_frame = tk.Frame(main_frame, bg="#f2f2f2")
        buttons_frame.pack(fill=tk.X, pady=20)
        
        self.convert_button = ttk.Button(
            buttons_frame,
            text="Convert",
            command=self.start_conversion,
            state=tk.DISABLED
        )
        self.convert_button.pack(side=tk.RIGHT, padx=5)
        
        preview_button = ttk.Button(
            buttons_frame,
            text="Preview Formulas",
            command=self.preview_formulas
        )
        preview_button.pack(side=tk.RIGHT, padx=5)
        
    def browse_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("Tableau Workbook Files", "*.twb")])
        if file_path:
            self.file_path_var.set(file_path)
            self.convert_button.config(state=tk.NORMAL)
            self.status_var.set(f"Selected: {os.path.basename(file_path)}")
            
            # Extract and populate table names
            try:
                with open(file_path, 'r', encoding='utf-8') as xml_file:
                    xml_data = xml_file.read()
                root = ET.fromstring(xml_data)
                table_names = extract_table_names(root)
                
                # Update combobox values with extracted table names
                self.table_combobox['values'] = table_names
                if table_names:
                    self.table_name_var.set(table_names[0])  # Select first table by default
                    
                self.status_var.set(f"Found {len(table_names)} tables in the workbook")
            except Exception as e:
                messagebox.showwarning("Table Extraction Warning", 
                                      f"Could not extract table names: {str(e)}\nUsing default table name.")
    
    def update_progress(self, progress, status):
        self.progress_var.set(progress)
        self.status_var.set(status)
        self.root.update_idletasks()
    
    def preview_formulas(self):
        file_path = self.file_path_var.get()
        if not file_path or not os.path.isfile(file_path):
            messagebox.showerror("Error", "Please select a valid .twb file!")
            return
            
        try:
            self.update_progress(10, "Reading file for preview...")
            with open(file_path, 'r', encoding='utf-8') as xml_file:
                xml_data = xml_file.read()
                
            self.update_progress(30, "Extracting calculated fields...")
            root = ET.fromstring(xml_data)
            table_name = self.table_name_var.get()
            calculated_fields = extract_calculated_fields(root, table_name)
            
            self.update_progress(100, "Preview ready")
            
            # Show preview
            self.preview_text.delete(1.0, tk.END)
            if calculated_fields:
                for field in calculated_fields:
                    self.preview_text.insert(tk.END, f"Name: {field['name']}\n")
                    self.preview_text.insert(tk.END, f"Tableau: {field['tableau_formula']}\n")
                    self.preview_text.insert(tk.END, f"DAX ({field['type']}): {field['dax_formula']}\n")
                    self.preview_text.insert(tk.END, "-" * 80 + "\n\n")
            else:
                self.preview_text.insert(tk.END, "No calculated fields found in the workbook.")
                
        except Exception as e:
            self.update_progress(-1, f"Error in preview: {str(e)}")
            messagebox.showerror("Preview Error", str(e))
    
    def start_conversion(self):
        file_path = self.file_path_var.get()
        if not file_path or not os.path.isfile(file_path):
            messagebox.showerror("Error", "Please select a valid .twb file!")
            return
            
        dax_output_path = os.path.splitext(file_path)[0] + "_dax_calculations.json"
        table_name = self.table_name_var.get()
        
        self.convert_button.config(state=tk.DISABLED)
        
        def conversion_thread():
            success, error = main(file_path, dax_output_path, table_name, self.update_progress)
            
            if success:
                messagebox.showinfo("Success", f"Conversion completed!\n\nOutput:\n- {os.path.basename(dax_output_path)}")
            else:
                messagebox.showerror("Error", error)
                
            self.convert_button.config(state=tk.NORMAL)
        
        # Start conversion in a separate thread to keep UI responsive
        Thread(target=conversion_thread).start()

# Initialize application
if __name__ == "__main__":
    root = tk.Tk()
    app = ConverterApp(root)
    root.mainloop()


