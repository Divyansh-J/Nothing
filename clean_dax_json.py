import json
import re
import sys
import os

def clean_csv_references(dax_formula):
    """Clean DAX formula by removing CSV references from column names."""
    # Fix references to columns with CSV file extensions
    cleaned = re.sub(r"'([^']+)'\[([^\]]+) \([^)]+\.csv\)\]", r"'\1'[\2]", dax_formula)
    cleaned = re.sub(r"\[([^\]]+) \([^)]+\.csv\)\]", r"[\1]", cleaned)
    return cleaned

def process_json_file(json_path):
    """Process the DAX JSON file to clean up column references."""
    print(f"Processing: {json_path}")
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Clean each formula
        for item in data:
            if 'dax_formula' in item:
                original = item['dax_formula']
                cleaned = clean_csv_references(original)
                item['dax_formula'] = cleaned
                if original != cleaned:
                    print(f"Cleaned formula: {original} -> {cleaned}")
        
        # Save back to file
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"Successfully cleaned DAX formulas in {json_path}")
        return True
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return False

def main():
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        # Default path
        file_path = './Book1_dax_calculations.json'
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return 1
    
    success = process_json_file(file_path)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
