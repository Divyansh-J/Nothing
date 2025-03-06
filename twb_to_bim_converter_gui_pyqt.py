import os
import sys
import json
import xml.etree.ElementTree as ET
import time
from threading import Thread

# PyQt5 imports
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                           QTabWidget, QPushButton, QLabel, QLineEdit, QComboBox, 
                           QFileDialog, QMessageBox, QCheckBox, QRadioButton, QGroupBox, 
                           QScrollArea, QFrame, QSplitter, QTextEdit, QProgressBar,
                           QButtonGroup, QFormLayout, QGridLayout, QTextBrowser, QDialog, QDialogButtonBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, pyqtSlot, QSize
from PyQt5.QtGui import QIcon, QFont, QColor, QPalette

# Import conversion functions
from twb_to_bim_converter_csv import (
    create_model_bim,
    process_dax_calculations,
    validate_bim_file,
    extract_table_names_from_twb,
    extract_columns_from_twb,
    extract_calculations_from_twb,
    extract_relationships_from_twb,
    extract_measure_base_tables,
    generate_conversion_report
)
from BI_Convert_Tool import (
    extract_calculated_fields,
    main as extract_dax_formulas
)

# Worker class for background tasks
class ConversionWorkerWithDebug(QThread):
    # Signals for progress updates and completion
    progress_updated = pyqtSignal(int, str)
    conversion_completed = pyqtSignal(bool, str)
    
    def __init__(self, twb_path, output_path, table_name, use_dax_only, extract_dax):
        super().__init__()
        self.twb_path = twb_path
        self.output_path = output_path
        self.table_name = table_name
        self.use_dax_only = use_dax_only
        self.extract_dax = extract_dax

    def run(self):
        try:
            self.progress_updated.emit(0, "Starting conversion...")
            
            # Create output paths
            base_name = os.path.splitext(os.path.basename(self.twb_path))[0]
            bim_path = os.path.join(self.output_path, f"{base_name}_Model.bim")
            dax_path = os.path.join(self.output_path, f"{base_name}_dax_calculations.json")
            
            # Extract DAX formulas first if option is selected
            if self.extract_dax:
                self.progress_updated.emit(20, "Extracting DAX formulas from Tableau calculations...")
                
                def progress_callback(value, status):
                    self.progress_updated.emit(20 + int(value * 0.2), status)
                
                success, error = extract_dax_formulas(
                    self.twb_path, 
                    dax_path, 
                    self.table_name,
                    progress_callback
                )
                
                if not success:
                    self.progress_updated.emit(25, f"Warning: DAX extraction had issues: {error}")
            
            # Create the model - MODIFIED: Always set embed_csv_data to False
            self.progress_updated.emit(40, f"Creating Power BI model with file references (no data embedding)...")
            success = create_model_bim(
                self.twb_path, 
                bim_path, 
                embed_csv_data=False,  # Always set to False to skip data embedding
                extract_measures=not self.use_dax_only
            )
            
            if not success:
                raise Exception("Failed to create model")
            
            # Process DAX calculations if they were extracted or already exist
            if os.path.exists(dax_path):
                self.progress_updated.emit(60, "Applying DAX calculations with measure base table detection...")
                process_dax_calculations(
                    dax_path, 
                    bim_path, 
                    tableau_file_path=self.twb_path,
                    replace_existing=True
                )
            else:
                self.progress_updated.emit(60, "No DAX file found, skipping DAX processing...")
            
            # Read the model to get statistics
            self.progress_updated.emit(80, "Extracting model statistics...")
            with open(bim_path, 'r') as f:
                model = json.load(f)
            
            # Collect data for report and statistics
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
            
            # Validate the model
            self.progress_updated.emit(90, "Validating model...")
            is_valid, issues = validate_bim_file(bim_path)
            
            self.progress_updated.emit(100, "Conversion complete!")
            
            # Return success info with measure table assignments
            result_message = (
                f"Conversion completed successfully!\n\n"
                f"Statistics:\n"
                f"- Tables: {len(tables)}\n"
                f"- Measures: {len(measures)}\n"
                f"- Relationships: {len(relationships)}\n"
                f"- Validation Issues: {len(issues)}\n\n"
            )
            
            result_message += f"\nMeasure Table Assignments:\n"
            
            # Group measures by table for better readability
            measures_by_table = {}
            for measure in measures:
                table = measure['table']
                if table not in measures_by_table:
                    measures_by_table[table] = []
                measures_by_table[table].append(measure['name'])
            
            for table, table_measures in measures_by_table.items():
                result_message += f"\n{table}:\n"
                for measure_name in sorted(table_measures):
                    result_message += f"  - {measure_name}\n"
            
            result_message += f"\nOutput files:\n"
            result_message += f"- {os.path.basename(bim_path)}"
            
            if os.path.exists(dax_path):
                result_message += f"\n- {os.path.basename(dax_path)}"
            
            self.conversion_completed.emit(True, result_message)
            
        except Exception as e:
            self.progress_updated.emit(-1, f"Error: {str(e)}")
            self.conversion_completed.emit(False, f"Conversion failed: {str(e)}")

# Define functions to check data files
def check_data_files_exist(table_names, data_dir):
    """Check if all required CSV files exist."""
    missing_files = []
    
    for table in table_names:
        csv_path = os.path.join(data_dir, f"{table}.csv")
        if not os.path.exists(csv_path):
            missing_files.append(csv_path)
    
    return missing_files

class ModernConverterApp(QMainWindow):
    def __init__(self):
        super().__init__()
        
        # Window setup
        self.setWindowTitle("Tableau to Power BI Converter")
        self.setGeometry(100, 100, 1200, 800)
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QPushButton {
                padding: 8px 15px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton#primaryButton {
                background-color: #0078D4;
                color: white;
            }
            QPushButton#deployButton {
                background-color: #107C10;
                color: white;
            }
            QPushButton:hover {
                opacity: 0.9;
            }
            QGroupBox, QFrame#mainFrame {
                border: 1px solid #ddd;
                border-radius: 5px;
                margin-top: 10px;
                background-color: white;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
            }
            QScrollArea {
                border: none;
            }
        """)
        
        # Create central widget and main layout
        central_widget = QWidget()
        main_layout = QVBoxLayout(central_widget)
        
        # App title
        title_label = QLabel("Tableau to Power BI Converter")
        title_label.setStyleSheet("font-size: 24px; font-weight: bold; color: #0078D4; margin: 10px;")
        title_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title_label)
        
        # Create tab widget
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #ddd;
                border-radius: 5px;
                background: white;
            }
            QTabBar::tab {
                background: #f0f0f0;
                border: 1px solid #ddd;
                padding: 8px 12px;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background: #0078D4;
                color: white;
            }
        """)
        
        # Create tabs
        self.setup_conversion_tab()
        self.setup_settings_tab()
        self.setup_logs_tab()
        self.setup_help_tab()
        
        # Add tabs to tab widget
        main_layout.addWidget(self.tabs)
        
        # Status bar
        self.status_bar = self.statusBar()
        self.status_bar.showMessage("Ready")
        
        # Set central widget
        self.setCentralWidget(central_widget)
        
        # Initialize variables
        self.worker = None
        
    def setup_conversion_tab(self):
        # Create main widget for the tab
        conversion_tab = QWidget()
        
        # Create a scroll area to ensure all elements are visible
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame)
        
        # Create content widget that will be scrollable
        content_widget = QWidget()
        layout = QVBoxLayout(content_widget)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        # File selection area
        files_group = QGroupBox("Input/Output Files")
        files_layout = QGridLayout()
        
        # Tableau file
        files_layout.addWidget(QLabel("Tableau Workbook:"), 0, 0)
        self.twb_path_edit = QLineEdit()
        files_layout.addWidget(self.twb_path_edit, 0, 1)
        browse_twb_btn = QPushButton("Browse")
        browse_twb_btn.clicked.connect(self.browse_twb_file)
        browse_twb_btn.setProperty("class", "primaryButton")
        files_layout.addWidget(browse_twb_btn, 0, 2)
        
        # Output directory
        files_layout.addWidget(QLabel("Output Directory:"), 1, 0)
        self.output_path_edit = QLineEdit()
        files_layout.addWidget(self.output_path_edit, 1, 1)
        browse_output_btn = QPushButton("Browse")
        browse_output_btn.clicked.connect(self.browse_output_dir)
        browse_output_btn.setProperty("class", "primaryButton")
        files_layout.addWidget(browse_output_btn, 1, 2)
        
        files_group.setLayout(files_layout)
        layout.addWidget(files_group)
        
        # Options area
        options_group = QGroupBox("Conversion Options")
        options_layout = QGridLayout()
        
        # Table selection
        options_layout.addWidget(QLabel("Default Table Name:"), 0, 0)
        self.table_combo = QComboBox()
        self.table_combo.addItem("YourTable")
        options_layout.addWidget(self.table_combo, 0, 1)
        
        # Generate report option
        self.generate_report_check = QCheckBox("Generate Conversion Report")
        self.generate_report_check.setChecked(True)
        options_layout.addWidget(self.generate_report_check, 1, 0)
        
        # DAX extraction option
        self.extract_dax_check = QCheckBox("Extract DAX Formulas First")
        self.extract_dax_check.setChecked(True)
        options_layout.addWidget(self.extract_dax_check)
        
        # Data handling options - MODIFIED: Removed embedding option
        data_group = QGroupBox("Data Connection Options")
        data_layout = QVBoxLayout()
        
        # REMOVED: Embed CSV data option
        # Instead, add an informative label
        info_label = QLabel("Using file references for data sources (CSV files)")
        info_label.setStyleSheet("color: #666; font-style: italic;")
        data_layout.addWidget(info_label)
        
        # Keep data connection options dropdown
        self.data_conn_combo = QComboBox()
        self.data_conn_combo.addItems([
            "CSV Files (Recommended for this tool)",
            "Direct Query (needs manual setup)",
            "Import from Database (needs manual setup)"
        ])
        data_layout.addWidget(self.data_conn_combo)
        
        data_group.setLayout(data_layout)
        options_layout.addWidget(data_group, 2, 0, 1, 2)  # Span 2 columns
        
        # Measure handling group
        measure_group = QGroupBox("Measure Handling")
        measure_layout = QVBoxLayout()
        
        self.measure_mode_group = QButtonGroup()
        self.use_dax_only_radio = QRadioButton("Use DAX File Only (Recommended)")
        self.use_dax_only_radio.setChecked(True)
        self.extract_measures_radio = QRadioButton("Extract from TWB Directly")
        
        measure_layout.addWidget(self.use_dax_only_radio)
        measure_layout.addWidget(self.extract_measures_radio)
        
        # Add radio button for enhanced table assignment
        self.enhanced_table_assignment = QCheckBox("Smart Table Assignment for Measures")
        self.enhanced_table_assignment.setChecked(True)
        self.enhanced_table_assignment.setToolTip("Analyzes each measure to place it in the most relevant table")
        measure_layout.addWidget(self.enhanced_table_assignment)
        
        self.measure_mode_group.addButton(self.use_dax_only_radio)
        self.measure_mode_group.addButton(self.extract_measures_radio)
        
        measure_group.setLayout(measure_layout)
        options_layout.addWidget(measure_group, 3, 0, 1, 2)  # Span 2 columns
        
        options_group.setLayout(options_layout)
        layout.addWidget(options_group)
        
        # Preview area
        preview_group = QGroupBox("Preview")
        preview_layout = QVBoxLayout()
        
        self.preview_text = QTextEdit()
        self.preview_text.setReadOnly(True)
        preview_layout.addWidget(self.preview_text)
        
        preview_group.setLayout(preview_layout)
        layout.addWidget(preview_group)
        
        # Progress bar
        progress_layout = QHBoxLayout()
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        progress_layout.addWidget(self.progress_bar)
        layout.addLayout(progress_layout)
        
        # Action buttons
        buttons_layout = QHBoxLayout()
        
        preview_btn = QPushButton("Preview")
        preview_btn.setObjectName("primaryButton")
        preview_btn.clicked.connect(self.preview_conversion)
        buttons_layout.addWidget(preview_btn)
        
        deploy_btn = QPushButton("Deploy to Power BI")
        deploy_btn.setObjectName("deployButton")
        deploy_btn.clicked.connect(self.deploy_to_powerbi)
        buttons_layout.addWidget(deploy_btn)
        
        buttons_layout.addStretch(1)  # Spacer
        
        convert_btn = QPushButton("Convert")
        convert_btn.setObjectName("primaryButton")
        convert_btn.clicked.connect(self.start_conversion)
        buttons_layout.addWidget(convert_btn)
        
        clean_dax_btn = QPushButton("Clean DAX JSON")
        clean_dax_btn.setObjectName("primaryButton")
        clean_dax_btn.clicked.connect(self.clean_dax_json)
        buttons_layout.addWidget(clean_dax_btn)
        
        layout.addLayout(buttons_layout)
        
        # Set the content widget into the scroll area
        scroll_area.setWidget(content_widget)
        
        # Create main layout for the tab
        main_tab_layout = QVBoxLayout(conversion_tab)
        main_tab_layout.addWidget(scroll_area)
        
        # Add the tab
        self.tabs.addTab(conversion_tab, "Conversion")
        
    def setup_settings_tab(self):
        settings_tab = QWidget()
        layout = QVBoxLayout(settings_tab)
        
        settings_group = QGroupBox("Converter Settings")
        settings_layout = QGridLayout()
        
        # Date table range
        settings_layout.addWidget(QLabel("Date Table Range:"), 0, 0)
        self.date_start_edit = QLineEdit("2015")
        settings_layout.addWidget(self.date_start_edit, 0, 1)
        settings_layout.addWidget(QLabel("to"), 0, 2)
        self.date_end_edit = QLineEdit("2030")
        settings_layout.addWidget(self.date_end_edit, 0, 3)
        
        # Data type mappings
        settings_layout.addWidget(QLabel("Default Data Type Mappings:"), 1, 0, 1, 4)
        
        mappings = [
            ("Tableau Integer", "Power BI Int64"),
            ("Tableau Real", "Power BI Double"),
            ("Tableau String", "Power BI String"),
            ("Tableau Date", "Power BI DateTime"),
            ("Tableau Boolean", "Power BI Boolean")
        ]
        
        for i, (tableau_type, powerbi_type) in enumerate(mappings):
            settings_layout.addWidget(QLabel(tableau_type), i+2, 0)
            settings_layout.addWidget(QLabel("→"), i+2, 1)
            settings_layout.addWidget(QLabel(powerbi_type), i+2, 2, 1, 2)
            
        settings_group.setLayout(settings_layout)
        layout.addWidget(settings_group)
        layout.addStretch(1)  # Add stretch to push content to the top
        
        self.tabs.addTab(settings_tab, "Settings")
        
    def setup_logs_tab(self):
        logs_tab = QWidget()
        layout = QVBoxLayout(logs_tab)
        
        # Log text area
        self.log_text = QTextBrowser()
        layout.addWidget(self.log_text)
        
        # Buttons
        buttons_layout = QHBoxLayout()
        buttons_layout.addStretch(1)
        
        save_btn = QPushButton("Save Logs")
        save_btn.setObjectName("primaryButton")
        save_btn.clicked.connect(self.save_logs)
        buttons_layout.addWidget(save_btn)
        
        clear_btn = QPushButton("Clear Logs")
        clear_btn.setObjectName("primaryButton")
        clear_btn.clicked.connect(self.clear_logs)
        buttons_layout.addWidget(clear_btn)
        
        layout.addLayout(buttons_layout)
        self.tabs.addTab(logs_tab, "Logs")

    def setup_help_tab(self):
        help_tab = QWidget()
        layout = QVBoxLayout(help_tab)
        
        # Title
        title_label = QLabel("Using the Tableau to Power BI Converter")
        title_label.setStyleSheet("font-size: 16px; font-weight: bold;")
        layout.addWidget(title_label)
        
        # Help content
        help_content = QTextBrowser()
        help_content.setOpenExternalLinks(True)
        
        help_text = """
        <h3>CONVERSION WORKFLOW</h3>
        <ol>
            <li>Select a Tableau workbook (.twb file) using the Browse button.</li>
            <li>Choose an output directory where the converted files will be saved.</li>
            <li>Click Preview to see what will be converted.</li>
            <li>Click Convert to generate the Power BI Model (.bim) file.</li>
            <li>After conversion, click Deploy to Power BI to publish your model.</li>
        </ol>
        
        <h3>DEPLOYMENT OPTIONS</h3>
        <p>There are two ways to deploy your converted model to Power BI:</p>
        
        <h4>Option 1: Using the Deploy button</h4>
        <p>The Deploy button will guide you through deploying your model to the Power BI service. You'll need to provide:</p>
        <ul>
            <li>Power BI workspace</li>
            <li>Dataset name</li>
            <li>Authentication (if prompted)</li>
        </ul>
        
        <h4>Option 2: Manual import in Power BI Desktop</h4>
        <ol>
            <li>Open Power BI Desktop</li>
            <li>Go to File > Open > Browse Reports</li>
            <li>Change the file type filter to 'All Files (*.*)'</li>
            <li>Navigate to your output folder and select the .bim file</li>
            <li>Use Power BI Desktop's Publish feature to deploy to the service</li>
        </ol>
        
        <h3>POWER BI WEB SERVICE COMPATIBILITY</h3>
        <p>For compatibility with Power BI Web Service, make sure to:</p>
        <ol>
            <li>Check the 'Embed CSV Data' option to include data directly in the model</li>
            <li>Configure a data gateway if using local data sources</li>
            <li>Ensure your Power BI account has permission to publish datasets</li>
        </ol>
        """
        
        help_content.setHtml(help_text)
        layout.addWidget(help_content)
        
        self.tabs.addTab(help_tab, "Help")
        
    def browse_twb_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Tableau Workbook", "", "Tableau Workbooks (*.twb);;All Files (*.*)"
        )
        if file_path:
            self.twb_path_edit.setText(file_path)
            self.update_table_names(file_path)
            
            # Update output directory to match input directory
            if not self.output_path_edit.text():
                self.output_path_edit.setText(os.path.dirname(file_path))
            
            self.log_message(f"Selected Tableau workbook: {file_path}")
            
    def browse_output_dir(self):
        dir_path = QFileDialog.getExistingDirectory(self, "Select Output Directory")
        if dir_path:
            self.output_path_edit.setText(dir_path)
            
    def update_table_names(self, twb_path):
        """Extract table names from the selected workbook"""
        try:
            self.log_message("Extracting tables from workbook...")
            tree = ET.parse(twb_path)
            root = tree.getroot()
            table_names = extract_table_names_from_twb(root)
            
            # Clear and update combobox
            self.table_combo.clear()
            
            if not table_names:
                self.log_message("Warning: No tables found in the workbook")
                self.preview_text.append("No tables found in the selected workbook.")
                self.table_combo.addItem("YourTable")
                return
            
            for table in table_names:
                self.table_combo.addItem(table)
                
            self.log_message(f"Found {len(table_names)} tables in the workbook")
            
        except Exception as e:
            self.log_message(f"Error extracting table names: {str(e)}")
            QMessageBox.warning(
                self, 
                "Table Extraction Error", 
                f"Could not extract tables from the workbook.\nError: {str(e)}"
            )
            
    def preview_conversion(self):
        if not self.validate_inputs():
            return
            
        self.preview_text.clear()
        self.preview_text.append("Analyzing Tableau workbook...\n")
        
        try:
            tree = ET.parse(self.twb_path_edit.text())
            root = tree.getroot()
            
            # Extract and show table information
            tables = extract_table_names_from_twb(root)
            self.preview_text.append("Tables found:")
            for table in tables:
                self.preview_text.append(f"  - {table}")
                
                # Get column info for each table
                columns = extract_columns_from_twb(root, table)
                if columns:
                    self.preview_text.append(f"    Columns: {len(columns)}")
                    # Show a few columns as examples
                    sample_cols = columns[:3] if len(columns) > 3 else columns
                    for col in sample_cols:
                        data_type = col.get('dataType', 'unknown')
                        self.preview_text.append(f"      - {col['name']} ({data_type})")
                    if len(columns) > 3:
                        self.preview_text.append(f"      - ... and {len(columns)-3} more columns")
            
            # Extract and show measure-to-table mappings
            self.preview_text.append("\nMeasure Base Table Analysis:")
            measure_tables = extract_measure_base_tables(root)
            if measure_tables:
                self.preview_text.append(f"Found {len(measure_tables)} measures with base table assignments:")
                # Show a few examples
                sample_measures = list(measure_tables.items())[:5]
                for measure, table in sample_measures:
                    self.preview_text.append(f"  - {measure} → {table}")
                if len(measure_tables) > 5:
                    self.preview_text.append(f"  - ... and {len(measure_tables)-5} more measures")
            else:
                self.preview_text.append("No measure base tables found.")
            
            # Check for calculations
            calculations = extract_calculations_from_twb(root)
            if calculations:
                self.preview_text.append(f"\nCalculations found: {len(calculations)}")
                # Show a few examples
                sample_calcs = calculations[:3] if len(calculations) > 3 else calculations
                for calc in sample_calcs:
                    name = calc.get('name', 'Unnamed')
                    calc_type = calc.get('type', 'unknown')
                    self.preview_text.append(f"  - {name} ({calc_type})")
                if len(calculations) > 3:
                    self.preview_text.append(f"  - ... and {len(calculations)-3} more calculations")
            
            # Look for relationships
            relationships = extract_relationships_from_twb(root, tables)
            if relationships:
                self.preview_text.append(f"\nRelationships found: {len(relationships)}")
                # Show a few examples
                sample_rels = relationships[:3] if len(relationships) > 3 else relationships
                for rel in sample_rels:
                    from_table = rel.get('fromTable', 'Unknown')
                    from_col = rel.get('fromColumn', 'Unknown')
                    to_table = rel.get('toTable', 'Unknown')
                    to_col = rel.get('toColumn', 'Unknown')
                    self.preview_text.append(f"  - {from_table}[{from_col}] → {to_table}[{to_col}]")
                if len(relationships) > 3:
                    self.preview_text.append(f"  - ... and {len(relationships)-3} more relationships")
            
            # Show conversion settings
            self.preview_text.append("\nConversion Settings:")
            self.preview_text.append(f"  Default Table: {self.table_combo.currentText()}")
            self.preview_text.append(f"  Generate Report: {self.generate_report_check.isChecked()}")
            self.preview_text.append(f"  Date Range: {self.date_start_edit.text()} - {self.date_end_edit.text()}")
            
            # Check data directory for CSVs
            data_dir = os.path.join(os.path.dirname(self.twb_path_edit.text()), 'data')
            missing_files = check_data_files_exist(tables, data_dir)
            if missing_files:
                self.preview_text.append("\nWarning: Missing CSV files:")
                for i, missing in enumerate(missing_files[:5]):
                    self.preview_text.append(f"  - {os.path.basename(missing)}")
                if len(missing_files) > 5:
                    self.preview_text.append(f"  - ... and {len(missing_files)-5} more")
                self.preview_text.append("\nModel will be created but data source connections may fail.")
            
        except Exception as e:
            self.preview_text.append(f"\nError during preview: {str(e)}")
            self.log_message(f"Preview error: {str(e)}")
            
    def start_conversion(self):
        if not self.validate_inputs():
            return
        
        # Get parameters 
        twb_path = self.twb_path_edit.text()
        output_path = self.output_path_edit.text()
        table_name = self.table_combo.currentText()
        # REMOVED: Embed CSV option is no longer needed
        use_dax_only = self.use_dax_only_radio.isChecked()
        extract_dax = self.extract_dax_check.isChecked()
        use_smart_table_assignment = self.enhanced_table_assignment.isChecked()
        
        # Add debug info
        self.log_message(f"Starting conversion with settings:")
        self.log_message(f"- TWB file: {twb_path}")
        self.log_message(f"- Output path: {output_path}")
        self.log_message(f"- Default table name: {table_name}")
        self.log_message(f"- Use DAX file only: {use_dax_only}")
        self.log_message(f"- Extract DAX first: {extract_dax}")
        self.log_message(f"- Smart table assignment: {use_smart_table_assignment}")
        
        # Create worker thread with additional parameters
        # REMOVED: Removed embed_csv parameter from worker
        self.worker = ConversionWorkerWithDebug(twb_path, output_path, table_name, use_dax_only, extract_dax)
        self.worker.progress_updated.connect(self.update_progress)
        self.worker.conversion_completed.connect(self.conversion_completed)
        self.worker.start()
        
        # Disable inputs during conversion
        self.disable_inputs()

    def deploy_to_powerbi(self):
        """Handle deployment to Power BI"""
        if not self.validate_inputs():
            return
            
        # Check if the model file exists
        twb_path = self.twb_path_edit.text()
        output_path = self.output_path_edit.text()
        base_name = os.path.splitext(os.path.basename(twb_path))[0]
        bim_path = os.path.join(output_path, f"{base_name}_Model.bim")
        
        if not os.path.exists(bim_path):
            QMessageBox.warning(
                self,
                "No Model Found",
                "Please convert the workbook first before deploying."
            )
            return
        
        # Create the deployment dialog
        deploy_dialog = QDialog(self)
        deploy_dialog.setWindowTitle("Deploy to Power BI")
        deploy_dialog.setMinimumWidth(450)
        
        layout = QVBoxLayout(deploy_dialog)
        
        # Title
        title = QLabel("Deploy to Power BI")
        title.setStyleSheet("font-size: 16px; font-weight: bold;")
        layout.addWidget(title)
        
        # Form for workspace and dataset name
        form = QFormLayout()
        
        workspace_combo = QComboBox()
        workspace_combo.addItem("My workspace")
        workspace_combo.addItem("Create new workspace")
        form.addRow("Workspace:", workspace_combo)
        
        dataset_edit = QLineEdit(base_name)
        form.addRow("Dataset Name:", dataset_edit)
        
        layout.addLayout(form)
        
        # Options
        options_group = QGroupBox("Deployment Options")
        options_layout = QVBoxLayout()
        
        overwrite_check = QCheckBox("Overwrite if exists")
        overwrite_check.setChecked(True)
        options_layout.addWidget(overwrite_check)
        
        create_report_check = QCheckBox("Create default report")
        create_report_check.setChecked(True)
        options_layout.addWidget(create_report_check)
        
        options_group.setLayout(options_layout)
        layout.addWidget(options_group)
        
        # Notice
        notice = QLabel("Note: You may be prompted to sign in to your Power BI account.")
        notice.setStyleSheet("font-style: italic; color: #666;")
        layout.addWidget(notice)
        
        # Button box
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(deploy_dialog.accept)
        button_box.rejected.connect(deploy_dialog.reject)
        layout.addWidget(button_box)
        
        # Execute the dialog
        if deploy_dialog.exec_() == QDialog.Accepted:
            workspace = workspace_combo.currentText()
            dataset_name = dataset_edit.text()
            
            self.log_message(f"Starting deployment to Power BI workspace: {workspace}")
            self.log_message(f"Dataset name: {dataset_name}")
            
            QMessageBox.information(
                self,
                "Deployment Steps",
                "To deploy to Power BI:\n\n"
                "1. Open Power BI Desktop\n"
                "2. Use 'Open Report' to select the generated BIM file\n"
                "3. Click 'Publish' in Power BI Desktop\n"
                "4. Select your destination workspace\n\n"
                "Your model has been prepared for deployment!"
            )

    def validate_inputs(self):
        """Validate user inputs before starting conversion"""
        twb_path = self.twb_path_edit.text()
        output_path = self.output_path_edit.text()
        
        if not twb_path:
            QMessageBox.critical(self, "Error", "Please select a Tableau workbook file.")
            return False
            
        if not output_path:
            QMessageBox.critical(self, "Error", "Please select an output directory.")
            return False
            
        if not os.path.exists(twb_path):
            QMessageBox.critical(self, "Error", "Selected Tableau workbook file does not exist.")
            return False
            
        # Validate date range
        try:
            start_year = int(self.date_start_edit.text())
            end_year = int(self.date_end_edit.text())
            if start_year >= end_year:
                QMessageBox.warning(
                    self, 
                    "Invalid Date Range", 
                    "Start year must be less than end year."
                )
                return False
        except ValueError:
            QMessageBox.warning(
                self, 
                "Invalid Date Range", 
                "Date range must contain valid years (numeric values)."
            )
            return False
            
        # Create output directory if it doesn't exist
        if not os.path.exists(output_path):
            try:
                os.makedirs(output_path)
                self.log_message(f"Created output directory: {output_path}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Could not create output directory: {str(e)}")
                return False
                
        return True
        
    @pyqtSlot(int, str)
    def update_progress(self, progress, status):
        if progress < 0:
            self.status_bar.showMessage(status)
            self.log_message(f"Error: {status}")
            return
            
        if 0 <= progress <= 100:
            self.progress_bar.setValue(progress)
            self.status_bar.showMessage(status)
            self.log_message(status)
    
    def log_message(self, message):
        """Add a log message with timestamp to the log tab"""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")
        
    def clear_logs(self):
        """Clear all log messages"""
        self.log_text.clear()
    
    def save_logs(self):
        """Save log messages to a file"""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Logs", "", "Log Files (*.log);;All Files (*.*)"
        )
        if file_path:
            try:
                with open(file_path, 'w') as f:
                    f.write(self.log_text.toPlainText())
                self.log_message(f"Logs saved to: {file_path}")
            except Exception as e:
                self.log_message(f"Error saving logs: {str(e)}")
    
    def disable_inputs(self):
        """Disable inputs during conversion process"""
        self.twb_path_edit.setEnabled(False)
        self.output_path_edit.setEnabled(False)
        self.table_combo.setEnabled(False)
        self.generate_report_check.setEnabled(False)
        self.extract_dax_check.setEnabled(False)
        # REMOVED: self.embed_csv_check.setEnabled(False)
        self.use_dax_only_radio.setEnabled(False)
        self.extract_measures_radio.setEnabled(False)
        
        # Disable all buttons in the conversion tab
        for child in self.tabs.currentWidget().findChildren(QPushButton):
            child.setEnabled(False)
    
    def enable_inputs(self):
        """Re-enable inputs after conversion process"""
        self.twb_path_edit.setEnabled(True)
        self.output_path_edit.setEnabled(True)
        self.table_combo.setEnabled(True)
        self.generate_report_check.setEnabled(True)
        self.extract_dax_check.setEnabled(True)
        # REMOVED: self.embed_csv_check.setEnabled(True)
        self.use_dax_only_radio.setEnabled(True)
        self.extract_measures_radio.setEnabled(True)
        
        # Re-enable all buttons in the conversion tab
        for child in self.tabs.currentWidget().findChildren(QPushButton):
            child.setEnabled(True)
    
    @pyqtSlot(bool, str)
    def conversion_completed(self, success, message):
        """Handle completion of conversion process"""
        self.enable_inputs()
        
        if success:
            QMessageBox.information(self, "Conversion Complete", message)
        else:
            QMessageBox.critical(self, "Conversion Failed", message)

    def clean_dax_json(self):
        """Clean any CSV references from DAX formulas in the JSON file"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select DAX JSON File to Clean", "", "JSON Files (*.json);;All Files (*.*)"
        )
        if not file_path:
            return
            
        try:
            self.log_message(f"Cleaning DAX JSON file: {file_path}")
            
            # Read the JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Clean each formula
            changes_made = 0
            for item in data:
                if 'dax_formula' in item:
                    original = item['dax_formula']
                    # Fix references to columns with CSV file extensions
                    cleaned = re.sub(r"'([^']+)'\[([^\]]+) \([^)]+\.csv\)\]", r"'\1'[\2]", original)
                    cleaned = re.sub(r"\[([^\]]+) \([^)]+\.csv\)\]", r"[\1]", cleaned)
                    
                    if original != cleaned:
                        item['dax_formula'] = cleaned
                        changes_made += 1
                        self.log_message(f"Cleaned formula: {item.get('name', 'unnamed')}")
            
            # Save back to file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            
            message = f"DAX cleaning complete: {changes_made} formulas updated"
            self.log_message(message)
            QMessageBox.information(self, "DAX Cleaning Complete", message)
            
        except Exception as e:
            error_message = f"Error cleaning DAX file: {str(e)}"
            self.log_message(error_message)
            QMessageBox.warning(self, "Error", error_message)

# Main entry point
def main():
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle("Fusion")
    
    # Create and show the main window
    window = ModernConverterApp()
    window.show()
    
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
