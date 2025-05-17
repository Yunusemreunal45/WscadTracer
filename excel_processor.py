import pandas as pd
import numpy as np
import os
import io
import openpyxl
from openpyxl.styles import PatternFill, Font, Border, Side
from datetime import datetime
import json

class ExcelProcessor:
    """Class for processing and comparing Excel files"""
    
    def __init__(self):
        """Initialize Excel processor"""
        self.default_sheet_name = 'Sheet1'
        
        # Define color fills for changed cells
        self.added_fill = PatternFill(start_color='90EE90', end_color='90EE90', fill_type='solid')  # Light green
        self.removed_fill = PatternFill(start_color='FFC0CB', end_color='FFC0CB', fill_type='solid')  # Light red
        self.changed_fill = PatternFill(start_color='FFFFE0', end_color='FFFFE0', fill_type='solid')  # Light yellow
    
    def is_wscad_excel(self, filepath):
        """Check if a file is a WSCAD Excel file based on specific patterns"""
        try:
            if not filepath.lower().endswith(('.xlsx', '.xls')):
                return False
            
            # Load the Excel file
            try:
                df = pd.read_excel(filepath)
                
                # Check for typical WSCAD headers or patterns
                # This is a simplified check - adjust based on actual WSCAD format
                wscad_indicators = ['WSCAD', 'Material', 'PartNumber', 'Component', 'Schematic']
                
                # Check column names
                for indicator in wscad_indicators:
                    if any(indicator.lower() in col.lower() for col in df.columns):
                        return True
                
                # Check content
                first_row_data = df.iloc[0].astype(str) if len(df) > 0 else []
                for indicator in wscad_indicators:
                    if any(indicator.lower() in cell.lower() for cell in first_row_data):
                        return True
                
                return False
            except Exception as e:
                print(f"Error checking Excel format: {e}")
                return False
        except Exception as e:
            print(f"Error in is_wscad_excel: {e}")
            return False
    
    def process_file(self, filepath):
        """Process an Excel file for later comparison"""
        try:
            # Check if file exists
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"File not found: {filepath}")
            
            # Check if this is a WSCAD Excel file
            if not self.is_wscad_excel(filepath):
                print(f"Warning: {filepath} may not be a WSCAD Excel file")
            
            # Load the Excel file to verify it's readable
            df = pd.read_excel(filepath)
            
            # Return basic info about the file
            return {
                'filepath': filepath,
                'filename': os.path.basename(filepath),
                'sheet_count': len(pd.ExcelFile(filepath).sheet_names),
                'row_count': len(df),
                'column_count': len(df.columns),
                'processed_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            raise Exception(f"Error processing file: {e}")
    
    def compare_excel_files(self, filepath1, filepath2):
        """Compare two Excel files and identify differences"""
        try:
            # Check if files exist
            if not os.path.exists(filepath1):
                raise FileNotFoundError(f"First file not found: {filepath1}")
            if not os.path.exists(filepath2):
                raise FileNotFoundError(f"Second file not found: {filepath2}")
            
            # Load Excel files
            df1 = pd.read_excel(filepath1)
            df2 = pd.read_excel(filepath2)
            
            # Basic structure comparison
            structure_diff = []
            
            # Compare row counts
            if len(df1) != len(df2):
                structure_diff.append({
                    'type': 'structure',
                    'element': 'row_count',
                    'value1': len(df1),
                    'value2': len(df2),
                    'diff': abs(len(df1) - len(df2))
                })
            
            # Compare column counts and names
            if len(df1.columns) != len(df2.columns):
                structure_diff.append({
                    'type': 'structure',
                    'element': 'column_count',
                    'value1': len(df1.columns),
                    'value2': len(df2.columns),
                    'diff': abs(len(df1.columns) - len(df2.columns))
                })
            
            # Create sets of column names for comparison
            cols1 = set(str(col) for col in df1.columns)
            cols2 = set(str(col) for col in df2.columns)
            
            # Find added columns
            added_cols = cols2 - cols1
            if added_cols:
                structure_diff.append({
                    'type': 'structure',
                    'element': 'added_columns',
                    'value1': '',
                    'value2': ', '.join(added_cols),
                    'diff': len(added_cols)
                })
            
            # Find removed columns
            removed_cols = cols1 - cols2
            if removed_cols:
                structure_diff.append({
                    'type': 'structure',
                    'element': 'removed_columns',
                    'value1': ', '.join(removed_cols),
                    'value2': '',
                    'diff': len(removed_cols)
                })
            
            # Cell-by-cell comparison for common columns
            common_cols = cols1.intersection(cols2)
            cell_diff = []
            
            # Determine the number of rows to compare (minimum of both DataFrames)
            max_rows = min(len(df1), len(df2))
            
            for col in common_cols:
                col1_idx = df1.columns.get_loc(col) if col in df1.columns else None
                col2_idx = df2.columns.get_loc(col) if col in df2.columns else None
                
                if col1_idx is not None and col2_idx is not None:
                    for row_idx in range(max_rows):
                        cell1 = df1.iloc[row_idx, col1_idx]
                        cell2 = df2.iloc[row_idx, col2_idx]
                        
                        # Handle NaN values
                        if pd.isna(cell1) and pd.isna(cell2):
                            continue
                        elif pd.isna(cell1):
                            cell1 = ""
                        elif pd.isna(cell2):
                            cell2 = ""
                        
                        # Convert to strings for comparison
                        cell1_str = str(cell1)
                        cell2_str = str(cell2)
                        
                        if cell1_str != cell2_str:
                            cell_diff.append({
                                'type': 'cell',
                                'row': row_idx + 1,  # 1-based indexing for user display
                                'column': col,
                                'value1': cell1_str,
                                'value2': cell2_str,
                                'change_type': 'modified'
                            })
            
            # Additional rows in df2
            if len(df2) > len(df1):
                for row_idx in range(len(df1), len(df2)):
                    for col in df2.columns:
                        if col in common_cols:
                            cell_value = df2.iloc[row_idx, df2.columns.get_loc(col)]
                            if not pd.isna(cell_value) and str(cell_value).strip() != "":
                                cell_diff.append({
                                    'type': 'cell',
                                    'row': row_idx + 1,
                                    'column': col,
                                    'value1': '',
                                    'value2': str(cell_value),
                                    'change_type': 'added'
                                })
            
            # Additional rows in df1
            if len(df1) > len(df2):
                for row_idx in range(len(df2), len(df1)):
                    for col in df1.columns:
                        if col in common_cols:
                            cell_value = df1.iloc[row_idx, df1.columns.get_loc(col)]
                            if not pd.isna(cell_value) and str(cell_value).strip() != "":
                                cell_diff.append({
                                    'type': 'cell',
                                    'row': row_idx + 1,
                                    'column': col,
                                    'value1': str(cell_value),
                                    'value2': '',
                                    'change_type': 'removed'
                                })
            
            # Combine results
            all_diff = structure_diff + cell_diff
            
            return all_diff
        except Exception as e:
            raise Exception(f"Error comparing files: {e}")
    
    def generate_comparison_report(self, comparison_results):
        """Generate an Excel report from comparison results"""
        try:
            # Create a new workbook
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Comparison Results"
            
            # Add headers
            headers = ["Type", "Row", "Column", "Original Value", "New Value", "Change Type"]
            for col_idx, header in enumerate(headers, 1):
                cell = ws.cell(row=1, column=col_idx, value=header)
                cell.font = Font(bold=True)
            
            # Add data
            for row_idx, diff in enumerate(comparison_results, 2):
                ws.cell(row=row_idx, column=1, value=diff.get('type', ''))
                ws.cell(row=row_idx, column=2, value=diff.get('row', '') if 'row' in diff else diff.get('element', ''))
                ws.cell(row=row_idx, column=3, value=diff.get('column', ''))
                
                # Original value cell
                orig_cell = ws.cell(row=row_idx, column=4, value=diff.get('value1', ''))
                if diff.get('change_type') == 'removed':
                    orig_cell.fill = self.removed_fill
                
                # New value cell
                new_cell = ws.cell(row=row_idx, column=5, value=diff.get('value2', ''))
                if diff.get('change_type') == 'added':
                    new_cell.fill = self.added_fill
                
                # For modified cells, highlight both
                if diff.get('change_type') == 'modified':
                    orig_cell.fill = self.changed_fill
                    new_cell.fill = self.changed_fill
                
                ws.cell(row=row_idx, column=6, value=diff.get('change_type', ''))
            
            # Format the table
            for col in range(1, len(headers) + 1):
                column_letter = openpyxl.utils.get_column_letter(col)
                ws.column_dimensions[column_letter].width = 20
            
            # Create a summary sheet
            summary_ws = wb.create_sheet(title="Summary")
            
            # Add summary headers
            summary_ws.cell(row=1, column=1, value="Comparison Summary").font = Font(bold=True, size=14)
            summary_ws.cell(row=3, column=1, value="Category").font = Font(bold=True)
            summary_ws.cell(row=3, column=2, value="Count").font = Font(bold=True)
            
            # Calculate summary statistics
            structure_changes = sum(1 for diff in comparison_results if diff.get('type') == 'structure')
            cell_changes = sum(1 for diff in comparison_results if diff.get('type') == 'cell')
            added_cells = sum(1 for diff in comparison_results if diff.get('change_type') == 'added')
            removed_cells = sum(1 for diff in comparison_results if diff.get('change_type') == 'removed')
            modified_cells = sum(1 for diff in comparison_results if diff.get('change_type') == 'modified')
            
            # Add summary data
            summary_data = [
                ("Structure Changes", structure_changes),
                ("Cell Changes", cell_changes),
                ("Added Cells", added_cells),
                ("Removed Cells", removed_cells),
                ("Modified Cells", modified_cells),
                ("Total Changes", len(comparison_results))
            ]
            
            for idx, (category, count) in enumerate(summary_data, 4):
                summary_ws.cell(row=idx, column=1, value=category)
                summary_ws.cell(row=idx, column=2, value=count)
            
            # Format summary sheet
            summary_ws.column_dimensions['A'].width = 20
            summary_ws.column_dimensions['B'].width = 10
            
            # Save to a byte stream instead of a file
            output = io.BytesIO()
            wb.save(output)
            output.seek(0)
            
            return output
        except Exception as e:
            raise Exception(f"Error generating comparison report: {e}")
    
    def prepare_for_export(self, filepath):
        """Prepare Excel data for export to ERP system"""
        try:
            # Check if file exists
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"File not found: {filepath}")
            
            # Load the Excel file
            df = pd.read_excel(filepath)
            
            # Get basic file information
            file_info = {
                'filename': os.path.basename(filepath),
                'file_path': filepath,
                'sheet_count': len(pd.ExcelFile(filepath).sheet_names),
                'processed_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Convert DataFrame to dict format suitable for ERP
            data_rows = []
            for _, row in df.iterrows():
                # Clean row data: convert NaN to None
                cleaned_row = {}
                for col, value in row.items():
                    if pd.isna(value):
                        cleaned_row[str(col)] = None
                    else:
                        cleaned_row[str(col)] = value
                
                data_rows.append(cleaned_row)
            
            # Create the export data structure
            export_data = {
                'file_info': file_info,
                'data': data_rows
            }
            
            return export_data
        except Exception as e:
            raise Exception(f"Error preparing file for export: {e}")
