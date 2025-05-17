import pandas as pd
import numpy as np
import os
import io
import openpyxl
from openpyxl.styles import PatternFill, Font, Border, Side
from datetime import datetime
import json
import glob

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
        """Check if file is an Excel file"""
        return filepath.lower().endswith(('.xlsx', '.xls'))

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

    def find_latest_excel_files(self, directory='.', pattern='*.xlsx'):
        """Find the two most recent Excel files in a directory"""
        try:
            # Get all Excel files (both .xlsx and .xls)
            xlsx_files = glob.glob(os.path.join(directory, '*.xlsx'))
            xls_files = glob.glob(os.path.join(directory, '*.xls'))
            all_files = xlsx_files + xls_files

            # Filter out directories, hidden files and check if they are WSCAD Excel files
            excel_files = []
            for f in all_files:
                if os.path.isfile(f) and not os.path.basename(f).startswith('.'):
                    try:
                        if self.is_wscad_excel(f):
                            excel_files.append(f)
                    except Exception as e:
                        print(f"WSCAD format kontrolünde hata: {e}")
                        continue

            if len(excel_files) < 2:
                raise ValueError(f"En az iki WSCAD Excel dosyası bulunamadı: {directory} dizininde.")

            # Sort by modification time (newest first)
            excel_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

            # Return the two most recent files
            return excel_files[0], excel_files[1]
        except Exception as e:
            raise Exception(f"Excel dosyalarını bulurken hata: {e}")

    def auto_compare_latest_files(self, directory='.'):
        """Automatically find and compare the two most recent Excel files"""
        try:
            print("Otomatik karşılaştırma başlatılıyor...")
            # İzlenen klasörden Excel dosyalarını bul
            excel_files = self.list_excel_files(directory)
            if len(excel_files) < 2:
                raise ValueError(f"En az iki Excel dosyası gerekli. Dizinde {len(excel_files)} dosya bulundu.")

            print(f"Bulunan Excel dosyaları: {len(excel_files)}")

            # En son iki dosyayı al
            file1 = excel_files[0]['filepath']
            file2 = excel_files[1]['filepath']

            if not os.path.exists(file1) or not os.path.exists(file2):
                raise FileNotFoundError("Karşılaştırılacak dosyalar bulunamadı")

            print(f"En son Excel dosyaları karşılaştırılıyor:\n1. {file1}\n2. {file2}")

            # Process both files
            info1 = self.process_file(file1)
            info2 = self.process_file(file2)

            # Compare the files
            comparison_data = self.compare_excel_files(file1, file2)

            # Create comparison result structure
            comparison_result = {
                'file1': {'filepath': file1},
                'file2': {'filepath': file2},
                'comparison_data': comparison_data,
                'comparison_count': len(comparison_data)
            }

            # Generate a report
            report_data = self.generate_comparison_report(comparison_data)

            # Create a timestamped filename for the report
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_filename = f"comparison_report_{timestamp}.xlsx"

            # Save the report
            with open(report_filename, "wb") as f:
                f.write(report_data.getvalue())

            return {
                'file1': info1,
                'file2': info2,
                'comparison_count': len(comparison_data),
                'comparison_data': comparison_data,
                'report_file': report_filename
            }
        except Exception as e:
            raise Exception(f"Otomatik karşılaştırma işleminde hata: {e}")

    def compare_specific_files(self, filepath1, filepath2):
        """Compare two specific Excel files by filepath and generate a report"""
        try:
            # Check if files exist
            if not os.path.exists(filepath1):
                raise FileNotFoundError(f"Birinci dosya bulunamadı: {filepath1}")
            if not os.path.exists(filepath2):
                raise FileNotFoundError(f"İkinci dosya bulunamadı: {filepath2}")

            print(f"Belirtilen Excel dosyaları karşılaştırılıyor:\n1. {filepath1}\n2. {filepath2}")

            # Process both files
            info1 = self.process_file(filepath1)
            info2 = self.process_file(filepath2)

            # Compare the files
            comparison_results = self.compare_excel_files(filepath1, filepath2)

            # Generate a report
            report_data = self.generate_comparison_report(comparison_results)

            # Create a timestamped filename for the report
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_filename = f"comparison_report_{timestamp}.xlsx"

            # Save the report
            with open(report_filename, "wb") as f:
                f.write(report_data.getvalue())

            return {
                'file1': info1,
                'file2': info2,
                'comparison_count': len(comparison_results),
                'report_file': report_filename
            }
        except Exception as e:
            raise Exception(f"Dosya karşılaştırma işleminde hata: {e}")

    def compare_excel_files(self, filepath1, filepath2):
        """Compare two Excel files and identify differences, focusing on column comparisons for WSCAD files"""
        try:
            # Validate file paths
            if not filepath1 or not filepath2:
                raise ValueError("Dosya yolları boş olamaz")

            # Check if files exist
            if not os.path.exists(filepath1):
                raise FileNotFoundError(f"Birinci dosya bulunamadı: {filepath1}")
            if not os.path.exists(filepath2):
                raise FileNotFoundError(f"İkinci dosya bulunamadı: {filepath2}")

            # Validate file extensions
            if not (filepath1.lower().endswith(('.xlsx', '.xls')) and filepath2.lower().endswith(('.xlsx', '.xls'))):
                raise ValueError("Dosyalar Excel formatında (.xlsx veya .xls) olmalıdır")

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
                    'diff': abs(len(df1) - len(df2)),
                    'description': f"Satır sayısı değişti: {len(df1)} -> {len(df2)}"
                })

            # Compare column counts and names
            if len(df1.columns) != len(df2.columns):
                structure_diff.append({
                    'type': 'structure',
                    'element': 'column_count',
                    'value1': len(df1.columns),
                    'value2': len(df2.columns),
                    'diff': abs(len(df1.columns) - len(df2.columns)),
                    'description': f"Sütun sayısı değişti: {len(df1.columns)} -> {len(df2.columns)}"
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
                    'diff': len(added_cols),
                    'description': f"Eklenen sütunlar: {', '.join(added_cols)}"
                })

            # Find removed columns
            removed_cols = cols1 - cols2
            if removed_cols:
                structure_diff.append({
                    'type': 'structure',
                    'element': 'removed_columns',
                    'value1': ', '.join(removed_cols),
                    'value2': '',
                    'diff': len(removed_cols),
                    'description': f"Kaldırılan sütunlar: {', '.join(removed_cols)}"
                })

            # Cell-by-cell comparison for common columns
            common_cols = cols1.intersection(cols2)
            cell_diff = []

            # WSCAD Excel dosyaları için sütun bazlı karşılaştırma
            # Typik WSCAD sütunları için özel kontroller
            wscad_key_columns = ['Material', 'Malzeme', 'PartNumber', 'Parça No', 'Component', 'Komponent', 'Ref', 'Miktar', 'Quantity', 'Değer', 'Value']

            # Önce WSCAD anahtar sütunlarını karşılaştır, sonra diğer sütunları
            priority_cols = [col for col in common_cols if any(key in col for key in wscad_key_columns)]
            other_cols = [col for col in common_cols if col not in priority_cols]

            # Sıralama: önce anahtar sütunlar, sonra diğerleri
            sorted_cols = priority_cols + other_cols

            # Determine the number of rows to compare (minimum of both DataFrames)
            max_rows = min(len(df1), len(df2))

            for col in sorted_cols:
                col1_idx = df1.columns.get_loc(col) if col in df1.columns else None
                col2_idx = df2.columns.get_loc(col) if col in df2.columns else None

                if col1_idx is not None and col2_idx is not None:
                    # Tüm sütun değerlerini karşılaştırma
                    col_values1 = df1.iloc[:max_rows, col1_idx].fillna("").astype(str)
                    col_values2 = df2.iloc[:max_rows, col2_idx].fillna("").astype(str)

                    # Farklı değerleri bulma
                    diff_mask = col_values1 != col_values2
                    diff_indices = diff_mask[diff_mask].index

                    # Sütun bazında genel değişimi belirtme
                    if len(diff_indices) > 0:
                        cell_diff.append({
                            'type': 'column',
                            'column': col,
                            'diff_count': len(diff_indices),
                            'description': f"'{col}' sütununda {len(diff_indices)} hücrede değişiklik",
                            'change_type': 'modified'
                        })

                    # Her bir hücre değişimini detaylı belirtme
                    for row_idx in diff_indices:
                        cell1 = col_values1.iloc[row_idx]
                        cell2 = col_values2.iloc[row_idx]

                        cell_diff.append({
                            'type': 'cell',
                            'row': row_idx + 1,  # 1-based indexing for user display
                            'column': col,
                            'value1': cell1,
                            'value2': cell2,
                            'change_type': 'modified'
                        })

            # Tablo yapısındaki ek satırları kontrol etme (df2'de fazla satırlar)
            if len(df2) > len(df1):
                additional_rows = len(df2) - len(df1)
                cell_diff.append({
                    'type': 'structure',
                    'element': 'additional_rows',
                    'value1': '',
                    'value2': f"{additional_rows} satır",
                    'diff': additional_rows,
                    'description': f"İkinci Excel dosyasında {additional_rows} ek satır bulunuyor"
                })

                # Ek satırlarda içerik kontrolü
                for col in df2.columns:
                    if col in common_cols:
                        for row_idx in range(len(df1), len(df2)):
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

            # Tablo yapısındaki eksik satırları kontrol etme (df1'de fazla satırlar)
            if len(df1) > len(df2):
                missing_rows = len(df1) - len(df2)
                cell_diff.append({
                    'type': 'structure',
                    'element': 'missing_rows',
                    'value1': f"{missing_rows} satır",
                    'value2': '',
                    'diff': missing_rows,
                    'description': f"İkinci Excel dosyasında {missing_rows} satır eksik"
                })

                # Eksik satırlarda içerik kontrolü
                for col in df1.columns:
                    if col in common_cols:
                        for row_idx in range(len(df2), len(df1)):
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

            # Sonuçları önemlilik sırasına göre sırala
            all_diff.sort(key=lambda x: (
                0 if x['type'] == 'structure' else 1,  # önce yapısal değişiklikler
                0 if x.get('change_type') == 'removed' else (1 if x.get('change_type') == 'added' else 2),  # sonra kaldırılan, eklenen ve değiştirilen
                x.get('row', 0)  # son olarak satır numarasına göre
            ))

            return all_diff
        except Exception as e:
            raise Exception(f"Excel dosyalarını karşılaştırırken hata: {e}")

    def save_to_supabase(self, comparison_results, supabase_conn):
        """Save comparison results to Supabase"""
        try:
            if not supabase_conn:
                print("Supabase bağlantısı bulunamadı")
                return False

            cursor = supabase_conn.cursor()

            # Karşılaştırma sonuçlarını JSON olarak kaydet
            cursor.execute("""
                INSERT INTO comparison_results 
                (file1_name, file2_name, comparison_data, created_at)
                VALUES (%s, %s, %s::jsonb, %s)
            """, (
                os.path.basename(comparison_results['file1']['filepath']),
                os.path.basename(comparison_results['file2']['filepath']),
                json.dumps(comparison_results.get('comparison_data', [])),
                datetime.now()
            ))

            supabase_conn.commit()
            print("Karşılaştırma sonuçları Supabase'e kaydedildi")
            return True

            # Karşılaştırma sonuçlarını kaydet
            comparison_data = comparison_results.get('comparison_data', [])
            if comparison_data:
                for result in comparison_data:
                    cursor.execute("""
                        INSERT INTO comparison_results 
                        (file_id, type, row_num, column_name, old_value, new_value, change_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        file_id,
                        result.get('type'),
                        result.get('row', 0),
                        result.get('column', ''),
                        result.get('value1', ''),
                        result.get('value2', ''),
                        result.get('change_type', '')
                    ))

            supabase_conn.commit()
            print(f"Karşılaştırma sonuçları Supabase'e kaydedildi. File ID: {file_id}")
            return True
        except Exception as e:
            print(f"Supabase kayıt hatası: {e}")
            return False

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

    def list_excel_files(self, directory='.'):
        """List all Excel files in the specified directory"""
        try:
            # Get all Excel files in the directory
            xlsx_files = glob.glob(os.path.join(directory, '*.xlsx'))
            xls_files = glob.glob(os.path.join(directory, '*.xls'))

            # Combine the lists
            excel_files = xlsx_files + xls_files

            # Filter out directories and hidden files
            excel_files = [f for f in excel_files if os.path.isfile(f) and not os.path.basename(f).startswith('.')]

            # Sort by modification time (newest first)
            excel_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

            # Create a list with basic file info
            file_list = []
            for file in excel_files:
                file_info = {
                    'filepath': file,
                    'filename': os.path.basename(file),
                    'size_kb': round(os.path.getsize(file) / 1024, 2),
                    'modified': datetime.fromtimestamp(os.path.getmtime(file)).strftime('%Y-%m-%d %H:%M:%S')
                }
                file_list.append(file_info)

            return file_list
        except Exception as e:
            raise Exception(f"Excel dosyalarını listelerken hata: {e}")


# Örnek kullanım
if __name__ == "__main__":
    try:
        processor = ExcelProcessor()

        # Kullanıcıya seçenekler sunma
        print("Excel İşlemcisi")
        print("1. Son iki Excel dosyasını otomatik karşılaştır")
        print("2. Belirli iki Excel dosyasını karşılaştır")
        print("3. Mevcut Excel dosyalarını listele")
        print("4. Çıkış")

        choice = input("İşlem seçin (1-4): ")

        if choice == "1":
            # Otomatik olarak son iki Excel dosyasını bul ve karşılaştır
            directory = input("Dizin yolu (Enter tuşuna basarak mevcut dizini kullanabilirsiniz): ") or '.'
            result = processor.auto_compare_latest_files(directory)

            print(f"\nKarşılaştırma tamamlandı!")
            print(f"İlk dosya: {result['file1']['filename']}")
            print(f"İkinci dosya: {result['file2']['filename']}")
            print(f"Toplam {result['comparison_count']} fark tespit edildi")
            print(f"Karşılaştırma raporu oluşturuldu: {result['report_file']}")

        elif choice == "2":
            # Mevcut Excel dosyalarını listele
            files = processor.list_excel_files()

            if len(files) < 2:
                print("Karşılaştırma için en az iki Excel dosyası gerekli!")
            else:
                print("\nMevcut Excel Dosyaları:")
                for i, file in enumerate(files, 1):
                    print(f"{i}. {file['filename']} - {file['modified']}")

                # Kullanıcıdan dosya seçimlerini alma
                try:
                    file1_idx = int(input("\nBirinci dosya numarasını girin: ")) - 1
                    file2_idx = int(input("İkinci dosya numarasını girin: ")) - 1

                    if 0 <= file1_idx < len(files) and 0 <= file2_idx < len(files):
                        # Seçilen dosyalarla karşılaştırma yap
                        result = processor.compare_specific_files(files[file1_idx]['filepath'], files[file2_idx]['filepath'])

                        print(f"\nKarşılaştırma tamamlandı!")
                        print(f"İlk dosya: {result['file1']['filename']}")
                        print(f"İkinci dosya: {result['file2']['filename']}")
                        print(f"Toplam {result['comparison_count']} fark tespit edildi")
                        print(f"Karşılaştırma raporu oluşturuldu: {result['report_file']}")
                    else:
                        print("Geçersiz dosya numarası!")
                except ValueError:
                    print("Lütfen geçerli bir sayı girin!")

        elif choice == "3":
            # Mevcut Excel dosyalarını listele
            directory = input("Dizin yolu (Enter tuşuna basarak mevcut dizini kullanabilirsiniz): ") or '.'
            files = processor.list_excel_files(directory)

            if not files:
                print(f"Belirtilen dizinde ({directory}) Excel dosyası bulunamadı!")
            else:
                print(f"\nMevcut Excel Dosyaları ({len(files)}):")
                for i, file in enumerate(files, 1):
                    print(f"{i}. {file['filename']} - {file['size_kb']} KB - {file['modified']}")

        elif choice == "4":
            print("Program sonlandırılıyor...")

        else:
            print("Geçersiz seçim!")

    except Exception as e:
        print(f"Hata: {e}")