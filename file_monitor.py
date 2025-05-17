import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
from datetime import datetime
import threading

class ExcelFileHandler(FileSystemEventHandler):
    def __init__(self, db, excel_processor=None):
        self.db = db
        self.excel_processor = excel_processor
        self.processed_files = set()

    def is_excel_file(self, path):
        return path.lower().endswith(('.xlsx', '.xls'))

    def on_created(self, event):
        if not event.is_directory and self.is_excel_file(event.src_path):
            print(f"Yeni Excel dosyası algılandı: {event.src_path}")
            time.sleep(1)  # Dosyanın tam yazılmasını bekle
            
            # Otomatik karşılaştırma yap
            if self.excel_processor:
                try:
                    comparison_result = self.excel_processor.auto_compare_latest_files(os.path.dirname(event.src_path))
                    if comparison_result and 'comparison_data' in comparison_result:
                        print(f"Otomatik karşılaştırma tamamlandı: {len(comparison_result['comparison_data'])} fark bulundu")
                    
                    # Supabase'e kaydet
                    from migrate_to_supabase import get_supabase_connection
                    supabase_conn = get_supabase_connection()
                    if supabase_conn:
                        save_result = self.excel_processor.save_to_supabase({
                            'file1': comparison_result['file1'],
                            'file2': comparison_result['file2'],
                            'comparison_data': comparison_result['comparison_data']
                        }, supabase_conn)
                        if save_result:
                            print("Karşılaştırma sonuçları Supabase'e kaydedildi")
                except Exception as e:
                    print(f"Otomatik karşılaştırma hatası: {e}")

            if os.path.exists(event.src_path) and os.access(event.src_path, os.R_OK):
                # Excel dosyasını binary modda açmayı dene
                try:
                    # Dosyanın tam olarak yazılmasını bekle
                    time.sleep(1)
                    with open(event.src_path, 'rb') as f:
                        # Dosyanın okunabilir olduğunu kontrol et
                        f.read(1)
                    print(f"Yeni Excel dosyası algılandı: {os.path.basename(event.src_path)}")
                except Exception as e:
                    print(f"Dosya okuma hatası: {e}")
                    return
                if event.src_path in self.processed_files:
                    return

                try:
                    if self.excel_processor and not self.excel_processor.is_wscad_excel(event.src_path):
                        print(f"WSCAD Excel dosyası değil, atlanıyor: {event.src_path}")
                        return
                except Exception as e:
                    print(f"WSCAD format kontrolünde hata: {e}")

                filename = os.path.basename(event.src_path)
                filesize = os.path.getsize(event.src_path) / 1024

                file_id = self.db.add_file(filename, event.src_path, filesize)

                if file_id:
                    print(f"Yeni Excel dosyası eklendi: {filename}")
                    self.processed_files.add(event.src_path)
                else:
                    print(f"Dosya eklenirken hata oluştu: {filename}")

    def on_modified(self, event):
        if not event.is_directory and self.is_excel_file(event.src_path):
            time.sleep(0.2)

            if os.path.exists(event.src_path) and os.access(event.src_path, os.R_OK):
                if event.src_path in self.processed_files:
                    self.processed_files.remove(event.src_path)
                    return

                try:
                    if self.excel_processor and not self.excel_processor.is_wscad_excel(event.src_path):
                        return
                except Exception:
                    pass

                filename = os.path.basename(event.src_path)
                filesize = os.path.getsize(event.src_path) / 1024

                file_id = self.db.add_file(filename, event.src_path, filesize)

                if file_id:
                    print(f"Excel dosyası güncellendi: {filename}")
                    self.processed_files.add(event.src_path)
                else:
                    print(f"Dosya güncellenirken hata oluştu: {filename}")

class FileMonitor:
    def __init__(self, directory, db, excel_processor=None):
        self.directory = directory
        self.db = db
        self.observer = None
        self.excel_processor = excel_processor
        self.stop_event = threading.Event()

    def start_monitoring(self):
        if not os.path.exists(self.directory):
            raise FileNotFoundError(f"İzlenecek dizin bulunamadı veya erişilemez durumda: {self.directory}. Lütfen geçerli bir dizin seçin.")

        # Önce attached_assets klasöründeki dosyaları kontrol et
        assets_dir = "attached_assets"
        if os.path.exists(assets_dir):
            print(f"Attached assets klasöründen dosyalar yükleniyor...")
            for filename in os.listdir(assets_dir):
                if filename.endswith(('.xlsx', '.xls')):
                    src_path = os.path.join(assets_dir, filename)
                    dst_path = os.path.join(self.directory, filename)
                    try:
                        import shutil
                        shutil.copy2(src_path, dst_path)
                        print(f"Dosya kopyalandı: {filename}")
                    except Exception as e:
                        print(f"Dosya kopyalama hatası {filename}: {e}")
                        # Hedef dizini oluştur
                        os.makedirs(self.directory, exist_ok=True)
                        # Tekrar kopyalamayı dene
                        shutil.copy2(src_path, dst_path)
            
        # Supabase bağlantısını başlat
        try:
            from migrate_to_supabase import get_supabase_connection
            self.supabase_conn = get_supabase_connection()
            if not self.supabase_conn:
                print("Supabase bağlantısı kurulamadı, yerel depolama kullanılacak")
        except Exception as e:
            print(f"Supabase bağlantı hatası: {e}")
            self.supabase_conn = None

        event_handler = ExcelFileHandler(self.db, self.excel_processor)
        self.observer = Observer()
        
        # Dizini izlemeye başla
        self.observer.schedule(event_handler, self.directory, recursive=True)
        self.observer.start()
        print(f"Dizin izlemeye başlandı: {self.directory}")
        
        # Mevcut Excel dosyalarını tara
        self.scan_existing_files(event_handler)
        
        # Önce Excel dosyalarını karşılaştır
        try:
            if self.excel_processor:
                excel_files = self.excel_processor.list_excel_files(self.directory)
                if len(excel_files) >= 2:
                    # En son 2 Excel dosyasını karşılaştır
                    latest_files = excel_files[:2]
                    comparison_result = self.excel_processor.compare_excel_files(
                        latest_files[0]['filepath'],
                        latest_files[1]['filepath']
                    )
                    
                    if comparison_result:
                        # Supabase'e kaydet
                        try:
                            from migrate_to_supabase import get_supabase_connection
                            supabase_conn = get_supabase_connection()
                            if supabase_conn:
                                self.excel_processor.save_to_supabase({
                                    'file1': latest_files[0],
                                    'file2': latest_files[1],
                                    'comparison_data': comparison_result
                                }, supabase_conn)
                                print("Karşılaştırma sonuçları Supabase'e kaydedildi")
                        except Exception as e:
                            print(f"Supabase kayıt hatası: {e}")
                        
                        print("Excel karşılaştırma sonucu:", comparison_result)
                        return comparison_result
                else:
                    print("Dizinde en az iki Excel dosyası bulunamadı")
        except Exception as e:
            print(f"Excel karşılaştırma hatası: {e}")
            return None
        print(f"Dizin izlemeye başlandı: {self.directory}")

        self.scan_existing_files(event_handler)

    def scan_existing_files(self, event_handler):
        for root, _, files in os.walk(self.directory):
            for file in files:
                if file.lower().endswith(('.xlsx', '.xls')):
                    file_path = os.path.join(root, file)
                    mock_event = type('Event', (), {'is_directory': False, 'src_path': file_path})
                    event_handler.on_created(mock_event)

    def stop_monitoring(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()
            print("Dizin izleme durduruldu")

        self.stop_event.set()

    def get_monitored_directory(self):
        return self.directory