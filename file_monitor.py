import os
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
from datetime import datetime

def get_filename_without_extension(filename):
    return os.path.splitext(filename)[0]

class ExcelFileHandler(FileSystemEventHandler):
    def __init__(self, db, excel_processor=None):
        self.db = db
        self.excel_processor = excel_processor
        self.processed_files = set()
        self.stop_event = threading.Event()

    def is_excel_file(self, path):
        return path.lower().endswith(('.xlsx', '.xls'))

    def on_created(self, event):
        if not event.is_directory and self.is_excel_file(event.src_path):
            print(f"Yeni Excel dosyası algılandı: {event.src_path}")
            # Dosyanın yazılmasını bekle
            max_attempts = 10
            for attempt in range(max_attempts):
                try:
                    if os.path.exists(event.src_path) and os.access(event.src_path, os.R_OK):
                        with pd.ExcelFile(event.src_path) as xls:
                            # Excel dosyasını doğrula
                            if len(xls.sheet_names) > 0:
                                break
                except:
                    if attempt < max_attempts - 1:
                        time.sleep(0.5)

            try:
                filename_with_ext = os.path.basename(event.src_path)
                filename_without_ext = get_filename_without_extension(filename_with_ext)
                filesize = os.path.getsize(event.src_path) / 1024

                # Dosya zaten var mı kontrol et
                existing_file = self.db.execute("SELECT id FROM files WHERE filename = ?", (filename_with_ext,)).fetchone()

                if existing_file:
                    # Dosyayı güncelle
                    self.db.execute(
                        "UPDATE files SET filepath = ?, filesize = ?, detected_time = ? WHERE filename = ?",
                        (event.src_path, filesize, datetime.now(), filename_with_ext)
                    )
                    file_id = existing_file[0]
                    print(f"Excel dosyası güncellendi: {filename_without_ext}")
                else:
                    file_id = self.db.add_file(filename_with_ext, event.src_path, filesize)
                    print(f"Excel dosyası eklendi: {filename_without_ext}")

                if file_id:
                    print(f"Excel dosyası eklendi: {filename_without_ext}")
                    self.processed_files.add(event.src_path)
                else:
                    print(f"Dosya eklenirken hata oluştu: {filename_without_ext}")

            except Exception as e:
                print(f"Dosya işleme hatası: {e}")

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

                filename_with_ext = os.path.basename(event.src_path)
                filename_without_ext = get_filename_without_extension(filename_with_ext)
                filesize = os.path.getsize(event.src_path) / 1024

                file_id = self.db.add_file(filename_with_ext, event.src_path, filesize)

                if file_id:
                    print(f"Excel dosyası güncellendi: {filename_without_ext}")
                    self.processed_files.add(event.src_path)
                else:
                    print(f"Dosya güncellenirken hata oluştu: {filename_without_ext}")

class FileMonitor:
    def __init__(self, directory, db, excel_processor=None):
        self.directory = os.path.abspath(directory)
        self.db = db
        self.excel_processor = excel_processor
        self.observer = None
        self.is_running = False
        self._lock = threading.Lock()
        self.stop_event = threading.Event()

    def clean_database(self):
        """Veritabanındaki eski dosya kayıtlarını temizle"""
        try:
            # Sistemde olmayan dosyaları veritabanından temizle
            files = self.db.query("SELECT id, filepath FROM files").fetchall()
            for file in files:
                if not os.path.exists(file[1]):
                    self.db.execute("DELETE FROM files WHERE id = ?", (file[0],))
                    self.db.execute("DELETE FROM file_revisions WHERE file_id = ?", (file[0],))
        except Exception as e:
            print(f"Veritabanı temizleme hatası: {e}")

    def start_monitoring(self):
        if not os.path.exists(self.directory):
            raise FileNotFoundError(f"İzlenecek dizin bulunamadı veya erişilemez durumda: {self.directory}. Lütfen geçerli bir dizin seçin.")

        # Önce veritabanını temizle
        self.clean_database()

        def monitor_task():
            print(f"Arka plan izleme başlatıldı: {self.directory}")
            self.observer = Observer()
            event_handler = ExcelFileHandler(self.db, self.excel_processor)
            self.observer.schedule(event_handler, self.directory, recursive=True)
            self.observer.start()

            # İlk taramayı yap
            self.scan_existing_files(event_handler)

            try:
                while not self.stop_event.is_set():
                    time.sleep(1)
            except Exception as e:
                print(f"İzleme hatası: {e}")
            finally:
                if self.observer:
                    self.observer.stop()
                    self.observer.join()

        self.monitor_thread = threading.Thread(target=monitor_task, daemon=True)
        self.monitor_thread.start()

        # Önce attached_assets klasöründeki dosyaları kontrol et ve kopyala
        assets_dir = "attached_assets"
        if os.path.exists(assets_dir):
            print(f"Attached assets klasöründen dosyalar yükleniyor...")
            import shutil
            os.makedirs(self.directory, exist_ok=True)

            for filename in os.listdir(assets_dir):
                if filename.lower().endswith(('.xlsx', '.xls')):
                    src_path = os.path.join(assets_dir, filename)
                    dst_path = os.path.join(self.directory, filename)
                    try:
                        if not os.path.exists(dst_path):
                            shutil.copy2(src_path, dst_path)
                            filename_without_ext = get_filename_without_extension(filename)
                            print(f"Dosya kopyalandı: {filename_without_ext}")

                            # Trigger file created event
                            mock_event = type('Event', (), {'is_directory': False, 'src_path': dst_path})
                            event_handler.on_created(mock_event)
                    except Exception as e:
                        filename_without_ext = get_filename_without_extension(filename)
                        print(f"Dosya kopyalama hatası {filename_without_ext}: {e}")

        # Supabase bağlantısını başlat
        try:
            from migrate_to_supabase import get_supabase_connection
            self.supabase_conn = get_supabase_connection()
            if not self.supabase_conn:
                print("Supabase bağlantısı kurulamadı, yerel depolama kullanılacak")
        except Exception as e:
            print(f"Supabase bağlantı hatası: {e}")
            self.supabase_conn = None

    def scan_existing_files(self, event_handler):
        """Mevcut Excel dosyalarını tara ve işle"""
        try:
            # Önce mevcut dosyaları temizle
            event_handler.processed_files.clear()

            for root, _, files in os.walk(self.directory):
                for file in files:
                    # Gizli dosyaları atla
                    if file.startswith('.'):
                        continue

                    if file.lower().endswith(('.xlsx', '.xls')):
                        file_path = os.path.join(root, file)

                        # Dosya okunabilir ve erişilebilir mi kontrol et
                        if os.path.exists(file_path) and os.access(file_path, os.R_OK):
                            try:
                                # Excel dosyası olduğunu doğrula
                                if self.excel_processor and self.excel_processor.is_wscad_excel(file_path):
                                    mock_event = type('Event', (), {'is_directory': False, 'src_path': file_path})
                                    event_handler.on_created(mock_event)
                                    print(f"Excel dosyası işlendi: {file}")
                            except Exception as e:
                                print(f"Dosya işleme hatası: {e}")
        except Exception as e:
            print(f"Dosya tarama hatası: {e}")

    def stop_monitoring(self):
        """İzlemeyi durdur"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
            print("Dizin izleme durduruldu")

    def get_monitored_directory(self):
        """İzlenen dizini döndür"""
        return self.directory