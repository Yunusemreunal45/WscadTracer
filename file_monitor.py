import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
from datetime import datetime
import threading

# Dosya adından uzantıyı kaldıran yardımcı fonksiyon
def get_filename_without_extension(filename):
    return os.path.splitext(filename)[0]

class ExcelFileHandler(FileSystemEventHandler):
    def __init__(self, db, excel_processor=None):
        self.db = db
        self.excel_processor = excel_processor
        self.processed_files = set()
        self.stop_event = threading.Event

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
                    continue

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
                        # Supabase'e kaydet
                        save_result = self.excel_processor.save_to_supabase({
                            'file1': comparison_result['file1'],
                            'file2': comparison_result['file2'],
                            'comparison_data': comparison_result['comparison_data']
                        }, supabase_conn)

                        # Revizyon olarak kaydet
                        revision_result = self.excel_processor.save_comparison_as_revision(
                            comparison_result,
                            self.db
                        )

                        if save_result and revision_result:
                            print("Karşılaştırma sonuçları Supabase ve revizyon olarak kaydedildi")
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
                    
                    # Uzantısız dosya adını kullan
                    filename_with_ext = os.path.basename(event.src_path)
                    filename_without_ext = get_filename_without_extension(filename_with_ext)
                    print(f"Yeni Excel dosyası algılandı: {filename_without_ext}")
                except Exception as e:
                    print(f"Dosya okuma hatası: {e}")
                    return
                if event.src_path in self.processed_files:
                    return

                try:
                    if self.excel_processor and not self.excel_processor.is_wscad_excel(event.src_path):
                        print(f"WSCAD Excel dosyası değil, atlanıyor: {filename_without_ext}")
                        return
                except Exception as e:
                    print(f"WSCAD format kontrolünde hata: {e}")

                try:
                    filename_with_ext = os.path.basename(event.src_path)
                    filename_without_ext = get_filename_without_extension(filename_with_ext)
                    filesize = os.path.getsize(event.src_path) / 1024
                    
                    # Dosya zaten var mı kontrol et (tam dosya adıyla)
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
                        # Yeni dosya ekle
                        file_id = self.db.add_file(filename_with_ext, event.src_path, filesize)
                        print(f"Yeni Excel dosyası eklendi: {filename_without_ext}")
                    
                    if file_id:
                        self.processed_files.add(event.src_path)
                        # Veritabanını zorla kaydet
                        self.db.commit()
                    else:
                        print(f"Dosya işlenirken hata oluştu: {filename_without_ext}")
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

    def start_monitoring(self):
        if not os.path.exists(self.directory):
            raise FileNotFoundError(f"İzlenecek dizin bulunamadı veya erişilemez durumda: {self.directory}. Lütfen geçerli bir dizin seçin.")

        def monitor_task():
            print(f"Arka plan izleme başlatıldı: {self.directory}")
            self.observer = Observer()
            event_handler = ExcelFileHandler(self.db, self.excel_processor)
            self.observer.schedule(event_handler, self.directory, recursive=True)
            self.observer.start()
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

            # Clear processed files from handler
            if self.observer.event_handlers:
                for handler in self.observer.event_handlers:
                    if isinstance(handler, ExcelFileHandler):
                        handler.processed_files.clear()

            # Clear files from database
            if self.db:
                self.db.execute("DELETE FROM files")
                self.db.execute("DELETE FROM file_revisions")
                print("Dosya listesi temizlendi")

        self.stop_event.set()

    def get_monitored_directory(self):
        return self.directory