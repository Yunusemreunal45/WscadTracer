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
            time.sleep(1)  # Dosyanın tam yazılmasını bekle

            if os.path.exists(event.src_path) and os.access(event.src_path, os.R_OK):
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
            time.sleep(1)

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
            raise FileNotFoundError(f"Dizin bulunamadı: {self.directory}")

        event_handler = ExcelFileHandler(self.db, self.excel_processor)
        self.observer = Observer()
        self.observer.schedule(event_handler, self.directory, recursive=True)

        self.observer.start()
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