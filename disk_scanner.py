import os
import csv
import time
import argparse
import traceback
from collections import defaultdict

def scan_disk(start_path, output_file, error_log_file):
    """Рекурсивно сканирует дисковое пространство с непрерывной записью результатов и ошибок"""
    # Статистика для прогресса
    stats = {'files': 0, 'dirs': 0, 'scan_errors': 0, 'write_errors': 0, 'start_time': time.time()}
    
    # Словарь для размеров директорий (полный путь -> размер)
    dir_sizes = defaultdict(int)
    
    # Открываем файлы для непрерывной записи
    with open(output_file, 'w', newline='', encoding='utf-8') as csv_f, \
         open(error_log_file, 'w', encoding='utf-8') as err_f:
        
        # Инициализируем CSV writer
        csv_writer = csv.writer(csv_f)
        csv_writer.writerow(['путь', 'имя файла(директории)', 'тип', 'размер', 'расширение'])
        
        # Записываем заголовок в лог ошибок
        err_f.write(f"Disk Scan Error Log - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        err_f.write(f"Start path: {start_path}\n")
        err_f.write("="*80 + "\n\n")
        err_f.flush()
        
        # Функция для записи строки в CSV
        def write_csv_row(path, name, item_type, size, extension=""):
            try:
                csv_writer.writerow([path, name, item_type, size, extension])
                # Периодически сбрасываем буфер
                if stats['files'] % 1000 == 0 or stats['dirs'] % 100 == 0:
                    csv_f.flush()
                return True
            except Exception as e:
                stats['write_errors'] += 1
                log_error(f"Ошибка записи в CSV: {path}/{name}", e)
                return False
        
        # Функция для записи ошибок
        def log_error(message, exception=None):
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            err_f.write(f"[{timestamp}] {message}\n")
            if exception:
                err_f.write(f"Тип ошибки: {type(exception).__name__}\n")
                err_f.write(f"Сообщение: {str(exception)}\n")
                err_f.write("Трассировка:\n")
                err_f.write(traceback.format_exc())
            err_f.write("\n" + "-"*80 + "\n\n")
            err_f.flush()
        
        # Информация о прогрессе
        def log_progress():
            elapsed = time.time() - stats['start_time']
            print(f"\rОбработано: {stats['dirs']} директорий, {stats['files']} файлов, "
                  f"ошибок сканирования: {stats['scan_errors']}, "
                  f"ошибок записи: {stats['write_errors']}, "
                  f"время: {elapsed:.1f} сек", end='', flush=True)
        
        # Рекурсивный обход с использованием os.walk (снизу вверх)
        for root, dirs, files in os.walk(start_path, topdown=False):
            current_dir_size = 0
            
            # Обработка файлов
            for name in files:
                file_path = os.path.join(root, name)
                try:
                    file_size = os.path.getsize(file_path)
                    current_dir_size += file_size
                    
                    # Получение расширения файла
                    file_ext = os.path.splitext(name)[1]
                    if file_ext:
                        file_ext = file_ext[1:]  # Убираем точку
                    else:
                        file_ext = "без расширения"
                    
                    # Записываем файл сразу в CSV
                    parent_path = os.path.dirname(file_path)
                    write_csv_row(parent_path, name, 'file', file_size, file_ext)
                    stats['files'] += 1
                except Exception as e:
                    stats['scan_errors'] += 1
                    log_error(f"Ошибка доступа к файлу: {file_path}", e)
                    continue
                
                # Вывод прогресса каждые 5000 файлов
                if stats['files'] % 5000 == 0:
                    log_progress()
            
            # Обработка поддиректорий
            for name in dirs:
                dir_path = os.path.join(root, name)
                if dir_path in dir_sizes:
                    current_dir_size += dir_sizes[dir_path]
            
            # Записываем директорию в CSV
            dir_sizes[root] = current_dir_size
            dir_name = os.path.basename(root) if root != start_path else start_path
            parent_path = os.path.dirname(root) if root != start_path else ""
            write_csv_row(parent_path, dir_name, 'dir', current_dir_size)
            stats['dirs'] += 1
            
            # Вывод прогресса каждые 100 директорий
            if stats['dirs'] % 100 == 0:
                log_progress()
    
    # Финальный отчет
    elapsed = time.time() - stats['start_time']
    print(f"\nСканирование завершено за {elapsed:.1f} сек")
    print(f"Всего: {stats['dirs']} директорий, {stats['files']} файлов")
    print(f"Ошибок сканирования: {stats['scan_errors']}")
    print(f"Ошибок записи: {stats['write_errors']}")
    print(f"Результаты сохранены в: {output_file}")
    print(f"Ошибки записаны в: {error_log_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Сканирование дискового пространства')
    parser.add_argument('--path', default='C:\\', help='Путь для сканирования (по умолчанию: C:\\)')
    parser.add_argument('--output', default='disk_usage.csv', help='Выходной CSV-файл')
    parser.add_argument('--error-log', default='disk_scan_errors.log', help='Файл для записи ошибок')
    
    args = parser.parse_args()
    
    print(f"Начало сканирования: {args.path}")
    print("Это может занять несколько минут...")
    print("Прогресс будет отображаться в реальном времени")
    print("Для прерывания нажмите Ctrl+C (данные сохранятся частично)")
    print(f"Ошибки будут записываться в: {args.error_log}")
    scan_disk(args.path, args.output, args.error_log)