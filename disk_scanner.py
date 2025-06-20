import os
import csv
import time
import argparse
import traceback
import json
from collections import defaultdict

# Путь к файлу конфигурации по умолчанию
DEFAULT_CONFIG_PATH = "disk_scanner_config.json"

def load_config(config_path):
    """Загружает конфигурацию из JSON-файла"""
    default_config = {
        "path": "C:\\",
        "output": "large_files_report.csv",
        "error_log": "disk_scan_errors.log",
        "min_size": 50,
        "log_interval_files": 500,
        "log_interval_dirs": 50,
        "flush_interval": 1000
    }
    
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # Объединяем с настройками по умолчанию
                return {**default_config, **config}
        except Exception as e:
            print(f"Ошибка загрузки конфигурации {config_path}: {str(e)}")
            print("Используются настройки по умолчанию")
            return default_config
    return default_config

def save_config(config_path, config):
    """Сохраняет текущую конфигурацию в JSON-файл"""
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
        print(f"Конфигурация сохранена в {config_path}")
    except Exception as e:
        print(f"Ошибка сохранения конфигурации: {str(e)}")

def scan_disk(start_path, output_file, error_log_file, min_size_mb, log_interval_files, log_interval_dirs, flush_interval):
    """Рекурсивно сканирует дисковое пространство с фильтрацией по размеру"""
    # Конвертируем МБ в байты
    min_size_bytes = min_size_mb * 1024 * 1024
    
    # Статистика для прогресса
    stats = {
        'files': 0, 
        'files_written': 0,
        'files_skipped': 0,
        'dirs': 0, 
        'scan_errors': 0, 
        'write_errors': 0,
        'start_time': time.time()
    }
    
    # Словарь для размеров директорий (полный путь -> размер)
    dir_sizes = defaultdict(int)
    
    # Открываем файлы для непрерывной записи
    with open(output_file, 'w', newline='', encoding='utf-8') as csv_f, \
         open(error_log_file, 'w', encoding='utf-8') as err_f:
        
        # Инициализируем CSV writer
        csv_writer = csv.writer(csv_f)
        csv_writer.writerow(['Path', 'Name', 'Type', 'Size', 'Extension', 'DateTimeCreate','DateTimeLastModification'])
        
        # Записываем заголовок в лог ошибок
        err_f.write(f"Disk Scan Report - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        err_f.write(f"Scan path: {start_path}\n")
        err_f.write(f"Minimum file size: {min_size_mb} MB\n")
        err_f.write("="*80 + "\n\n")
        err_f.flush()
        
        # Функция для записи строки в CSV
        def write_csv_row(path, name, item_type, size, extension="", creation_date="", last_modification_time=""):
            try:
                csv_writer.writerow([path, name, item_type, size, extension, creation_date, last_modification_time])
                # Периодически сбрасываем буфер
                if stats['files_written'] % flush_interval == 0 or stats['dirs'] % 100 == 0:
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
                err_f.write(f"Error type: {type(exception).__name__}\n")
                err_f.write(f"Message: {str(exception)}\n")
                err_f.write("Traceback:\n")
                err_f.write(traceback.format_exc())
            err_f.write("\n" + "-"*80 + "\n\n")
            err_f.flush()
        
        # Информация о прогрессе
        def log_progress():
            elapsed = time.time() - stats['start_time']
            speed = stats['files'] / elapsed if elapsed > 0 else 0
            print(f"\rОбработано: {stats['dirs']} директорий, {stats['files']} файлов, "
                  f"записано: {stats['files_written']}, пропущено: {stats['files_skipped']}, "
                  f"скорость: {speed:.1f} файл/сек, "
                  f"ошибок: {stats['scan_errors'] + stats['write_errors']}", 
                  end='', flush=True)
        
        # Рекурсивный обход с использованием os.walk (снизу вверх)
        for root, dirs, files in os.walk(start_path, topdown=False):
            current_dir_size = 0
            
            # Обработка файлов
            for name in files:
                stats['files'] += 1
                file_path = os.path.join(root, name)
                try:
                    file_size = os.path.getsize(file_path)
                    current_dir_size += file_size
                    
                    # Пропускаем файлы меньше порога
                    if file_size < min_size_bytes:
                        stats['files_skipped'] += 1
                        continue
                    
                    # Получаем расширение файла
                    file_ext = os.path.splitext(name)[1]
                    if file_ext:
                        file_ext = file_ext[1:]  # Убираем точку
                    else:
                        file_ext = "без расширения"
                    
                    # Получаем дату создания файла
                    try:
                        creation_time = os.path.getctime(file_path)
                        creation_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(creation_time))
                    except Exception:
                        creation_date = "ошибка даты создания файла"
                    
                     # Получаем дату последней модификации файла
                    try:
                        last_modification_time = os.path.getmtime(file_path)
                        last_modification_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_modification_time))
                    except Exception:
                        last_modification_time = "ошибка даты последней модификации файла"


                    
                    # Записываем файл в CSV
                    parent_path = os.path.dirname(file_path)
                    write_csv_row(parent_path, name, 'file', file_size, file_ext, creation_date, last_modification_time)
                    stats['files_written'] += 1
                except Exception as e:
                    stats['scan_errors'] += 1
                    log_error(f"Ошибка доступа к файлу: {file_path}", e)
                    continue
                
                # Вывод прогресса с заданным интервалом
                if stats['files'] % log_interval_files == 0:
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
            
            # Вывод прогресса с заданным интервалом
            if stats['dirs'] % log_interval_dirs == 0:
                log_progress()
    
    # Финальный отчет
    elapsed = time.time() - stats['start_time']
    print(f"\n\n{'='*80}")
    print(f"Сканирование завершено за {elapsed:.1f} секунд")
    print(f"Всего обработано: {stats['dirs']} директорий, {stats['files']} файлов")
    print(f"Файлов записано в отчет: {stats['files_written']} (размер > {min_size_mb} МБ)")
    print(f"Файлов пропущено: {stats['files_skipped']} (размер < {min_size_mb} МБ)")
    print(f"Ошибок сканирования: {stats['scan_errors']}")
    print(f"Ошибок записи: {stats['write_errors']}")
    print(f"Результаты сохранены в: {output_file}")
    print(f"Ошибки записаны в: {error_log_file}")
    print(f"{'='*80}")

def main():
    # Загружаем конфигурацию по умолчанию
    config = load_config(DEFAULT_CONFIG_PATH)
    
    # Настраиваем парсер аргументов
    parser = argparse.ArgumentParser(
        description='Сканирование дискового пространства с фильтрацией по размеру',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--path', default=config['path'], 
                        help='Путь для сканирования')
    parser.add_argument('--output', default=config['output'], 
                        help='Выходной CSV-файл')
    parser.add_argument('--error-log', default=config['error_log'], 
                        help='Файл для записи ошибок')
    parser.add_argument('--min-size', type=float, default=config['min_size'], 
                        help='Минимальный размер файлов для включения в отчет (в мегабайтах)')
    parser.add_argument('--config', default=DEFAULT_CONFIG_PATH,
                        help='Путь к файлу конфигурации JSON')
    parser.add_argument('--save-config', action='store_true',
                        help='Сохранить текущие настройки в файл конфигурации')
    
    args = parser.parse_args()
    
    # Если указан другой файл конфигурации, загружаем его
    if args.config != DEFAULT_CONFIG_PATH:
        config = load_config(args.config)
    
    # Обновляем конфигурацию значениями из командной строки
    final_config = {
        'path': args.path,
        'output': args.output,
        'error_log': args.error_log,
        'min_size': args.min_size,
        'log_interval_files': config.get('log_interval_files', 500),
        'log_interval_dirs': config.get('log_interval_dirs', 50),
        'flush_interval': config.get('flush_interval', 1000)
    }
    
    # Сохраняем конфигурацию если нужно
    if args.save_config:
        save_config(args.config, final_config)
    
    print(f"{'='*80}")
    print(f"Запуск сканирования диска")
    print(f"Конфигурация: {args.config}")
    print(f"Путь: {final_config['path']}")
    print(f"Минимальный размер файлов: {final_config['min_size']} МБ")
    print(f"Отчет будет сохранен в: {final_config['output']}")
    print(f"Ошибки будут записываться в: {final_config['error_log']}")
    print(f"Интервал лога файлов: {final_config['log_interval_files']}")
    print(f"Интервал лога директорий: {final_config['log_interval_dirs']}")
    print(f"Интервал сброса буфера: {final_config['flush_interval']}")
    print(f"{'='*80}")
    print("Сканирование начато... Это может занять значительное время")
    print("Для прерывания нажмите Ctrl+C (данные сохранятся частично)")
    print(f"{'-'*80}")
    
    try:
        scan_disk(
            start_path=final_config['path'],
            output_file=final_config['output'],
            error_log_file=final_config['error_log'],
            min_size_mb=final_config['min_size'],
            log_interval_files=final_config['log_interval_files'],
            log_interval_dirs=final_config['log_interval_dirs'],
            flush_interval=final_config['flush_interval']
        )
    except KeyboardInterrupt:
        print("\n\nСканирование прервано пользователем!")
        print("Частичные результаты сохранены в выходных файлах")
    except Exception as e:
        print(f"\n\nКритическая ошибка: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()