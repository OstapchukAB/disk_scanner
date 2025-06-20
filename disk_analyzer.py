#python disk_analyzer.py --input "scan_results.csv" --output-dir "analysis_results" --top-n 15
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import argparse
from datetime import datetime
import matplotlib

# Используем библиотеку для улучшения визуализации
matplotlib.rcParams['font.family'] = 'DejaVu Sans'
plt.style.use('ggplot')

def analyze_disk_data(input_file, output_dir, top_n=20):
    """Анализирует данные сканирования диска и создает визуализации"""
    # Создаем директорию для результатов, если ее нет
    os.makedirs(output_dir, exist_ok=True)
    
    # Загрузка данных
    print(f"Загрузка данных из {input_file}...")
    df = pd.read_csv(input_file)
    
    # Преобразование размеров в МБ
    df['размер_МБ'] = df['Size'] / (1024 * 1024)
    
    # Фильтрация данных
    files_df = df[df['Type'] == 'file']
    dirs_df = df[df['Type'] == 'dir']
    
    print(f"Загружено {len(df)} записей")
    print(f"Файлов: {len(files_df)}, Директорий: {len(dirs_df)}")
    
    # 1. Топ файлов по размеру
    plt.figure(figsize=(14, 10))
    top_files = files_df.nlargest(top_n, 'размер_МБ')
    plt.barh(
        top_files['Name'] + " (" + top_files['Extension'] + ")",
        top_files['размер_МБ'],
        color='royalblue'
    )
    plt.xlabel('Размер (МБ)')
    plt.title(f'Топ-{top_n} файлов по размеру')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'top_files.png'), dpi=150)
    
    # 2. Топ директорий по размеру
    plt.figure(figsize=(14, 10))
    top_dirs = dirs_df.nlargest(top_n, 'размер_МБ')
    plt.barh(top_dirs['Name'], top_dirs['размер_МБ'], color='forestgreen')
    plt.xlabel('Размер (МБ)')
    plt.title(f'Топ-{top_n} директорий по размеру')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'top_dirs.png'), dpi=150)
    
    # 3. Распределение по типам файлов (расширениям)
    plt.figure(figsize=(12, 12))
    ext_size = files_df.groupby('Size')['размер_МБ'].sum()
    ext_count = files_df.groupby('Size').size()
    
    # Фильтрация мелких категорий
    ext_size = ext_size[ext_size > ext_size.quantile(0.1)]
    
    plt.pie(
        ext_size,
        labels=ext_size.index,
        autopct=lambda p: f'{p:.1f}%\n({ext_size.sum()*p/100:.0f} МБ)',
        startangle=90,
        textprops={'fontsize': 9}
    )
    plt.title('Распределение места по типам файлов')
    plt.savefig(os.path.join(output_dir, 'file_types.png'), dpi=150)
    
    # 4. Кумулятивный объем файлов
    plt.figure(figsize=(12, 8))
    sorted_files = files_df.sort_values('Size', ascending=False)
    sorted_files['cumulative'] = sorted_files['размер_МБ'].cumsum()
    plt.plot(np.arange(len(sorted_files)), sorted_files['cumulative'], 'b-')
    plt.xlabel('Количество файлов (отсортировано по размеру)')
    plt.ylabel('Накопленный объем (МБ)')
    plt.title('Кумулятивное распределение объема файлов')
    plt.grid(True, alpha=0.3)
    plt.savefig(os.path.join(output_dir, 'cumulative_size.png'), dpi=150)
    
    # 5. Динамика создания файлов (если есть данные)
    if 'дата создания' in files_df.columns:
        try:
            # Преобразуем даты и фильтруем корректные
            files_df['дата_создания'] = pd.to_datetime(
                files_df['дата создания'], 
                errors='coerce',
                format='%Y-%m-%d %H:%M:%S'
            )
            valid_dates = files_df.dropna(subset=['дата_создания'])
            
            # Группировка по году и месяцу
            valid_dates['год_месяц'] = valid_dates['дата_создания'].dt.to_period('M')
            monthly = valid_dates.groupby('год_месяц')['размер_МБ'].sum().reset_index()
            monthly['год_месяц'] = monthly['год_месяц'].astype(str)
            
            plt.figure(figsize=(14, 7))
            plt.bar(monthly['год_месяц'], monthly['размер_МБ'], color='purple')
            plt.xlabel('Год-месяц')
            plt.ylabel('Общий размер созданных файлов (МБ)')
            plt.title('Динамика создания файлов по времени')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'creation_timeline.png'), dpi=150)
        except Exception as e:
            print(f"Ошибка при обработке дат: {str(e)}")
    
    # 6. Соотношение файлов и директорий
    plt.figure(figsize=(8, 6))
    sizes = [files_df['размер_МБ'].sum(), dirs_df['размер_МБ'].sum()]
    labels = ['Файлы', 'Директории']
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=['lightblue', 'lightgreen'])
    plt.title('Распределение места между файлами и директориями')
    plt.savefig(os.path.join(output_dir, 'files_vs_dirs.png'), dpi=150)
    
    # 7. Тепловая карта расширений и размеров
    plt.figure(figsize=(12, 8))
    top_extensions = ext_size.nlargest(30).index
    filtered_files = files_df[files_df['Extension'].isin(top_extensions)]
    
    # Логарифмируем размеры для лучшей визуализации
    filtered_files['log_size'] = np.log10(filtered_files['размер_МБ'] + 1)
    
    plt.scatter(
        filtered_files['Extension'],
        filtered_files['размер_МБ'],
        c=filtered_files['log_size'],
        cmap='viridis',
        alpha=0.6,
        s=30
    )
    plt.colorbar(label='log10(Размер в МБ)')
    plt.yscale('log')
    plt.xlabel('Расширение файла')
    plt.ylabel('Размер файла (МБ)')
    plt.title('Распределение размеров файлов по расширениям')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'size_heatmap.png'), dpi=150)
    
    # Генерация отчета
    generate_text_report(df, output_dir, top_n)
    
    print(f"Анализ завершен. Результаты сохранены в: {output_dir}")

def generate_text_report(df, output_dir, top_n=10):
    """Генерирует текстовый отчет с основной статистикой"""
    report_path = os.path.join(output_dir, 'disk_report.txt')
    
    files_df = df[df['Type'] == 'file']
    dirs_df = df[df['Type'] == 'dir']
    
    with open(report_path, 'w', encoding='utf-8') as report:
        report.write("="*80 + "\n")
        report.write(f"АНАЛИТИЧЕСКИЙ ОТЧЕТ ПО ИСПОЛЬЗОВАНИЮ ДИСКОВОГО ПРОСТРАНСТВА\n")
        report.write(f"Сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        report.write("="*80 + "\n\n")
        
        # Общая статистика
        total_size_gb = df['Size'].sum() / (1024 ** 3)
        avg_file_size_mb = files_df['Size'].mean() / (1024 ** 2)
        max_file_size_mb = files_df['Size'].max() / (1024 ** 2)
        
        report.write(f"Общий размер просканированных данных: {total_size_gb:.2f} ГБ\n")
        report.write(f"Количество файлов: {len(files_df)}\n")
        report.write(f"Количество директорий: {len(dirs_df)}\n")
        report.write(f"Средний размер файла: {avg_file_size_mb:.2f} МБ\n")
        report.write(f"Максимальный размер файла: {max_file_size_mb:.2f} МБ\n\n")
        
        # Топ файлов
        report.write("="*80 + "\n")
        report.write(f"ТОП-{top_n} ФАЙЛОВ ПО РАЗМЕРУ:\n")
        report.write("="*80 + "\n")
        top_files = files_df.nlargest(top_n, 'Size')
        for i, (_, row) in enumerate(top_files.iterrows(), 1):
            size_mb = row['Size'] / (1024 ** 2)
            report.write(f"{i}. {row['Path']}/{row['Name']} - {size_mb:.2f} МБ\n")
        
        # Топ директорий
        report.write("\n" + "="*80 + "\n")
        report.write(f"ТОП-{top_n} ДИРЕКТОРИЙ ПО РАЗМЕРУ:\n")
        report.write("="*80 + "\n")
        top_dirs = dirs_df.nlargest(top_n, 'Size')
        for i, (_, row) in enumerate(top_dirs.iterrows(), 1):
            size_mb = row['Size'] / (1024 ** 2)
            report.write(f"{i}. {row['Path']}/{row['Name']} - {size_mb:.2f} МБ\n")
        
        # Анализ по расширениям
        report.write("\n" + "="*80 + "\n")
        report.write("РАСПРЕДЕЛЕНИЕ ПО ТИПАМ ФАЙЛОВ:\n")
        report.write("="*80 + "\n")
        ext_stats = files_df.groupby('Extension').agg(
            count=('Size', 'size'),
            total_size=('Size', 'sum'),
            avg_size=('Size', 'mean')
        ).sort_values('total_size', ascending=False).head(15)
        
        ext_stats['total_size_gb'] = ext_stats['total_size'] / (1024 ** 3)
        ext_stats['avg_size_mb'] = ext_stats['avg_size'] / (1024 ** 2)
        
        report.write(ext_stats[['count', 'total_size_gb', 'avg_size_mb']].to_string())
    
    print(f"Текстовый отчет сохранен в: {report_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Анализ данных сканирования дискового пространства')
    parser.add_argument('--input', required=True, help='Входной CSV-файл с результатами сканирования')
    parser.add_argument('--output-dir', default='disk_analysis', help='Директория для сохранения результатов')
    parser.add_argument('--top-n', type=int, default=20, help='Количество элементов в топах')
    
    args = parser.parse_args()
    
    print(f"Запуск анализа данных диска")
    print(f"Входной файл: {args.input}")
    print(f"Выходная директория: {args.output_dir}")
    print(f"Количество элементов в топах: {args.top_n}")
    print("="*80)
    
    analyze_disk_data(args.input, args.output_dir, args.top_n)