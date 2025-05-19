#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
from pathlib import Path
from datetime import datetime
from heapq import nlargest
from concurrent.futures import ThreadPoolExecutor

# Минимальный размер для "больших" файлов (10 МБ)
MIN_FILE_SIZE = 10 * 1024 * 1024  # 10 MB в байтах

# Максимальное количество результатов для отображения
MAX_RESULTS = 100

# Форматирование размеров файлов для удобного чтения
def format_size(size_bytes):
    """Преобразует размер в байтах в человекочитаемый формат"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024 or unit == 'TB':
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024

# Получение размера директории
def get_dir_size(path):
    """Рекурсивно вычисляет размер директории"""
    total_size = 0
    try:
        for entry in os.scandir(path):
            try:
                if entry.is_file(follow_symlinks=False):
                    total_size += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total_size += get_dir_size(entry.path)
            except (PermissionError, FileNotFoundError, OSError):
                # Игнорируем ошибки доступа к файлам
                continue
    except (PermissionError, FileNotFoundError, OSError):
        # Игнорируем ошибки доступа к директориям
        pass
    return total_size

# Основная функция сканирования
def scan_system(start_path):
    """Сканирует систему, находя большие файлы и директории"""
    if not os.path.exists(start_path):
        print(f"Путь {start_path} не существует!")
        return [], []
    
    # Для отслеживания прогресса
    last_status_time = time.time()
    total_items_scanned = 0
    
    # Для хранения больших файлов и директорий
    large_files = []
    dirs_info = {}
    
    # Сканирование директорий и файлов
    print(f"Начинаю сканирование от {start_path}...")
    print("Это может занять некоторое время в зависимости от размера диска и количества файлов.")
    
    for root, dirs, files in os.walk(start_path, topdown=True, onerror=None, followlinks=False):

        print(f"\nСканирование директории: {root}")
        # print(f"dirs: {dirs}")
        # print(f"files: {files}")

        # Обновление статуса каждые 2 секунды
        current_time = time.time()
        if current_time - last_status_time > 2:
            print(f"Сканирование... Обработано {total_items_scanned} элементов. Текущая директория: {root}", end='\r')
            last_status_time = current_time
        
        # Пропускаем некоторые системные директории
        dirs[:] = [d for d in dirs if not d.startswith(('.', '$', 'System Volume Information'))]
        
        # Обработка файлов
        for file in files:
            total_items_scanned += 1
            try:
                file_path = os.path.join(root, file)
                # Получаем размер файла
                file_size = os.path.getsize(file_path)
                # Если файл больше минимального размера, добавляем в список
                if file_size >= MIN_FILE_SIZE:
                    large_files.append((file_path, file_size))
            except (PermissionError, FileNotFoundError, OSError):
                # Игнорируем ошибки доступа
                continue
        
        # Обрабатываем директории верхнего уровня для определения их размера
        if root == start_path:
            print("\nВычисление размеров директорий верхнего уровня...")
            top_dirs = [os.path.join(root, d) for d in dirs]
            
            # Используем многопоточность для ускорения расчета размеров директорий
            with ThreadPoolExecutor(max_workers=min(10, os.cpu_count() or 4)) as executor:
                # Запускаем параллельное вычисление размера каждой директории
                future_to_dir = {executor.submit(get_dir_size, d): d for d in top_dirs}
                
                # Собираем результаты по мере их завершения
                for i, future in enumerate(future_to_dir):
                    dir_path = future_to_dir[future]
                    try:
                        dir_size = future.result()
                        dirs_info[dir_path] = dir_size
                        print(f"Прогресс: {i+1}/{len(top_dirs)} директорий. Текущая: {os.path.basename(dir_path)}", end='\r')
                    except Exception as e:
                        print(f"\nОшибка при обработке {dir_path}: {str(e)}")
    
    # Получаем TOP-N больших файлов
    largest_files = nlargest(MAX_RESULTS, large_files, key=lambda x: x[1])
    
    # Преобразуем информацию о директориях в список и сортируем
    largest_dirs = nlargest(MAX_RESULTS, [(path, size) for path, size in dirs_info.items()], key=lambda x: x[1])
    
    return largest_files, largest_dirs

# Основная функция
def main():
    """Основная функция программы"""
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        start_path = sys.argv[1]
    else:
        # По умолчанию используем корневую директорию
        if sys.platform == 'win32':
            start_path = os.environ.get('SYSTEMDRIVE', 'C:')
        else:
            start_path = '/'
    
    print(f"Текущая дата и время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Сканирование начинается с: {start_path}")
    
    # Запускаем сканирование
    start_time = time.time()
    largest_files, largest_dirs = scan_system(start_path)
    end_time = time.time()
    
    # Вывод результатов
    print("\n" + "="*80)
    print(f"Сканирование завершено за {end_time - start_time:.2f} секунд.")
    
    # Вывод информации о больших файлах
    print("\n" + "="*80)
    print(f"ТОП-{MAX_RESULTS} САМЫХ БОЛЬШИХ ФАЙЛОВ (более {format_size(MIN_FILE_SIZE)}):")
    print("="*80)
    for i, (file_path, file_size) in enumerate(largest_files, 1):
        print(f"{i}. {file_path}")
        print(f"   Размер: {format_size(file_size)}")
    
    # Вывод информации о больших директориях
    print("\n" + "="*80)
    print(f"ТОП-{MAX_RESULTS} САМЫХ БОЛЬШИХ ДИРЕКТОРИЙ:")
    print("="*80)
    for i, (dir_path, dir_size) in enumerate(largest_dirs, 1):
        print(f"{i}. {dir_path}")
        print(f"   Размер: {format_size(dir_size)}")
    
    # Сохранение результатов в файл
    output_file = f"disk_space_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"Отчет о дисковом пространстве от {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Сканирование начиналось с: {start_path}\n\n")
            
            f.write("="*80 + "\n")
            f.write(f"ТОП-{MAX_RESULTS} САМЫХ БОЛЬШИХ ФАЙЛОВ (более {format_size(MIN_FILE_SIZE)}):\n")
            f.write("="*80 + "\n")
            for i, (file_path, file_size) in enumerate(largest_files, 1):
                f.write(f"{i}. {file_path}\n")
                f.write(f"   Размер: {format_size(file_size)}\n")
            
            f.write("\n" + "="*80 + "\n")
            f.write(f"ТОП-{MAX_RESULTS} САМЫХ БОЛЬШИХ ДИРЕКТОРИЙ:\n")
            f.write("="*80 + "\n")
            for i, (dir_path, dir_size) in enumerate(largest_dirs, 1):
                f.write(f"{i}. {dir_path}\n")
                f.write(f"   Размер: {format_size(dir_size)}\n")
        
        print(f"\nОтчет сохранен в файл: {output_file}")
    except Exception as e:
        print(f"\nОшибка при сохранении отчета: {str(e)}")

if __name__ == "__main__":
    main()