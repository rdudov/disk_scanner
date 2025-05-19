#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
from pathlib import Path
from datetime import datetime
from heapq import nlargest
from concurrent.futures import ThreadPoolExecutor
import traceback

# Минимальный размер для "больших" файлов (10 МБ)
MIN_FILE_SIZE = 10 * 1024 * 1024  # 10 MB в байтах

# Минимальный размер для иерархии директорий (500 МБ)
MIN_DIR_SIZE_HIERARCHY = 500 * 1024 * 1024  # 500 MB в байтах

# Максимальное количество результатов для отображения
MAX_RESULTS = 100

# Форматирование размеров файлов для удобного чтения
def format_size(size_bytes):
    """Преобразует размер в байтах в человекочитаемый формат"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024 or unit == 'TB':
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024

# Получение размера директории и её поддиректорий
def get_dir_size_and_subdirs(path):
    """
    Рекурсивно вычисляет размер директории и возвращает структуру поддиректорий
    Возвращает: (общий размер, словарь {поддиректория: (размер, поддиректории)})
    """
    total_size = 0
    subdirs = {}
    
    try:
        # Используем os.scandir вместо listdir для лучшей производительности
        for entry in os.scandir(path):
            try:
                if entry.is_file(follow_symlinks=False):
                    # Это файл, просто добавляем его размер
                    file_size = entry.stat().st_size
                    total_size += file_size
                elif entry.is_dir(follow_symlinks=False):
                    # Это директория, рекурсивно вычисляем её размер и структуру
                    subdir_size, subdir_structure = get_dir_size_and_subdirs(entry.path)
                    if subdir_size > 0:  # Пропускаем пустые директории
                        subdirs[entry.path] = (subdir_size, subdir_structure)
                        total_size += subdir_size
            except (PermissionError, FileNotFoundError, OSError):
                # Игнорируем ошибки доступа к файлам и директориям
                continue
    except (PermissionError, FileNotFoundError, OSError) as e:
        # При ошибке доступа к директории, возвращаем 0 и пустую структуру
        return 0, {}
    
    return total_size, subdirs

# Функция для печати иерархии директорий
def print_dir_hierarchy(path, size, structure, depth=0, file=None, min_size=MIN_DIR_SIZE_HIERARCHY):
    """
    Рекурсивно печатает иерархию директорий, начиная с указанной.
    Показывает только директории больше min_size.
    """
    indent = "  " * depth
    dir_name = os.path.basename(path) or path  # Используем полный путь, если имя пустое
    
    output = f"{indent}{'└─ ' if depth > 0 else ''}{dir_name} [{format_size(size)}]"
    print(output)
    if file:
        file.write(output + "\n")
    
    # Сортируем поддиректории по размеру (от большего к меньшему)
    sorted_subdirs = sorted(structure.items(), key=lambda x: x[1][0], reverse=True)
    
    # Печатаем только достаточно большие поддиректории
    for subdir_path, (subdir_size, subdir_structure) in sorted_subdirs:
        if subdir_size >= min_size:
            print_dir_hierarchy(subdir_path, subdir_size, subdir_structure, 
                               depth + 1, file, min_size)

# Основная функция сканирования
def scan_system(start_path):
    """Сканирует систему, находя большие файлы и директории"""
    if not os.path.exists(start_path):
        print(f"Путь {start_path} не существует!")
        return [], [], {}
    
    # Для отслеживания прогресса
    last_status_time = time.time()
    total_items_scanned = 0
    
    # Для хранения больших файлов
    large_files = []
    
    # Сканирование файлов
    print(f"Начинаю поиск больших файлов от {start_path}...")
    print("Это может занять некоторое время в зависимости от размера диска и количества файлов.")
    
    for root, dirs, files in os.walk(start_path, topdown=True, onerror=None, followlinks=False):
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
    
    print("\nПоиск больших файлов завершен.")
    
    # Получаем TOP-N больших файлов
    largest_files = nlargest(MAX_RESULTS, large_files, key=lambda x: x[1])
    
    # Теперь собираем информацию о директориях
    print("\nНачинаю анализ иерархии директорий...")
    print("Это может занять некоторое время для больших дисков.")
    
    # Получаем размеры и структуру директорий верхнего уровня
    top_dirs = []
    dir_structures = {}
    
    # Получаем список директорий верхнего уровня
    try:
        top_dirs = [os.path.join(start_path, d) for d in os.listdir(start_path) 
                  if os.path.isdir(os.path.join(start_path, d)) and 
                  not d.startswith(('.', '$', 'System Volume Information'))]
    except (PermissionError, FileNotFoundError, OSError) as e:
        print(f"\nОшибка при получении списка директорий: {str(e)}")
        top_dirs = []
    
    # Используем многопоточность для ускорения расчета размеров директорий
    print(f"Анализирую {len(top_dirs)} директорий верхнего уровня...")
    
    # Подготавливаем структуры для хранения результатов
    dir_sizes = {}
    dir_structures = {}
    
    with ThreadPoolExecutor(max_workers=min(10, os.cpu_count() or 4)) as executor:
        futures = []
        for dir_path in top_dirs:
            futures.append(executor.submit(get_dir_size_and_subdirs, dir_path))
        
        # Собираем результаты по мере их завершения
        for i, (dir_path, future) in enumerate(zip(top_dirs, futures)):
            try:
                dir_size, structure = future.result()
                dir_sizes[dir_path] = dir_size
                dir_structures[dir_path] = structure
                print(f"Прогресс: {i+1}/{len(top_dirs)} директорий. Текущая: {os.path.basename(dir_path)}", end='\r')
            except Exception as e:
                print(f"\nОшибка при обработке {dir_path}: {str(e)}")
    
    # Сортируем директории по размеру
    largest_dirs = nlargest(MAX_RESULTS, [(path, size) for path, size in dir_sizes.items()], key=lambda x: x[1])
    
    return largest_files, largest_dirs, dir_structures

# Основная функция
def main():
    """Основная функция программы"""
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        start_path = sys.argv[1]
    else:
        # По умолчанию используем корневую директорию
        if sys.platform == 'win32':
            start_path = os.environ.get('SYSTEMDRIVE', 'C:\\')
        else:
            start_path = '/'
    
    print(f"Текущая дата и время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Сканирование начинается с: {start_path}")
    
    # Запускаем сканирование
    start_time = time.time()
    try:
        largest_files, largest_dirs, dir_structures = scan_system(start_path)
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
        
        # Вывод иерархии директорий
        print("\n" + "="*80)
        print(f"ИЕРАРХИЯ ДИРЕКТОРИЙ (более {format_size(MIN_DIR_SIZE_HIERARCHY)}):")
        print("="*80)
        
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
                
                f.write("\n" + "="*80 + "\n")
                f.write(f"ИЕРАРХИЯ ДИРЕКТОРИЙ (более {format_size(MIN_DIR_SIZE_HIERARCHY)}):\n")
                f.write("="*80 + "\n")
                
                # Выводим иерархию для каждой директории верхнего уровня
                for dir_path, dir_size in largest_dirs:
                    if dir_size >= MIN_DIR_SIZE_HIERARCHY:
                        print(f"\nИерархия для: {dir_path}")
                        f.write(f"\nИерархия для: {dir_path}\n")
                        structure = dir_structures.get(dir_path, {})
                        print_dir_hierarchy(dir_path, dir_size, structure, file=f)
            
            print(f"\nОтчет сохранен в файл: {output_file}")
        except Exception as e:
            print(f"\nОшибка при сохранении отчета: {str(e)}")
            traceback.print_exc()
            
    except KeyboardInterrupt:
        print("\n\nСканирование прервано пользователем.")
    except Exception as e:
        print(f"\nПроизошла ошибка: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()