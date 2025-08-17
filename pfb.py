import os
import json
from ftplib import FTP
import sqlite3
import time
from logger import log_console_out, exception_handler, read_config_ini


def clean_fn_sale_task():
    try:
        config = read_config_ini("source/config.ini")
        dbname = config.get("db-update", "db-name", fallback=None)

        conn = sqlite3.connect(f'source/{dbname}.db')
        cursor = conn.cursor()

        conn = sqlite3.connect(f'source/{dbname}.db')
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fn_sale_task (
                serialNumber TEXT PRIMARY KEY,
                fn_serial TEXT
            )
        ''')
        conn.commit()
        conn.close()

        conn = sqlite3.connect(f'source/{dbname}.db')
        cursor = conn.cursor()

        # Получаем все записи из fn_sale_task
        cursor.execute('SELECT serialNumber, fn_serial FROM fn_sale_task')
        task_records = cursor.fetchall()

        for task_serial, task_fn in task_records:
            # Ищем соответствующую запись в pos_fiscals
            cursor.execute('''
                SELECT fn_serial 
                FROM pos_fiscals 
                WHERE serialNumber = ?
            ''', (task_serial,))

            pos_record = cursor.fetchone()

            # Удаляем запись если:
            # 1. serialNumber не найден в pos_fiscals (pos_record is None)
            # 2. fn_serial не совпадает
            if pos_record is None or pos_record[0] != task_fn:
                cursor.execute('''
                    DELETE FROM fn_sale_task 
                    WHERE serialNumber = ?
                ''', (task_serial,))
                log_console_out(
                    f"Удалена неактуальная запись из fn_sale_task: serialNumber={task_serial}, fn_serial={task_fn}",
                    "pfb")

        conn.commit()
        conn.close()

        log_console_out("Очистка устаревших записей в fn_sale_task завершена", "pfb")

    except Exception as e:
        log_console_out("Error: не удалось выполнить очистку fn_sale_task", "pfb")
        exception_handler(type(e), e, e.__traceback__, "pfb")


def ftp_connect():
    config = read_config_ini("source/config.ini")
    dbupdate_period = int(config.get("db-update", "dbupdate-period-sec", fallback=None))
    while True:
        log_console_out("Начато обновление базы ФР", "pfb")
        try:
            FTP_HOST = config.get("ftp-connect", "ftpHost", fallback=None)
            FTP_USER = config.get("ftp-connect", "ftpUser", fallback=None)
            FTP_PASS = config.get("ftp-connect", "ftpPass", fallback=None)

            # Подключение к FTP и чтение файлов JSON
            ftp = FTP(FTP_HOST)
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
        except Exception as e:
            log_console_out("Error: Не удалось подключиться к FTP-серверу, проверьте параметры подключения", "pfb")
            exception_handler(type(e), e, e.__traceback__, "pfb")
            log_console_out(f"Следущая попытка обновления будет произведена через ({dbupdate_period}) секунд.", "pfb")
            time.sleep(dbupdate_period)
            continue

        try:
            files = []
            ftp.retrlines('LIST', lambda x: files.append(x.split()))
            json_files = [f[8] for f in files if f[8].endswith('.json')]

            # Создание временной директории для сохранения файлов
            temp_dir = 'temp_files'
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)

            # Скачивание файлов JSON на локальный компьютер
            for filename in json_files:
                local_filename = os.path.join(temp_dir, filename)
                with open(local_filename, 'wb') as file:
                    ftp.retrbinary('RETR ' + filename, file.write)
                with open(local_filename, 'r', encoding='utf-8') as file:
                    try:
                        json_data = json.load(file)
                        if "serialNumber" in json_data:
                            get_db_data({json_data["serialNumber"]: json_data})
                        else:
                            # Если ключ "serialNumber" отсутствует, сохраняем JSON в отдельную таблицу
                            save_not_fiscal(json_data, filename)
                        # Удаление временных файлов после чтения данных
                        os.remove(local_filename)
                    except json.JSONDecodeError:
                        log_console_out(f"Error: Файл {filename} содержит некорректный JSON, пропускаем", "pfb")

            ftp.quit()

            clean_fn_sale_task()

            log_console_out("Обновление базы ФР завершено", "pfb")
            log_console_out(f"Следущее обновление будет произведено через ({dbupdate_period}) секунд.", "pfb")

            time.sleep(dbupdate_period)

        except Exception as e:
            ftp.quit()
            log_console_out(f"Error: не удалось загрузить файл c FTP", "pfb")
            exception_handler(type(e), e, e.__traceback__, "pfb")
            log_console_out(f"Следущая попытка обновления будет произведена через ({dbupdate_period}) секунд.", "pfb")
            time.sleep(dbupdate_period)
            continue


def save_not_fiscal(json_data, filename):
    try:
        config = read_config_ini("source/config.ini")
        dbname = config.get("db-update", "db-name", fallback=None)
        conn = sqlite3.connect(f'source/{dbname}.db')
        cursor = conn.cursor()

        # Получение списка уникальных ключей JSON для создания столбцов
        json_keys = json_data.keys()

        # Создание таблицы для нефискальных JSON-файлов, если её ещё нет
        cursor.execute('''CREATE TABLE IF NOT EXISTS pos_not_fiscals (
                            filename TEXT PRIMARY KEY
                        )''')

        # Получение существующих столбцов из таблицы
        cursor.execute('PRAGMA table_info(pos_not_fiscals)')
        existing_columns = {row[1] for row in cursor.fetchall()}

        # Проверка и добавление новых столбцов
        for key in json_keys:
            if key not in existing_columns:
                cursor.execute(f'''ALTER TABLE pos_not_fiscals ADD COLUMN {key} TEXT''')
                existing_columns.add(key)

        # Формирование значений для вставки
        values = []
        columns = []
        for key in existing_columns:
            if key != 'filename':  # Пропускаем filename, так как он обрабатывается отдельно
                columns.append(key)
                value = json_data.get(key, '')
                if isinstance(value, dict):
                    value = json.dumps(value, ensure_ascii=False)
                values.append(value)

        # Формирование SQL запроса
        placeholders = ', '.join(['?'] * len(values))
        columns_str = ', '.join(columns)

        # Вставка данных
        cursor.execute(
            f'''INSERT OR REPLACE INTO pos_not_fiscals (filename, {columns_str}) 
                VALUES (?, {placeholders})''',
            (filename, *values)
        )

        conn.commit()
    except Exception as e:
        log_console_out(f"Error: Не удалось сохранить JSON-файл {filename} в таблицу pos_not_fiscals", "pfb")
        exception_handler(type(e), e, e.__traceback__, "pfb")
    finally:
        conn.close()

def get_db_data(data):
    try:
        config = read_config_ini("source/config.ini")
        dbname = config.get("db-update", "db-name", fallback=None)
        #log_console_out("Начато обновление базы ФР", "pfb")

        # Создание SQLite-базы данных и подключение к ней
        conn = sqlite3.connect(f'source/{dbname}.db')
        cursor = conn.cursor()

        # Создание таблицы, если она не существует
        cursor.execute('''CREATE TABLE IF NOT EXISTS pos_fiscals (
                            serialNumber TEXT PRIMARY KEY
                        )''')

        # Получение существующих столбцов из таблицы
        cursor.execute('PRAGMA table_info(pos_fiscals)')
        existing_columns = {row[1] for row in cursor.fetchall()}

        try:
            # Вставка данных
            for filename, json_data in data.items():
                # Проверка и добавление новых столбцов
                for key, value in json_data.items():
                    if key not in existing_columns:
                        cursor.execute(f'''ALTER TABLE pos_fiscals ADD COLUMN {key} TEXT''')
                        existing_columns.add(key)

                # Формирование значений для вставки
                values = []
                for col in existing_columns:
                    value = json_data.get(col, '')
                    if isinstance(value, dict):
                        value = json.dumps(value, ensure_ascii=False)  # Сериализация словаря в JSON-строку
                    values.append(value)

                placeholders = ', '.join(['?'] * len(values))
                # Используем оператор INSERT OR REPLACE для обновления текущих полей при совпадении первичного ключа
                cursor.execute(
                    f'''INSERT OR REPLACE INTO pos_fiscals (serialNumber, {', '.join(existing_columns)}) VALUES (?, {placeholders})''',
                    (filename, *values))
        except Exception as e:
            log_console_out(f"Error: Файл уже был удалён", "pfb")
            exception_handler(type(e), e, e.__traceback__, "pfb")
            pass

        # Сохранение изменений и закрытие соединения
        conn.commit()
        conn.close()
        #log_console_out(f"ФР с серийным номером '{filename}' добавлен\обновлён в БД.", "pfb")
    except Exception as e:
        log_console_out("Error: попытка сохранить полученные данные в базу данных завершилась неудачей", "pfb")
        exception_handler(type(e), e, e.__traceback__, "pfb")