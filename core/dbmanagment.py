import core.sys_manager
import core.logger
import core.configs
import core.connectors
import about
from datetime import datetime, timedelta
from flask import request
import os
import json
import sqlite3
import time

iikorms = core.connectors.IikoRms()

class DatabaseContextManager(core.sys_manager.ResourceManagement):
    def __init__(self):
        super().__init__()
        self.dbname = self.config.get("db-update", "db-name", fallback=None)
        self.format_db_path = about.db_path.format(dbname=self.dbname)
        self.conn = None
        self.cursor = None

    def __enter__(self):
        try:
            self.conn = sqlite3.connect(self.format_db_path)
            self.cursor = self.conn.cursor()
            return self
        except Exception:
            core.logger.db_service.error(
                "Не удалось подключиться к базе данных", exc_info=True)
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            # Если произошло исключение, откатываем изменения
            if self.conn:
                try: self.conn.rollback()
                except: pass
        else:
            # Если всё в порядке, сохраняем изменения
            if self.conn:
                try: self.conn.commit()
                except: pass

        # Закрываем соединение в любом случае
        if self.conn:
            try: self.conn.close()
            except: pass

class DbUpdate(DatabaseContextManager):
    def __init__(self):
        super().__init__()
        self.dbupdate_period = int(self.config.get("db-update", "dbupdate-period-sec", fallback=None))

    def pos_tables_update(self):
        clients_update = 0

        while True:
            core.logger.db_service.info("Начато обновление базы ФР")

            try:
                with core.connectors.FtpContextManager() as ftp:
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
                                    self.save_fiscals({json_data["serialNumber"]: json_data})
                                else:
                                    # Если ключ "serialNumber" отсутствует, сохраняем JSON в отдельную таблицу
                                    self.save_not_fiscal(json_data, filename)
                            except json.JSONDecodeError:
                                core.logger.db_service.error(
                                    f"Файл {filename} содержит некорректный JSON, пропускаем", exc_info=True)
                        # Удаление временных файлов после чтения данных
                        os.remove(local_filename)

                self.clean_fn_sale_task()

                if clients_update == 0:
                    iikorms.update_clients_info()
                    clients_update = 1

                core.logger.db_service.info("Обновление базы ФР завершено")
                core.logger.db_service.info(
                    f"Следущее обновление будет произведено через ({self.dbupdate_period}) секунд")

                time.sleep(self.dbupdate_period)

            except Exception:
                core.logger.db_service.error(f"Не удалось загрузить файл c FTP", exc_info=True)
                core.logger.db_service.info(
                    f"Следущая попытка обновления будет произведена через ({self.dbupdate_period}) секунд")
                time.sleep(self.dbupdate_period)
                continue

    def save_not_fiscal(self, json_data, filename):
        try:
            with DatabaseContextManager() as db:
                # Получение списка уникальных ключей JSON для создания столбцов
                json_keys = json_data.keys()

                # Создание таблицы для нефискальных JSON-файлов, если её ещё нет
                db.cursor.execute('''CREATE TABLE IF NOT EXISTS pos_not_fiscals (
                                    filename TEXT PRIMARY KEY
                                )''')

                # Получение существующих столбцов из таблицы
                db.cursor.execute('PRAGMA table_info(pos_not_fiscals)')
                existing_columns = {row[1] for row in db.cursor.fetchall()}

                # Проверка и добавление новых столбцов
                for key in json_keys:
                    if key not in existing_columns:
                        db.cursor.execute(f'''ALTER TABLE pos_not_fiscals ADD COLUMN {key} TEXT''')
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
                db.cursor.execute(
                    f'''INSERT OR REPLACE INTO pos_not_fiscals (filename, {columns_str}) 
                        VALUES (?, {placeholders})''',
                    (filename, *values)
                )
        except Exception:
            core.logger.db_service.error(
                f"Не удалось сохранить JSON-файл {filename} в таблицу [pos_not_fiscals]", exc_info=True)

    def save_fiscals(self, data):
        try:
            with DatabaseContextManager() as db:
                # Создание таблицы, если она не существует
                db.cursor.execute('''CREATE TABLE IF NOT EXISTS pos_fiscals (
                                    serialNumber TEXT PRIMARY KEY
                                )''')

                # Получение существующих столбцов из таблицы
                db.cursor.execute('PRAGMA table_info(pos_fiscals)')
                existing_columns = {row[1] for row in db.cursor.fetchall()}

                try:
                    # Вставка данных
                    for filename, json_data in data.items():
                        # Проверка и добавление новых столбцов
                        for key, value in json_data.items():
                            if key not in existing_columns:
                                db.cursor.execute(f'''ALTER TABLE pos_fiscals ADD COLUMN {key} TEXT''')
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
                        db.cursor.execute(
                            f'''INSERT OR REPLACE INTO pos_fiscals (serialNumber, {', '.join(existing_columns)}) VALUES (?, {placeholders})''',
                            (filename, *values))
                except Exception:
                    core.logger.db_service.error(f"Файл уже был удалён", exc_info=True)
                    pass
        except Exception:
            core.logger.db_service.error(
                f"Попытка сохранить данные '{data}' в базу данных завершилась неудачей", exc_info=True)

    def clean_fn_sale_task(self):
        try:
            with DatabaseContextManager() as db:
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS fn_sale_task (
                        serialNumber TEXT PRIMARY KEY,
                        fn_serial TEXT
                    )
                ''')

                # Получаем все записи из fn_sale_task
                db.cursor.execute('SELECT serialNumber, fn_serial FROM fn_sale_task')
                task_records = db.cursor.fetchall()

                for task_serial, task_fn in task_records:
                    # Ищем соответствующую запись в pos_fiscals
                    db.cursor.execute('''
                        SELECT fn_serial 
                        FROM pos_fiscals 
                        WHERE serialNumber = ?
                    ''', (task_serial,))

                    pos_record = db.cursor.fetchone()

                    # Удаляем запись если:
                    # 1. serialNumber не найден в pos_fiscals (pos_record is None)
                    # 2. fn_serial не совпадает
                    if pos_record is None or pos_record[0] != task_fn:
                        db.cursor.execute('''
                            DELETE FROM fn_sale_task 
                            WHERE serialNumber = ?
                        ''', (task_serial,))
                        core.logger.db_service.info(
                            f"Удалена неактуальная запись из fn_sale_task: serialNumber={task_serial}, fn_serial={task_fn}")

                core.logger.db_service.info("Очистка устаревших записей в 'fn_sale_task' завершена")

        except Exception:
            core.logger.db_service.error("Не удалось выполнить очистку fn_sale_task", exc_info=True)


class DbQueries(DatabaseContextManager):
    def __init__(self):
        super().__init__()
        self.dont_valid_fn = int(self.config.get("db-update", "day_filter_expire", fallback=5))

    def get_data_pos_fiscals(self):
        try:
            with DatabaseContextManager() as db:
                db.cursor.execute("SELECT * FROM pos_fiscals")
                data = db.cursor.fetchall()
                db.cursor.execute("PRAGMA table_info(pos_fiscals)")
                columns = [column[1] for column in db.cursor.fetchall()]

                # Создаем новый список для данных с дополнительной информацией об устаревании
                modified_data = []
                for row in data:
                    modified_row = list(row)
                    licenses_data_index = columns.index('licenses')
                    current_time_index = columns.index('current_time')
                    v_time_index = columns.index('v_time')

                    # Обработка licenses
                    licenses_data = row[licenses_data_index]
                    if licenses_data:
                        modified_row[licenses_data_index] = licenses_data

                    # Проверка устаревания записи
                    time_to_check = row[v_time_index] if row[v_time_index] not in (None, '', 'None') else row[
                        current_time_index]
                    is_expired = not self.if_show_fn_to_date(
                        time_to_check, self.dont_valid_fn) if time_to_check else False

                    # Добавляем признак устаревания в строку
                    modified_row.append(is_expired)

                    modified_data.append(modified_row)

            return modified_data, columns
        except Exception:
            core.logger.db_service.error("При чтении таблицы 'pos_fiscals' произошло исключение", exc_info=True)

    def only_pos(self):
        try:
            with DatabaseContextManager() as db:
                db.cursor.execute("SELECT * FROM pos_not_fiscals")
                data = db.cursor.fetchall()
                db.cursor.execute("PRAGMA table_info(pos_not_fiscals)")
                columns = [column[1] for column in db.cursor.fetchall()]  # Получаем названия столбцов
            return data, columns
        except Exception:
            core.logger.db_service.error("При чтении таблицы 'pos_not_fiscals' произошло исключение", exc_info=True)

    def search_querie(self):
        try:
            if request.method == 'POST':
                search_query = request.form['search_query']

                with DatabaseContextManager() as db:
                    db.cursor.execute("PRAGMA table_info(pos_fiscals)")
                    columns = [column[1] for column in db.cursor.fetchall()]

                    query = "SELECT * FROM pos_fiscals WHERE "
                    for column in columns:
                        query += f"{column} LIKE '%{search_query}%' OR "
                    query = query[:-4]

                    db.cursor.execute(query)
                    search_results = db.cursor.fetchall()

                    # Создаем новый список для данных с проверкой на устаревание
                    modified_data = []
                    for row in search_results:
                        modified_row = list(row)
                        licenses_data_index = columns.index('licenses')
                        current_time_index = columns.index('current_time')
                        v_time_index = columns.index('v_time')

                        # Обработка licenses
                        licenses_data = row[licenses_data_index]
                        if licenses_data:
                            modified_row[licenses_data_index] = licenses_data

                        # Проверка устаревания записи
                        time_to_check = row[v_time_index] if row[v_time_index] not in (None, '', 'None') else row[
                            current_time_index]
                        is_expired = not self.if_show_fn_to_date(time_to_check, self.dont_valid_fn) if time_to_check else False

                        # Добавляем признак устаревания в строку
                        modified_row.append(is_expired)

                        modified_data.append(modified_row)

                default_visible_columns = ['serialNumber', 'modelName', 'RNM', 'organizationName', 'fn_serial',
                                           'dateTime_end',
                                           'bootVersion', 'ffdVersion', 'INN', 'attribute_excise', 'attribute_marked',
                                           'installed_driver', 'url_rms', 'teamviewer_id', 'anydesk_id',
                                           'litemanager_id']

                return search_query, modified_data, columns, default_visible_columns
        except Exception:
            core.logger.db_service.error("Не удалось сделать поисковый запрос", exc_info=True)


    def get_expire_fn(self):
        try:
            with DatabaseContextManager() as db:
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS fn_sale_task (
                        serialNumber TEXT PRIMARY KEY,
                        fn_serial TEXT
                    )
                ''')

                # Добавляем создание таблицы clients, если она не существует
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS clients (
                        id TEXT PRIMARY KEY,
                        url_rms TEXT,
                        INN TEXT,
                        organizationName TEXT,
                        serverName TEXT,
                        version TEXT,
                        manual_edit INTEGER DEFAULT 0,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

            if request.method == 'POST':
                start_date = request.form.get('start_date')
                end_date = request.form.get('end_date')
            else:
                start_date = request.args.get('start_date')
                end_date = request.args.get('end_date')
                if not start_date or not end_date:
                    start_date, end_date = self.get_default_dates()

            show_marked_only = request.args.get('show_marked_only', 'true') == 'true'

            with DatabaseContextManager() as db:
                db.cursor.execute('SELECT serialNumber FROM fn_sale_task')
                marked_records = {row[0] for row in db.cursor.fetchall()}

                base_query = """
                    SELECT pos_fiscals.serialNumber, 
                           clients.serverName as client,
                           pos_fiscals.RNM, 
                           pos_fiscals.fn_serial, 
                           pos_fiscals.organizationName, 
                           pos_fiscals.INN, 
                           date(pos_fiscals.dateTime_end) as dateTime_end,
                           pos_fiscals.current_time, 
                           pos_fiscals.v_time,
                           pos_fiscals.url_rms
                    FROM pos_fiscals 
                    LEFT JOIN clients ON pos_fiscals.url_rms = clients.url_rms
                    WHERE date(dateTime_end) >= date(?) AND date(dateTime_end) <= date(?)
                """

                if not show_marked_only:
                    base_query += " AND serialNumber NOT IN (SELECT serialNumber FROM fn_sale_task)"

                base_query += " ORDER BY dateTime_end ASC"

                db.cursor.execute(base_query, (start_date, end_date))
                rows = db.cursor.fetchall()

                records = []
                for row in rows:
                    record = dict(
                        zip(['serialNumber', 'client', 'RNM', 'fn_serial', 'organizationName', 'INN',
                             'dateTime_end', 'current_time', 'v_time', 'url_rms'], row))

                    # Определяем, какое время использовать
                    time_to_check = record['v_time'] if record['v_time'] not in (None, '', 'None') else record[
                        'current_time']

                    # Проверяем условие через функцию if_show_fn_to_date
                    if self.if_show_fn_to_date(time_to_check, self.dont_valid_fn):
                        # Удаляем временные поля из словаря
                        del record['current_time']
                        del record['v_time']
                        # Добавляем информацию о том, отмечена ли запись
                        record['is_marked'] = record['serialNumber'] in marked_records
                        records.append(record)

            return records, start_date, end_date, show_marked_only
        except Exception:
            core.logger.db_service.error("Неожиданное исключение при запросе к заканчивающимся ФН", exc_info=True)

    def search_dont_update(self, field):
        try:
            if request.method == 'POST':
                search_query = request.form['search_query']
                days = int(search_query)

                with DatabaseContextManager() as db:
                    # Преобразуем текущую дату в формат, который хранится в базе данных
                    today_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    # Вычисляем дату, которая на days дней меньше текущей даты
                    past_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

                    db.cursor.execute("PRAGMA table_info(pos_fiscals)")  # Получаем названия столбцов из базы данных
                    columns = [column[1] for column in db.cursor.fetchall()]

                    # Создаем запрос SQL для выборки строк, удовлетворяющих условиям
                    query = f"SELECT * FROM pos_fiscals WHERE strftime('%s', [{field}]) < strftime('%s', '{past_date}')"
                    db.cursor.execute(query)
                    search_results = db.cursor.fetchall()
                    # Создаем новый список для данных с замененными значениями в столбце licenses
                    modified_data = []
                    for row in search_results:
                        modified_row = list(row)  # Преобразуем кортеж в список
                        licenses_data_index = columns.index('licenses')
                        current_time_index = columns.index('current_time')
                        v_time_index = columns.index('v_time')

                        licenses_data = row[licenses_data_index]
                        if licenses_data:  # Если есть данные в столбце licenses
                            # Замена данных в столбце licenses на ссылку
                            modified_row[licenses_data_index] = licenses_data

                        time_to_check = row[v_time_index] if row[v_time_index] not in (None, '', 'None') else row[
                            current_time_index]
                        is_expired = not self.if_show_fn_to_date(
                            time_to_check, self.dont_valid_fn) if time_to_check else False

                        modified_data.append(modified_row)

                        modified_row.append(is_expired)

                return search_query, modified_data, columns
        except Exception:
            core.logger.db_service.error("Не удалось сделать посиковый запрос", exc_info=True)

    def toggle_task(self, serial_number, fn_serial, checked):
        try:
            with DatabaseContextManager() as db:
                # Создаем таблицу если её нет
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS fn_sale_task (
                        serialNumber TEXT PRIMARY KEY,
                        fn_serial TEXT
                    )
                ''')

                if checked:
                    db.cursor.execute(
                        'INSERT OR IGNORE INTO fn_sale_task (serialNumber, fn_serial) VALUES (?, ?)',
                        (serial_number, fn_serial)
                    )
                else:
                    db.cursor.execute(
                        'DELETE FROM fn_sale_task WHERE serialNumber = ?',
                        (serial_number,)
                    )

                return {'status': 'success'}

        except Exception as e:
            core.logger.db_service.error(
                "Ошибка при обновлении таблицы fn_sale_task", exc_info=True
            )
            return {'status': 'error', 'message': str(e)}


    def update_client_name(self, url_rms, server_name):
        try:
            with DatabaseContextManager() as db:
                # Создаем таблицу clients если её нет
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS clients (
                        id TEXT PRIMARY KEY,
                        url_rms TEXT,
                        INN TEXT,
                        organizationName TEXT,
                        serverName TEXT,
                        version TEXT,
                        manual_edit INTEGER DEFAULT 0,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Проверяем, существует ли запись
                db.cursor.execute(
                    'SELECT id FROM clients WHERE url_rms = ?',
                    (url_rms,)
                )
                record = db.cursor.fetchone()

                if record:
                    # Обновляем существующую запись
                    db.cursor.execute('''
                        UPDATE clients 
                        SET serverName = ?, manual_edit = 1, last_updated = CURRENT_TIMESTAMP
                        WHERE url_rms = ?
                    ''', (server_name, url_rms))
                else:
                    # Создаем новую запись
                    import uuid
                    unique_id = str(uuid.uuid4())
                    db.cursor.execute('''
                        INSERT INTO clients 
                        (id, url_rms, serverName, manual_edit) 
                        VALUES (?, ?, ?, 1)
                    ''', (unique_id, url_rms, server_name))

                return {'success': True}

        except Exception as e:
            core.logger.db_service.error(
                "Ошибка при обновлении имени клиента", exc_info=True
            )
            return {'success': False, 'error': str(e)}
