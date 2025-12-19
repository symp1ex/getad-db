import core.sys_manager
import core.logger
import core.configs
import os
import json
import time
import threading
import uuid
import psycopg2
import psycopg2.extras


class DatabaseContextManager(core.sys_manager.ResourceManagement):
    def __init__(self):
        super().__init__()
        self.dbname = self.config.get("db-update", "db-name", fallback="getad")
        self.host = self.config.get("db-update", "host", fallback="localhost")
        self.port = self.config.get("db-update", "port", fallback="5432")
        self.user = self.config.get("db-update", "user", fallback="postgres")
        self.password = self.config.get("db-update", "password", fallback="")
        self.conn = None
        self.cursor = None

    def __enter__(self):
        try:
            # Пытаемся подключиться к базе данных
            try:
                self.conn = psycopg2.connect(
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )
                self.cursor = self.conn.cursor()
            except psycopg2.OperationalError as e:
                # Проверяем, что ошибка связана с отсутствием базы данных
                if "database" in str(e) and "does not exist" in str(e):
                    temp_conn = psycopg2.connect(
                        dbname='postgres',
                        user=self.user,
                        password=self.password,
                        host=self.host,
                        port=self.port
                    )
                    temp_conn.autocommit = True  # Необходимо для создания БД
                    temp_cursor = temp_conn.cursor()

                    # Создаём новую базу данных
                    temp_cursor.execute(f'CREATE DATABASE "{self.dbname}"')

                    # Закрываем временное соединение
                    temp_cursor.close()
                    temp_conn.close()

                    core.logger.db_service.info(f"Создана новая база данных: {self.dbname}")

                    # Теперь подключаемся к только что созданной базе
                    self.conn = psycopg2.connect(
                        dbname=self.dbname,
                        user=self.user,
                        password=self.password,
                        host=self.host,
                        port=self.port
                    )
                    self.cursor = self.conn.cursor()
                else:
                    # Если ошибка не связана с отсутствием БД, пробрасываем её дальше
                    raise

            return self
        except Exception:
            core.logger.db_service.error(
                "Не удалось подключиться к базе данных", exc_info=True)
            time.sleep(5)
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


class DbQueries(DatabaseContextManager):
    def __init__(self):
        super().__init__()
        try: self.dont_valid_fn = int(self.config.get("db-update", "day_filter_expire", fallback=14))
        except: self.dont_valid_fn = 14

    def save_not_fiscal(self, json_data, filename):
        try:
            with DatabaseContextManager() as db:
                # Получение списка уникальных ключей JSON для создания столбцов
                json_keys = json_data.keys()

                # Создание таблицы для нефискальных JSON-файлов, если её ещё нет
                db.cursor.execute('''CREATE TABLE IF NOT EXISTS pos_not_fiscals (
                                    "filename" TEXT PRIMARY KEY
                                );''')

                # Получение существующих столбцов из таблицы
                db.cursor.execute('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'pos_not_fiscals'
                ''')
                existing_columns = {row[0].lower() for row in db.cursor.fetchall()}

                # Проверка и добавление новых столбцов
                for key in json_keys:
                    if key.lower() not in existing_columns:
                        db.cursor.execute(f'''ALTER TABLE pos_not_fiscals ADD COLUMN "{key}" TEXT''')
                        existing_columns.add(key.lower())

                # Формирование значений для вставки
                values = []
                columns = []
                original_columns = []

                for key in json_keys:
                    if key.lower() != 'filename':  # Пропускаем filename, так как он обрабатывается отдельно
                        columns.append(f'"{key}"')
                        original_columns.append(key)
                        value = json_data.get(key, '')
                        if isinstance(value, dict):
                            value = json.dumps(value, ensure_ascii=False)
                        values.append(value)

                # Формирование SQL запроса
                placeholders = ', '.join(['%s'] * len(values))  # В PostgreSQL используем %s вместо ?
                columns_str = ', '.join(columns)

                # Вставка данных (используем ON CONFLICT вместо INSERT OR REPLACE)
                if columns:  # Проверяем, что у нас есть столбцы для вставки
                    db.cursor.execute(
                        f'''INSERT INTO pos_not_fiscals ("filename", {columns_str})
                            VALUES (%s, {placeholders})
                            ON CONFLICT ("filename") 
                            DO UPDATE SET {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in original_columns])}''',
                        (filename, *values)
                    )
                else:
                    # Если нет дополнительных столбцов, просто вставляем filename
                    db.cursor.execute(
                        f'''INSERT INTO pos_not_fiscals ("filename")
                            VALUES (%s)
                            ON CONFLICT ("filename") DO NOTHING''',
                        (filename,)
                    )
                core.logger.db_service.debug(f"Запись '{filename}' успешно добавлена в базу")
        except Exception:
            core.logger.db_service.error(
                f"Не удалось сохранить JSON-файл {filename} в таблицу [pos_not_fiscals]", exc_info=True)

    def save_fiscals(self, data):
        try:
            with DatabaseContextManager() as db:
                # Создание таблицы, если она не существует
                db.cursor.execute('''CREATE TABLE IF NOT EXISTS pos_fiscals (
                                    "serialNumber" TEXT PRIMARY KEY
                                )''')

                # Получение существующих столбцов из таблицы
                db.cursor.execute('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'pos_fiscals'
                ''')
                existing_columns = {row[0].lower() for row in db.cursor.fetchall()}  # Преобразуем в нижний регистр

                try:
                    # Вставка данных
                    for filename, json_data in data.items():
                        # Проверяем существование записи в pos_fiscals
                        db.cursor.execute('''
                            SELECT "serialNumber", "v_time" FROM pos_fiscals
                            WHERE "serialNumber" = %s
                        ''', (filename,))
                        existing_record = db.cursor.fetchone()

                        # Проверка v_time, если запись уже существует
                        if existing_record and "v_time" in json_data:
                            existing_v_time = existing_record[1]
                            new_v_time = json_data.get("v_time")

                            # Пропускаем обновление, если новый v_time отсутствует или старее существующего
                            if not new_v_time or (existing_v_time and str(existing_v_time) > str(new_v_time)):
                                core.logger.db_service.warning(
                                    f"Добавляемый файл имеет более раннюю дату, обновление записи пропущено")
                                return

                        # Проверка и добавление новых столбцов
                        for key, value in json_data.items():
                            if key.lower() not in existing_columns:
                                db.cursor.execute(f'''ALTER TABLE pos_fiscals ADD COLUMN "{key}" TEXT''')
                                existing_columns.add(key.lower())

                        # Формирование значений для вставки
                        values = []
                        columns = []
                        original_columns = []  # Сохраняем оригинальные названия столбцов

                        for key, value in json_data.items():
                            if key.lower() != 'serialnumber':  # Пропускаем serialNumber, обрабатываем отдельно
                                columns.append(f'"{key}"')  # Заключаем имя столбца в кавычки
                                original_columns.append(key)  # Сохраняем оригинальное имя для получения значений
                                value = json_data.get(key, '')
                                if isinstance(value, dict):
                                    value = json.dumps(value, ensure_ascii=False)
                                values.append(value)

                        # Формирование SQL запроса
                        placeholders = ', '.join(['%s'] * len(values))
                        columns_str = ', '.join(columns)

                        # Используем оператор INSERT с ON CONFLICT для обновления при совпадении первичного ключа
                        if columns:  # Проверяем, что у нас есть столбцы для вставки
                            db.cursor.execute(
                                f'''INSERT INTO pos_fiscals ("serialNumber", {columns_str})
                                   VALUES (%s, {placeholders})
                                   ON CONFLICT ("serialNumber") 
                                   DO UPDATE SET {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in original_columns])}''',
                                (filename, *values))
                        else:
                            # Если нет дополнительных столбцов, просто вставляем serialNumber
                            db.cursor.execute(
                                f'''INSERT INTO pos_fiscals ("serialNumber")
                                   VALUES (%s)
                                   ON CONFLICT ("serialNumber") DO NOTHING''',
                                (filename,))

                        # Если запись новая и есть url_rms - добавляем в таблицу clients
                        if not existing_record and 'url_rms' in json_data and json_data['url_rms']:
                            url_rms = json_data['url_rms']
                            inn = json_data.get('INN', '')
                            org_name = json_data.get('organizationName', '')

                            self.add_new_clients(url_rms, inn, org_name)

                    core.logger.db_service.debug(f"Запись '{filename}' успешно добавлена в базу")
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
                        "serialNumber" TEXT PRIMARY KEY,
                        "fn_serial" TEXT
                    )
                ''')

                # Получаем все записи из fn_sale_task
                db.cursor.execute('SELECT "serialNumber", "fn_serial" FROM fn_sale_task')
                task_records = db.cursor.fetchall()

                for task_serial, task_fn in task_records:
                    # Ищем соответствующую запись в pos_fiscals
                    db.cursor.execute('''
                        SELECT "fn_serial"
                        FROM pos_fiscals 
                        WHERE "serialNumber" = %s
                    ''', (task_serial,))

                    pos_record = db.cursor.fetchone()

                    # Удаляем запись если:
                    # 1. serialNumber не найден в pos_fiscals (pos_record is None)
                    # 2. fn_serial не совпадает
                    if pos_record is None or pos_record[0] != task_fn:
                        db.cursor.execute('''
                            DELETE FROM fn_sale_task 
                            WHERE "serialNumber" = %s
                        ''', (task_serial,))
                        core.logger.db_service.info(
                            f"Удалена неактуальная запись из fn_sale_task: serialNumber={task_serial}, fn_serial={task_fn}")

                core.logger.db_service.info("Очистка устаревших записей в 'fn_sale_task' завершена")

        except Exception:
            core.logger.db_service.error("Не удалось выполнить очистку fn_sale_task", exc_info=True)

    def clean_obsolete_clients(self):
        core.logger.db_service.info("Будет произведена очистка базы клиентов")

        try:
            with DatabaseContextManager() as db:
                # Получаем все url_rms из pos_fiscals
                db.cursor.execute('''
                    SELECT DISTINCT "url_rms" 
                    FROM pos_fiscals 
                    WHERE "url_rms" IS NOT NULL AND "url_rms" != ''
                ''')
                pos_fiscals_urls = {row[0] for row in db.cursor.fetchall()}

                # Получаем все записи из clients
                db.cursor.execute('''
                    SELECT "id", "url_rms" 
                    FROM clients
                    WHERE "url_rms" IS NOT NULL AND "url_rms" != ''
                ''')
                clients_records = db.cursor.fetchall()

                # Находим url_rms, которые есть в clients, но отсутствуют в pos_fiscals
                clients_to_delete = []
                clients_to_delete_list = []
                for client_id, client_url in clients_records:
                    if client_url not in pos_fiscals_urls:
                        clients_to_delete.append(client_id)
                        clients_to_delete_list.append(client_url)

                core.logger.db_service.debug("Список клиентов подлежащих удалению:")
                core.logger.db_service.debug(clients_to_delete_list)

                # Если есть записи для удаления
                if clients_to_delete:
                    core.logger.db_service.info(
                        f"Начато удаление ({len(clients_to_delete)}) устаревших записей из таблицы 'clients'")

                    # Удаляем записи
                    placeholders = ','.join(['%s'] * len(clients_to_delete))
                    db.cursor.execute(
                        f'''DELETE FROM clients WHERE "id" IN ({placeholders})''',
                        tuple(clients_to_delete)
                    )

                    core.logger.db_service.info(
                        f"Успешно удалено ({len(clients_to_delete)}) устаревших записей из таблицы 'clients'")
                else:
                    core.logger.db_service.info(
                        "Устаревшие записи в таблице 'clients' не найдены")

                core.logger.db_service.info(
                    f"Очистка базы клиентов завершена, следующая очистка через '24' часа")
        except Exception:
            core.logger.db_service.error(
                "Не удалось выполнить очистку устаревших записей из таблицы 'clients'", exc_info=True)

    def get_data_pos_fiscals(self):
        try:
            with DatabaseContextManager() as db:
                db.cursor.execute('SELECT * FROM pos_fiscals')
                data = db.cursor.fetchall()

                # Получаем названия столбцов из информационной схемы PostgreSQL
                db.cursor.execute('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'pos_fiscals'
                    ORDER BY ordinal_position
                ''')
                columns = [column[0] for column in db.cursor.fetchall()]

                # Создаём новый список для данных с дополнительной информацией об устаревании
                modified_data = []
                for row in data:
                    row_dict = dict(zip(columns, row))  # Преобразуем строку в словарь для удобства доступа
                    modified_row = list(row)

                    # Определяем индексы важных столбцов
                    licenses_data_index = columns.index('licenses') if 'licenses' in columns else -1
                    current_time_index = columns.index('current_time') if 'current_time' in columns else -1
                    v_time_index = columns.index('v_time') if 'v_time' in columns else -1

                    # Обработка licenses
                    if licenses_data_index >= 0 and row[licenses_data_index]:
                        modified_row[licenses_data_index] = row[licenses_data_index]

                    # Проверка устаревания записи
                    time_to_check = None
                    if v_time_index >= 0 and current_time_index >= 0:
                        time_to_check = row[v_time_index] if row[v_time_index] not in (None, '', 'None') else row[
                            current_time_index]

                    is_expired = False
                    if time_to_check:
                        is_expired = not self.if_show_fn_to_date(time_to_check, self.dont_valid_fn)

                    # Добавляем признак устаревания в строку
                    modified_row.append(is_expired)

                    modified_data.append(modified_row)

            return modified_data, columns
        except Exception:
            core.logger.db_service.error("При чтении таблицы 'pos_fiscals' произошло исключение", exc_info=True)
            return [], []

    def get_only_pos(self):
        try:
            with DatabaseContextManager() as db:
                db.cursor.execute('SELECT * FROM pos_not_fiscals')
                data = db.cursor.fetchall()

                # Получаем названия столбцов из информационной схемы PostgreSQL
                db.cursor.execute('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'pos_not_fiscals'
                    ORDER BY ordinal_position
                ''')
                columns = [column[0] for column in db.cursor.fetchall()]

            return data, columns
        except Exception:
            core.logger.db_service.error("При чтении таблицы 'pos_not_fiscals' произошло исключение", exc_info=True)
            return [], []

    def search_querie(self, search_query):
        try:
            with DatabaseContextManager() as db:
                # Получаем названия столбцов из информационной схемы PostgreSQL
                db.cursor.execute('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'pos_fiscals'
                    ORDER BY ordinal_position
                ''')
                columns = [column[0] for column in db.cursor.fetchall()]

                # Создаём запрос SQL для поиска по всем столбцам
                query = "SELECT * FROM pos_fiscals WHERE "
                conditions = []

                for column in columns:
                    conditions.append(f'"{column}"::TEXT ILIKE %s')

                # Соединяем условия поиска оператором OR
                query += " OR ".join(conditions)

                # Создаём список параметров для запроса (по одному '%значение%' на каждый столбец)
                params = [f'%{search_query}%'] * len(columns)

                # Выполняем запрос
                db.cursor.execute(query, params)
                search_results = db.cursor.fetchall()

                # Создаём новый список для данных с проверкой на устаревание
                modified_data = []
                for row in search_results:
                    modified_row = list(row)

                    # Определяем индексы важных столбцов
                    licenses_data_index = columns.index('licenses') if 'licenses' in columns else -1
                    current_time_index = columns.index('current_time') if 'current_time' in columns else -1
                    v_time_index = columns.index('v_time') if 'v_time' in columns else -1

                    # Обработка licenses
                    if licenses_data_index >= 0 and row[licenses_data_index]:
                        modified_row[licenses_data_index] = row[licenses_data_index]

                    # Проверка устаревания записи
                    time_to_check = None
                    if v_time_index >= 0 and current_time_index >= 0:
                        time_to_check = row[v_time_index] if row[v_time_index] not in (None, '', 'None') else row[
                            current_time_index]

                    is_expired = False
                    if time_to_check:
                        is_expired = not self.if_show_fn_to_date(time_to_check, self.dont_valid_fn)

                    # Добавляем признак устаревания в строку
                    modified_row.append(is_expired)

                    modified_data.append(modified_row)

                return modified_data, columns

        except Exception:
            core.logger.db_service.error("Не удалось сделать поисковый запрос", exc_info=True)

    def get_expire_fn(self, start_date, end_date, show_marked):
        try:
            with DatabaseContextManager() as db:
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS fn_sale_task (
                        "serialNumber" TEXT PRIMARY KEY,
                        "fn_serial" TEXT
                    )
                ''')

                # Добавляем создание таблицы clients, если она не существует
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS clients (
                        "id" TEXT PRIMARY KEY,
                        "url_rms" TEXT,
                        "INN" TEXT,
                        "organizationName" TEXT,
                        "serverName" TEXT,
                        "version" TEXT,
                        "manual_edit" INTEGER DEFAULT 0,
                        "last_updated" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

            with DatabaseContextManager() as db:
                db.cursor.execute('SELECT "serialNumber" FROM fn_sale_task')
                marked_records = {row[0] for row in db.cursor.fetchall()}

                base_query = """
                    SELECT pos_fiscals."serialNumber", 
                           clients."serverName" as client,
                           pos_fiscals."RNM", 
                           pos_fiscals."fn_serial", 
                           pos_fiscals."organizationName", 
                           pos_fiscals."INN", 
                           date(pos_fiscals."dateTime_end") as dateTime_end,
                           pos_fiscals."current_time", 
                           pos_fiscals."v_time",
                           pos_fiscals."url_rms",
                           pos_fiscals."address"
                    FROM pos_fiscals 
                    LEFT JOIN clients ON pos_fiscals."url_rms" = clients."url_rms"
                    WHERE date(pos_fiscals."dateTime_end") >= date(%s) AND date(pos_fiscals."dateTime_end") <= date(%s)
                """

                if not show_marked:
                    base_query += ' AND pos_fiscals."serialNumber" NOT IN (SELECT "serialNumber" FROM fn_sale_task)'

                base_query += ' ORDER BY pos_fiscals."dateTime_end" ASC'

                db.cursor.execute(base_query, (start_date, end_date))
                rows = db.cursor.fetchall()

                core.logger.db_service.debug(
                    f"Производится поиск клиентов, которым потребуется замена ФН в интервале от '{start_date}' до '{end_date}'")
                records = []
                for row in rows:
                    record = dict(
                        zip(['serialNumber', 'client', 'RNM', 'fn_serial', 'organizationName', 'INN',
                             'dateTime_end', 'current_time', 'v_time', 'url_rms', 'address'], row))

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

            core.logger.db_service.debug(
                f"Поиск клиентов ({len(records)}), которым потребуется замена ФН в интервале от '{start_date}' до '{end_date}', завершён:")
            core.logger.db_service.debug(f"{records}")
            return records
        except Exception:
            core.logger.db_service.error("Неожиданное исключение при запросе к заканчивающимся ФН", exc_info=True)

    def search_dont_update(self, field, days):
        try:
            with DatabaseContextManager() as db:
                # Получаем названия столбцов из информационной схемы PostgreSQL
                db.cursor.execute('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'pos_fiscals'
                    ORDER BY ordinal_position
                ''')
                columns = [column[0] for column in db.cursor.fetchall()]

                # Строим запрос для поиска устаревших записей
                # Используем синтаксис PostgreSQL для работы с датами
                query = f'''
                    SELECT * FROM pos_fiscals 
                    WHERE "{field}"::timestamp < (CURRENT_TIMESTAMP - INTERVAL '{days} days')
                '''

                # Выполняем запрос
                db.cursor.execute(query)
                search_results = db.cursor.fetchall()

                # Создаем новый список для данных с проверкой на устаревание
                modified_data = []
                for row in search_results:
                    modified_row = list(row)

                    # Определяем индексы важных столбцов
                    licenses_data_index = columns.index('licenses') if 'licenses' in columns else -1
                    current_time_index = columns.index("current_time") if "current_time" in columns else -1
                    v_time_index = columns.index('v_time') if 'v_time' in columns else -1

                    # Обработка licenses
                    if licenses_data_index >= 0 and row[licenses_data_index]:
                        modified_row[licenses_data_index] = row[licenses_data_index]

                    # Проверка устаревания записи
                    time_to_check = None
                    if v_time_index >= 0 and current_time_index >= 0:
                        time_to_check = row[v_time_index] if row[v_time_index] not in (None, '', 'None') else row[
                            current_time_index]

                    is_expired = False
                    if time_to_check:
                        is_expired = not self.if_show_fn_to_date(time_to_check, self.dont_valid_fn)

                    # Добавляем признак устаревания в строку
                    modified_row.append(is_expired)

                    modified_data.append(modified_row)

                return modified_data, columns
        except Exception:
            core.logger.db_service.error("Не удалось сделать поисковый запрос устаревших записей", exc_info=True)

    def toggle_task(self, serial_number, fn_serial, checked, bitrix24):
        try:
            with DatabaseContextManager() as db:
                # Создаем таблицу если её нет
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS fn_sale_task (
                        "serialNumber" TEXT PRIMARY KEY,
                        "fn_serial" TEXT
                    )
                ''')

                if checked:
                    db.cursor.execute(
                        '''INSERT INTO fn_sale_task ("serialNumber", "fn_serial") 
                           VALUES (%s, %s)
                           ON CONFLICT ("serialNumber") DO NOTHING''',
                        (serial_number, fn_serial)
                    )
                    core.logger.db_service.debug(f"Запись '{serial_number}' успешно добавлена в базу созданных задач")
                else:
                    if bitrix24.enabled:
                        return {'status': 'error'}

                    db.cursor.execute(
                        '''DELETE FROM fn_sale_task 
                           WHERE "serialNumber" = %s''',
                        (serial_number,)
                    )
                    core.logger.db_service.debug(f"Запись '{serial_number}' успешно удалена из базы созданных задач")

                return {'status': 'success'}

        except Exception as e:
            core.logger.db_service.error("Ошибка при обновлении таблицы fn_sale_task", exc_info=True)
            return {'status': 'error', 'message': str(e)}

    def save_client_name(self, url_rms, inn, org_name, existing_record=None):
        import core.connectors
        iikorms = core.connectors.IikoRms()

        server_name, version = iikorms.get_rms_name(url_rms)

        with DatabaseContextManager() as db:
            # Если запись существует и manual_edit = 1, сохраняем старое значение serverName
            if existing_record and existing_record[2] == 1:
                server_name = existing_record[0]  # Оставляем старое значение serverName

            # Генерируем уникальный ID только для новой записи
            unique_id = str(uuid.uuid4()) if not existing_record else None

            if existing_record:
                # Обновляем существующую запись, сохраняя manual_edit
                db.cursor.execute('''
                        UPDATE clients 
                        SET "version" = %s,
                            "serverName" = %s,
                            "INN" = %s,
                            "organizationName" = %s,
                            "last_updated" = CURRENT_TIMESTAMP
                        WHERE "url_rms" = %s
                    ''', (version, server_name, inn, org_name, url_rms))
            else:
                # Вставляем новую запись
                db.cursor.execute('''
                        INSERT INTO clients 
                        ("id", "url_rms", "INN", "organizationName", "serverName", "version", "manual_edit") 
                        VALUES (%s, %s, %s, %s, %s, %s, 0)
                    ''', (unique_id, url_rms, inn, org_name, server_name, version))
            db.conn.commit()
            core.logger.db_service.debug(f"Успешно обновлена информация для {url_rms}")

    def add_new_clients(self, url_rms, inn, org_name):
        try:
            with DatabaseContextManager() as db:
                # Создаём таблицу clients, если она не существует
                db.cursor.execute('''
                        CREATE TABLE IF NOT EXISTS clients (
                            "id" TEXT PRIMARY KEY,
                            "url_rms" TEXT,
                            "INN" TEXT,
                            "organizationName" TEXT,
                            "serverName" TEXT,
                            "version" TEXT,
                            "manual_edit" INTEGER DEFAULT 0,
                            "last_updated" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                db.conn.commit()

                # Проверяем, существует ли уже запись с таким url_rms
                db.cursor.execute('SELECT "id" FROM clients WHERE "url_rms" = %s', (url_rms,))
                existing_record = db.cursor.fetchone()

                if not existing_record:
                    for attempt in range(3):
                        try:
                            self.save_client_name(url_rms, inn, org_name)
                            time.sleep(1)
                            break
                        except Exception:
                            if attempt < 2:
                                time.sleep(5)
                                continue
                            core.logger.db_service.error(f"Не удалось получить имя клиента после '3' попыток",
                                                             exc_info=True)
                            # Добавляем запись без serverName и version
                            unique_id = str(uuid.uuid4())
                            db.cursor.execute('''
                                    INSERT INTO clients 
                                    ("id", "url_rms", "INN", "organizationName", "serverName", "version", "manual_edit") 
                                    VALUES (%s, %s, %s, %s, %s, %s, 0)
                                ''', (unique_id, url_rms, inn, org_name, None, None))
                            core.logger.db_service.debug(f"Добавлена запись с 'None' для '{url_rms}'")
        except Exception:
            core.logger.db_service.error(
                f"Произошла ошибка при попытке добавить нового клиента в базу", exc_info=True)

    def edit_client_name(self, url_rms, server_name):
        try:
            with DatabaseContextManager() as db:
                # Создаем таблицу clients если её нет
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS clients (
                        "id" TEXT PRIMARY KEY,
                        "url_rms" TEXT,
                        "INN" TEXT,
                        "organizationName" TEXT,
                        "serverName" TEXT,
                        "version" TEXT,
                        "manual_edit" INTEGER DEFAULT 0,
                        "last_updated" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Проверяем, существует ли запись
                db.cursor.execute(
                    'SELECT "id" FROM clients WHERE "url_rms" = %s',
                    (url_rms,)
                )
                record = db.cursor.fetchone()

                if record:
                    # Обновляем существующую запись
                    db.cursor.execute('''
                        UPDATE clients 
                        SET "serverName" = %s, "manual_edit" = 1, "last_updated" = CURRENT_TIMESTAMP
                        WHERE "url_rms" = %s
                    ''', (server_name, url_rms))
                else:
                    # Создаем новую запись
                    unique_id = str(uuid.uuid4())
                    db.cursor.execute('''
                        INSERT INTO clients 
                        ("id", "url_rms", "serverName", "manual_edit") 
                        VALUES (%s, %s, %s, 1)
                    ''', (unique_id, url_rms, server_name))

                return {'success': True}

        except Exception as e:
            core.logger.db_service.error(
                "Ошибка при обновлении имени клиента", exc_info=True
            )
            return {'success': False, 'error': str(e)}

    def add_api_key(self, name, admin_tag):
        try:
            # Генерация уникального API-ключа
            api_key = str(uuid.uuid4())

            with DatabaseContextManager() as db:
                # Создание таблицы api_keys, если она не существует
                db.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS api_keys (
                        "api_key" TEXT PRIMARY KEY,
                        "name" TEXT,
                        "admin_tag" INTEGER,
                        "active" INTEGER
                    )
                ''')

                # Сохранение API-ключа в базу данных
                db.cursor.execute('''
                    INSERT INTO api_keys ("api_key", "name", "admin_tag", "active") 
                    VALUES (%s, %s, %s, 1)
                ''', (api_key, name, admin_tag))

                core.logger.db_service.info(f"Создан новый API-ключ для '{name}'")

                return {
                    "success": True,
                    "api_key": api_key,
                    "name": name,
                    "admin_tag": admin_tag
                }

        except Exception as e:
            core.logger.db_service.error(f"Не удалось создать API-ключ для '{name}'", exc_info=True)
            return {"success": False, "error": str(e)}

    def get_api_key(self, admin, show_deleted=None, extended=None):
        try:
            with DatabaseContextManager() as db:
                query = '''
                    SELECT "api_key", "name", "admin_tag", "active" 
                    FROM api_keys
                '''

                if not show_deleted:
                    query += ' WHERE "active" = 1'
                # Если admin=1, добавляем условие для фильтрации только админских ключей
                if admin == 1:
                    query += ' AND "admin_tag" = 1'

                db.cursor.execute(query)
                results = db.cursor.fetchall()

                if not extended:
                    # Извлекаем только api_key из результатов
                    api_keys = [row[0] for row in results]
                else:
                    api_keys = []
                    for row in results:
                        api_keys.append({
                            'api_key': row[0],
                            'name': row[1],
                            'admin_tag': row[2],
                            'active': row[3]
                        })

                return api_keys
        except Exception:
            core.logger.db_service.error("Не удалось получить список API-ключей из базы данных", exc_info=True)
            return []

    def remove_api_key(self, active, api_key, name):
        try:
            with DatabaseContextManager() as db:
                db.cursor.execute(
                    'UPDATE api_keys SET "active" = %s WHERE "api_key" = %s',
                    (active, api_key)
                )
                core.logger.db_service.debug(f"Изменено состояние API-ключа '{name}' на 'active: {active}'")
        except Exception:
            core.logger.db_service.error("Не удалось изменить состояние ключа в базе данных", exc_info=True)

    def get_serial_numbers_info(self, info):
        try:
            with DatabaseContextManager() as db:
                if info:
                    # Запрос с JOIN для получения serialNumber, url_rms и serverName
                    db.cursor.execute('''
                        SELECT pos_fiscals."serialNumber", pos_fiscals."url_rms", pos_fiscals."organizationName", 
                        pos_fiscals."INN",clients."serverName", clients."id"
                        FROM pos_fiscals
                        LEFT JOIN clients ON pos_fiscals."url_rms" = clients."url_rms"
                        WHERE pos_fiscals."serialNumber" IS NOT NULL
                    ''')
                    results = db.cursor.fetchall()

                    # Формируем словарь с нужной структурой
                    data = {}
                    for row in results:
                        serial_number, url_rms, organization_name, inn, server_name, client_id = row
                        if serial_number:  # Проверяем, что serial_number не пустой
                            data[serial_number] = {
                                "client_id": client_id or "",
                                "url_rms": url_rms or "",
                                "serverName": server_name or "",
                                "organizationName": organization_name or "",
                                "INN": inn or ""
                            }

                    return data
                else:
                    db.cursor.execute('SELECT "serialNumber" FROM pos_fiscals')
                    results = db.cursor.fetchall()
                    # Преобразуем результаты в плоский список
                    serial_numbers = [row[0] for row in results if row[0]]

                    return serial_numbers
        except Exception:
            core.logger.db_service.error("Не удалось получить расширенную информацию о серийных номерах", exc_info=True)
            return {}

    def get_fiscals_by_serial_numbers(self, serial_numbers):
        try:
            with DatabaseContextManager() as db:
                # Получаем названия столбцов из информационной схемы
                db.cursor.execute('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'pos_fiscals'
                    ORDER BY ordinal_position
                ''')
                columns = [column[0] for column in db.cursor.fetchall()]

                # Создаем параметры для запроса
                placeholders = ','.join(['%s'] * len(serial_numbers))

                # Формируем SQL-запрос
                query = f'''
                    SELECT * 
                    FROM pos_fiscals 
                    WHERE "serialNumber" IN ({placeholders})
                '''

                # Выполняем запрос
                db.cursor.execute(query, tuple(serial_numbers))
                results = db.cursor.fetchall()

                # Преобразуем результаты в список словарей
                fiscals_data = []
                for row in results:
                    fiscal_dict = {}
                    for i, column in enumerate(columns):
                        # Для полей типа JSON или списков преобразуем в строковый формат
                        if isinstance(row[i], (dict, list)):
                            fiscal_dict[column] = json.dumps(row[i], ensure_ascii=False)
                        else:
                            fiscal_dict[column] = row[i]
                    fiscals_data.append(fiscal_dict)

                return fiscals_data

        except Exception:
            core.logger.db_service.error("Не удалось получить данные о ККТ по серийным номерам", exc_info=True)
            return []

    def get_bitrix_contractors(self, table_name, field_name, last_name):
        try:
            with DatabaseContextManager() as db:
                query = f'SELECT "id", "NAME", "{last_name}" FROM "{table_name}" WHERE "{field_name}" = 1'
                db.cursor.execute(query)
                results = db.cursor.fetchall()

                # Извлекаем id из результатов запроса
                id_list = []
                for row in results:
                    id_value = row[0]
                    name = row[1] if len(row) > 1 else ""
                    last_name = row[2] if len(row) > 2 else ""

                    core.logger.db_service.debug(
                        f"Найдена активная запись в таблице '{table_name}':")
                    core.logger.db_service.debug(f"ID: {id_value}, Name: {name} {last_name}")

                    id_list.append(id_value)

                if id_list == []:
                    core.logger.db_service.warning(
                        f"Не найдено активных записей в таблице '{table_name}'")

                return id_list
        except Exception:
            core.logger.db_service.error(f"Ошибка при поиске записей в таблице '{table_name}'", exc_info=True)
            return []

    def select_bitrix_contractors(self, responsible_id, observers_id):
        self.reset_bitrix_contractors()

        try:
            with DatabaseContextManager() as db:
                if not responsible_id == None:
                    # Устанавливаем значение responsible для выбранного сотрудника
                    db.cursor.execute('UPDATE bitrix_employees SET responsible = 1 WHERE id = %s', (responsible_id,))

                if not observers_id == None:
                    # Устанавливаем значение observers для выбранной группы
                    db.cursor.execute('UPDATE bitrix_projects SET observers = 1 WHERE id = %s', (observers_id,))
        except Exception:
            core.logger.db_service.error(
                "Ошибка при обновлении записей в таблицах 'bitrix_employees' и 'bitrix_projects'", exc_info=True)

    def reset_bitrix_contractors(self):
        try:
            with DatabaseContextManager() as db:
                # Сбрасываем все значения responsible
                db.cursor.execute('UPDATE bitrix_employees SET responsible = 0')
                # Сбрасываем все значения observers
                db.cursor.execute('UPDATE bitrix_projects SET observers = 0')
                core.logger.db_service.debug("Списки ответственного сотрудника и группы наблюдателей сброшены")
        except Exception:
            core.logger.db_service.error(
                "Ошибка при сбросе записей в таблицах 'bitrix_employees' и 'bitrix_projects'", exc_info=True)

    def get_list_bitrix_contractors(self):
        try:
            with DatabaseContextManager() as db:
                db.cursor.execute(
                    'SELECT id, "NAME", "LAST_NAME", responsible FROM bitrix_employees ORDER BY  CAST(id AS INTEGER)')
                bitrix_employees = [dict(zip(['id', 'NAME', 'LAST_NAME', 'responsible'], row)) for row in
                                    db.cursor.fetchall()]

                db.cursor.execute('SELECT id, "NAME", observers FROM bitrix_projects ORDER BY  CAST(id AS INTEGER)')
                bitrix_projects = [dict(zip(['id', 'NAME', 'observers'], row)) for row in db.cursor.fetchall()]

                return bitrix_employees, bitrix_projects

        except Exception:
            core.logger.db_service.error(
                "Ошибка при получении списка сотрудников и проектов из базы данных", exc_info=True)


class DbUpdate(DbQueries):
    def __init__(self):
        super().__init__()
        try: self.dbupdate_period = int(self.config.get("db-update", "dbupdate-period-sec", fallback=900))
        except: self.dbupdate_period = 900

        try: self.reference_flaq = int(self.config.get("db-update", "reference", fallback=0))
        except: self.reference_flaq = 0

        try: self.ftp_update = int(self.config.get("ftp-connect", "ftp_update", fallback=0))
        except: self.ftp_update = 0

        self.clients_update_process = 0

    def pos_tables_update(self):
        import core.connectors
        
        if self.reference_flaq == True:
            while True:
                if self.ftp_update == True:
                    try:
                        core.logger.db_service.info("Начато обновление базы ККТ")

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
                    except Exception:
                        core.logger.db_service.error(f"Не удалось загрузить файл c FTP", exc_info=True)
                        core.logger.db_service.info(
                            f"Следующая попытка обновления будет произведена через ({self.dbupdate_period}) секунд")
                        time.sleep(self.dbupdate_period)
                        continue

                self.clean_fn_sale_task()

                if self.clients_update_process == 0:
                    update_clients_info_thread = threading.Thread(target=self.update_clients_info_on_schedule, daemon=True)
                    update_clients_info_thread.start()
                    self.clients_update_process = 1

                core.logger.db_service.info("Обновление базы завершено")
                core.logger.db_service.info(
                    f"Следующее обновление будет произведено через ({self.dbupdate_period}) секунд")
                time.sleep(self.dbupdate_period)
            
    def update_clients_info_on_schedule(self):
        while True:
            self.clean_obsolete_clients()

            time.sleep(86400)

            core.logger.db_service.info(f"Начато обновление базы клиентов")
            try:
                with DatabaseContextManager() as db:
                    # Получаем все url_rms, INN и organizationName из pos_fiscals
                    db.cursor.execute('''
                            SELECT DISTINCT "url_rms", "INN", "organizationName" 
                            FROM pos_fiscals 
                            WHERE "url_rms" IS NOT NULL AND "url_rms" != ''
                        ''')
                    records = db.cursor.fetchall()
                    core.logger.db_service.debug(f"Найдено клиентов: {len(records)}")

                    # Обрабатываем каждую запись
                    for record in records:
                        url_rms, inn, org_name = record

                        # Проверяем существующую запись и значение manual_edit
                        db.cursor.execute(
                            'SELECT "serverName", "version", "manual_edit" FROM clients WHERE "url_rms" = %s',
                            (url_rms,))
                        existing_record = db.cursor.fetchone()

                        try:
                            DbQueries().save_client_name(url_rms, inn, org_name, existing_record)
                            time.sleep(1.5)
                        except Exception:
                            time.sleep(1.5)
                            continue
                core.logger.db_service.info(
                    f"Обновление базы клиентов завершено, следующее обновление через '24' часа")
                time.sleep(60)
            except Exception:
                core.logger.db_service.error(
                    "Ошибка при обновлении информации о клиентах, следующая попытка через '24' часа", exc_info=True)
                time.sleep(60)

    def update_bitrix_employees_table(self, employees):
        try:
            with DatabaseContextManager() as db:
                # Создаём таблицу bitrix_employees, если она не существует
                db.cursor.execute('''CREATE TABLE IF NOT EXISTS bitrix_employees (
                                    "id" TEXT PRIMARY KEY,
                                    "NAME" TEXT,
                                    "LAST_NAME" TEXT,
                                    "UF_DEPARTMENT" TEXT,
                                    "responsible" INTEGER DEFAULT 0
                                )''')

                if employees:
                    # Получаем все id, которые были до обновления
                    db.cursor.execute('SELECT "id", "NAME", "LAST_NAME" FROM bitrix_employees')
                    old_employees = db.cursor.fetchall()
                    old_ids = set(row[0] for row in old_employees)

                    # Собираем все актуальные ID сотрудников
                    employee_ids = []
                    for employee in employees:
                        employee_id = employee.get('ID')
                        employee_ids.append(employee_id)
                        name = employee.get('NAME', '')
                        last_name = employee.get('LAST_NAME', '')

                        # UF_DEPARTMENT может быть списком, преобразуем его в строку JSON
                        uf_department = employee.get('UF_DEPARTMENT')
                        if isinstance(uf_department, list):
                            uf_department = json.dumps(uf_department, ensure_ascii=False)
                        else:
                            uf_department = json.dumps([uf_department] if uf_department else [], ensure_ascii=False)

                        # Вставляем или обновляем запись в таблице
                        db.cursor.execute('''
                            INSERT INTO bitrix_employees ("id", "NAME", "LAST_NAME", "UF_DEPARTMENT", "responsible")
                            VALUES (%s, %s, %s, %s, 0)
                            ON CONFLICT ("id") 
                            DO UPDATE SET 
                                "NAME" = EXCLUDED."NAME", 
                                "LAST_NAME" = EXCLUDED."LAST_NAME", 
                                "UF_DEPARTMENT" = EXCLUDED."UF_DEPARTMENT"
                        ''', (employee_id, name, last_name, uf_department))

                    # Определяем id для удаления
                    to_delete_ids = old_ids - set(employee_ids)
                    if to_delete_ids:
                        deleted_employees = [row for row in old_employees if row[0] in to_delete_ids]
                        for emp in deleted_employees:
                            core.logger.bitrix24.debug(
                                f"Будет удалён сотрудник: id={emp[0]}, NAME={emp[1]}, LAST_NAME={emp[2]}"
                            )

                        # Удаляем из таблицы
                        placeholders = ','.join(['%s'] * len(to_delete_ids))
                        db.cursor.execute(
                            f'DELETE FROM bitrix_employees WHERE "id" IN ({placeholders})',
                            tuple(to_delete_ids)
                        )

                    core.logger.bitrix24.info(
                        f"Таблица 'bitrix_employees' обновлена, добавлено '{len(employees)}' сотрудников")
                else:
                    core.logger.bitrix24.warning("Не удалось получить данные о сотрудниках из Bitrix24")
                    return False

                return True
        except Exception:
            core.logger.bitrix24.error("Не удалось обновить таблицу 'bitrix_employees'", exc_info=True)
            return False

    def update_bitrix_projects_table(self, projects):
        try:
            with DatabaseContextManager() as db:
                # Создаём таблицу bitrix_employees, если она не существует
                db.cursor.execute('''CREATE TABLE IF NOT EXISTS bitrix_projects (
                                    "id" TEXT PRIMARY KEY,
                                    "NAME" TEXT,
                                    "SUBJECT_NAME" TEXT,
                                    "observers" INTEGER DEFAULT 0
                                )''')

                if projects:
                    # Получаем все id, которые были до обновления
                    db.cursor.execute('SELECT "id", "NAME", "SUBJECT_NAME" FROM bitrix_projects')
                    old_projects = db.cursor.fetchall()
                    old_ids = set(row[0] for row in old_projects)

                    project_ids = []
                    # Обрабатываем каждого сотрудника
                    for project in projects:
                        project_id = project.get('ID')
                        project_ids.append(project_id)
                        name = project.get('NAME', '')
                        subject_name = project.get('SUBJECT_NAME', '')

                        # Вставляем или обновляем запись в таблице
                        db.cursor.execute('''
                            INSERT INTO bitrix_projects ("id", "NAME", "SUBJECT_NAME", "observers")
                            VALUES (%s, %s, %s, 0)
                            ON CONFLICT ("id") 
                            DO UPDATE SET 
                                "NAME" = EXCLUDED."NAME", 
                                "SUBJECT_NAME" = EXCLUDED."SUBJECT_NAME"
                        ''', (project_id, name, subject_name))

                    # Определяем id для удаления
                    to_delete_ids = old_ids - set(project_ids)
                    if to_delete_ids:
                        deleted_projects = [row for row in old_projects if row[0] in to_delete_ids]
                        for emp in deleted_projects:
                            core.logger.bitrix24.debug(
                                f"Будет удалёна группа наблюдателей: id={emp[0]}, NAME={emp[1]}, LAST_NAME={emp[2]}"
                            )

                        # Удаляем из таблицы
                        placeholders = ','.join(['%s'] * len(to_delete_ids))
                        db.cursor.execute(
                            f'DELETE FROM bitrix_projects WHERE "id" IN ({placeholders})',
                            tuple(to_delete_ids)
                        )

                    core.logger.bitrix24.info(
                        f"Таблица 'bitrix_projects' обновлена, добавлено '{len(projects)}' проектов")
                else:
                    core.logger.bitrix24.warning("Не удалось получить данные о проектах из Bitrix24")
                    return False

                return True
        except Exception:
            core.logger.bitrix24.error("Не удалось обновить таблицу 'bitrix_projects'", exc_info=True)
            return False
