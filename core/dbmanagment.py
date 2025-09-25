import core.sys_manager
import core.logger
import core.configs
import core.connectors
import os
import json
import time
import threading
import uuid

iikorms = core.connectors.IikoRms()

class DbUpdate(core.sys_manager.DatabaseContextManager):
    def __init__(self):
        super().__init__()
        self.dbupdate_period = int(self.config.get("db-update", "dbupdate-period-sec", fallback=None))
        self.reference_flaq = int(self.config.get("db-update", "reference", fallback=None))
        self.clients_update_process = 0

    def pos_tables_update(self):
        if self.reference_flaq == True:
            while True:
                core.logger.db_service.info("Начато обновление базы ККТ")

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

                    if self.clients_update_process == 0:
                        update_clients_info_thread = threading.Thread(target=iikorms.update_clients_info, daemon=True)
                        update_clients_info_thread.start()
                        self.clients_update_process = 1

                    core.logger.db_service.info("Обновление базы ФР завершено")
                    core.logger.db_service.info(
                        f"Следующее обновление будет произведено через ({self.dbupdate_period}) секунд")

                    time.sleep(self.dbupdate_period)

                except Exception:
                    core.logger.db_service.error(f"Не удалось загрузить файл c FTP", exc_info=True)
                    core.logger.db_service.info(
                        f"Следующая попытка обновления будет произведена через ({self.dbupdate_period}) секунд")
                    time.sleep(self.dbupdate_period)
                    continue

    def save_not_fiscal(self, json_data, filename):
        try:
            with core.sys_manager.DatabaseContextManager() as db:
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
            with core.sys_manager.DatabaseContextManager() as db:
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
                    core.logger.db_service.debug(f"Запись '{filename}' успешно добавлена в базу")
                except Exception:
                    core.logger.db_service.error(f"Файл уже был удалён", exc_info=True)
                    pass
        except Exception:
            core.logger.db_service.error(
                f"Попытка сохранить данные '{data}' в базу данных завершилась неудачей", exc_info=True)

    def clean_fn_sale_task(self):
        try:
            with core.sys_manager.DatabaseContextManager() as db:
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

    def update_bitrix_employees_table(self, employees):
        try:
            with core.sys_manager.DatabaseContextManager() as db:
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
            with core.sys_manager.DatabaseContextManager() as db:
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

class DbQueries(core.sys_manager.DatabaseContextManager):
    def __init__(self):
        super().__init__()
        self.dont_valid_fn = int(self.config.get("db-update", "day_filter_expire", fallback=5))

    def get_data_pos_fiscals(self):
        try:
            with core.sys_manager.DatabaseContextManager() as db:
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

    def only_pos(self):
        try:
            with core.sys_manager.DatabaseContextManager() as db:
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
            with core.sys_manager.DatabaseContextManager() as db:
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
            with core.sys_manager.DatabaseContextManager() as db:
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

            with core.sys_manager.DatabaseContextManager() as db:
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
                           pos_fiscals."url_rms"
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

            core.logger.db_service.debug(
                f"Поиск клиентов ({len(records)}), которым потребуется замена ФН в интервале от '{start_date}' до '{end_date}', завершён:")
            core.logger.db_service.debug(f"{records}")
            return records
        except Exception:
            core.logger.db_service.error("Неожиданное исключение при запросе к заканчивающимся ФН", exc_info=True)

    def search_dont_update(self, field, days):
        try:
            with core.sys_manager.DatabaseContextManager() as db:
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
            with core.sys_manager.DatabaseContextManager() as db:
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

    def edit_client_name(self, url_rms, server_name):
        try:
            with core.sys_manager.DatabaseContextManager() as db:
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

    def get_bitrix_contractors(self, table_name, field_name, last_name):
        try:
            with core.sys_manager.DatabaseContextManager() as db:
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
        try:
            with core.sys_manager.DatabaseContextManager() as db:
                # Сбрасываем все значения responsible
                db.cursor.execute('UPDATE bitrix_employees SET responsible = 0')
                # Сбрасываем все значения observers
                db.cursor.execute('UPDATE bitrix_projects SET observers = 0')

                if not responsible_id == None:
                    # Устанавливаем значение responsible для выбранного сотрудника
                    db.cursor.execute('UPDATE bitrix_employees SET responsible = 1 WHERE id = %s', (responsible_id,))

                if not observers_id == None:
                    # Устанавливаем значение observers для выбранной группы
                    db.cursor.execute('UPDATE bitrix_projects SET observers = 1 WHERE id = %s', (observers_id,))
        except Exception:
            core.logger.db_service.error(
                "Ошибка при обновлении записей в таблицах 'bitrix_employees' и 'bitrix_projects'", exc_info=True)

    def get_list_bitrix_contractors(self):
        try:
            with core.sys_manager.DatabaseContextManager() as db:
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