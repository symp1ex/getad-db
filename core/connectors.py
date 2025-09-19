import sqlite3
import requests
import uuid
import time
import core.logger
from core.configs import read_config_ini
import about

class IikoRms:
    def __init__(self):
        self.config = read_config_ini(about.config_path)
        self.dbname = self.config.get("db-update", "db-name", fallback=None)
        self.format_db_path = about.db_path.format(dbname=self.dbname)

    def update_clients_info(self):
            try:
                # Подключаемся к БД
                conn = sqlite3.connect(self.format_db_path)
                cursor = conn.cursor()

                # Создаем таблицу clients, если она не существует
                cursor.execute('''
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

                # Получаем все url_rms, INN и organizationName из pos_fiscals
                cursor.execute('''
                    SELECT DISTINCT url_rms, INN, organizationName 
                    FROM pos_fiscals 
                    WHERE url_rms IS NOT NULL AND url_rms != ""
                ''')
                records = cursor.fetchall()

                # Обрабатываем каждую запись
                for record in records:
                    url_rms, inn, org_name = record

                    # Проверяем существующую запись и значение manual_edit
                    cursor.execute('SELECT serverName, version, manual_edit FROM clients WHERE url_rms = ?', (url_rms,))
                    existing_record = cursor.fetchone()

                    try:
                        # Формируем URL для запроса
                        monitoring_url = f"{url_rms.rstrip('/')}/getServerMonitoringInfo.jsp"

                        # Делаем запрос
                        response = requests.get(monitoring_url, timeout=20)

                        if response.status_code == 200:
                            json_data = response.json()
                            server_name = json_data.get('serverName', '')
                            version = json_data.get('version', '')

                            # Если запись существует и manual_edit = 1, сохраняем старое значение serverName
                            if existing_record and existing_record[2] == 1:
                                server_name = existing_record[0]  # Оставляем старое значение serverName

                            # Генерируем уникальный ID только для новой записи
                            unique_id = str(uuid.uuid4()) if not existing_record else None

                            if existing_record:
                                # Обновляем существующую запись, сохраняя manual_edit
                                cursor.execute('''
                                    UPDATE clients 
                                    SET version = ?,
                                        serverName = ?,
                                        INN = ?,
                                        organizationName = ?,
                                        last_updated = CURRENT_TIMESTAMP
                                    WHERE url_rms = ?
                                ''', (version, server_name, inn, org_name, url_rms))
                            else:
                                # Вставляем новую запись
                                cursor.execute('''
                                    INSERT INTO clients 
                                    (id, url_rms, INN, organizationName, serverName, version, manual_edit) 
                                    VALUES (?, ?, ?, ?, ?, ?, 0)
                                ''', (unique_id, url_rms, inn, org_name, server_name, version))

                            conn.commit()
                            core.logger.clients_update.info(f"Успешно обновлена информация для {url_rms}")
                        time.sleep(1.5)

                    except requests.RequestException as e:
                        core.logger.clients_update.error(f"Ошибка при запросе к {url_rms}: {str(e)}")

                        # Если записи нет или она содержит None - добавляем новую с None
                        if not existing_record:
                            unique_id = str(uuid.uuid4())
                            cursor.execute('''
                                INSERT OR REPLACE INTO clients 
                                (id, url_rms, INN, organizationName, serverName, version, manual_edit) 
                                VALUES (?, ?, ?, ?, ?, ?, 0)
                            ''', (unique_id, url_rms, inn, org_name, None, None))
                            conn.commit()
                            core.logger.clients_update.info(f"Добавлена запись с None для {url_rms}")
                        time.sleep(1.5)
                        continue

                conn.close()
            except Exception as e:
                core.logger.clients_update.error(f"Произошла ошибка при обновлении clients: {str(e)}", exc_info=True)
