import core.logger
import core.sys_manager
import requests
import uuid
import time
import ftplib

class FtpContextManager(core.sys_manager.ResourceManagement):
    def __init__(self):
        super().__init__()
        try:
            self.server = self.config.get("ftp-connect", "ftpHost", fallback=None)
            self.username = self.config.get("ftp-connect", "ftpUser", fallback=None)
            self.password = self.config.get("ftp-connect", "ftpPass", fallback=None)
        except Exception:
            core.logger.db_service.error(
                "Не удалось параметры подключения к FTP-серверу, проверьте настройки", exc_info=True)

        self.ftp = None

    def __enter__(self):
        try:
            self.ftp = ftplib.FTP(self.server)
            self.ftp.login(self.username, self.password)
            return self.ftp
        except Exception:
            core.logger.db_service.error(
                "Не удалось подключиться к FTP-серверу, проверьте параметры подключения", exc_info=True)

    def __exit__(self, exc_type, exc_value, traceback):
        if self.ftp:
            try: self.ftp.quit()
            except: pass

class IikoRms(core.sys_manager.DatabaseContextManager):
    def __init__(self):
        super().__init__()

    def update_clients_info(self):
        while True:
            core.logger.clients_update.info(f"Начато обновление базы клиентов")
            try:
                with core.sys_manager.DatabaseContextManager() as db:
                    # Создаем таблицу clients, если она не существует
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

                    # Получаем все url_rms, INN и organizationName из pos_fiscals
                    db.cursor.execute('''
                            SELECT DISTINCT "url_rms", "INN", "organizationName" 
                            FROM pos_fiscals 
                            WHERE "url_rms" IS NOT NULL AND "url_rms" != ''
                        ''')
                    records = db.cursor.fetchall()

                    # Обрабатываем каждую запись
                    for record in records:
                        url_rms, inn, org_name = record

                        # Проверяем существующую запись и значение manual_edit
                        db.cursor.execute(
                            'SELECT "serverName", "version", "manual_edit" FROM clients WHERE "url_rms" = %s',
                            (url_rms,))
                        existing_record = db.cursor.fetchone()

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
                                core.logger.clients_update.debug(f"Успешно обновлена информация для {url_rms}")
                            time.sleep(1.5)

                        except requests.RequestException as e:
                            core.logger.clients_update.error(f"Ошибка при запросе к {url_rms}: {str(e)}")

                            # Если записи нет - добавляем новую с None
                            if not existing_record:
                                unique_id = str(uuid.uuid4())
                                db.cursor.execute('''
                                        INSERT INTO clients 
                                        ("id", "url_rms", "INN", "organizationName", "serverName", "version", "manual_edit") 
                                        VALUES (%s, %s, %s, %s, %s, %s, 0)
                                    ''', (unique_id, url_rms, inn, org_name, None, None))
                                db.conn.commit()
                                core.logger.clients_update.debug(f"Добавлена запись с None для {url_rms}")
                            time.sleep(1.5)
                            continue
                core.logger.clients_update.info(
                    f"Обновление базы клиентов завершено, следующее обновление через '12' часов")
                time.sleep(43200)
            except Exception:
                core.logger.db_service.error(
                    "Ошибка при обновлении информации о клиентах, следующая попытка через '5' минут", exc_info=True)
                time.sleep(300)
