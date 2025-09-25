import time
from core.configs import read_config_ini
import core.logger
import os
import json
import calendar
from datetime import date, datetime, timedelta
import psycopg2
import psycopg2.extras

class ResourceManagement:
    config_path = 'source/config.ini'

    def __init__(self):
        self.config = read_config_ini(self.config_path)

    def write_json_file(self, config, json_path, file_name):
        file_path = os.path.join(json_path, file_name)
        try:
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(config, file, ensure_ascii=False, indent=4)
            core.logger.web_server.info(f"Данные записаны в '{file_path}'")
            core.logger.web_server.debug(f"{config}")
            core.logger.web_server.debug(config)
        except Exception:
            core.logger.web_server.error(f"Не удалось записать данные в '{file_path}'.", exc_info=True)

    def read_json_file(self, folder_name, file_name, data=None, create=False):
        json_file = os.path.join(folder_name, file_name)
        try:
            with open(json_file, "r", encoding="utf-8") as file:
                config = json.load(file)
                return config
        except FileNotFoundError:
            core.logger.web_server.warn(f"Файл конфига '{json_file}' отсутствует.")
            if create:
                self.write_json_file(data, folder_name, file_name)
                return False
        except json.JSONDecodeError:
            core.logger.web_server.warn(f"Файл конфига '{json_file}' имеет некорректный формат данных")
            if create:
                self.write_json_file(data, folder_name, file_name)
                return False

    def get_default_dates(self):
        try:
            today = date.today()
            next_month = today.replace(day=1)
            if today.month == 12:
                next_month = next_month.replace(year=today.year + 1, month=1)
            else:
                next_month = next_month.replace(month=today.month + 1)
            last_day = next_month.replace(day=calendar.monthrange(next_month.year, next_month.month)[1])
            return today.strftime('%Y-%m-%d'), last_day.strftime('%Y-%m-%d')
        except Exception:
            core.logger.web_server.error("Error: не установить дефолтный диапозон дат", exc_info=True)

    def if_show_fn_to_date(self, date_string, dont_valid_fn):
        try:
            # Преобразуем строку в объект datetime
            input_date = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")

            # Добавляем 5 дней
            input_date_plus = input_date + timedelta(days=dont_valid_fn)

            # Получаем текущую дату
            current_date = datetime.now()

            # Сравниваем и выводим результат
            result = input_date_plus >= current_date
            return result
        except Exception:
            core.logger.web_server.error(f"Не удалось вычислить разницу между текущей датой и {date_string}",
                                         exc_info=True)

class DatabaseContextManager(ResourceManagement):
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

