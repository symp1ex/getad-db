from core.configs import read_config_ini
import core.logger
import os
import json
import calendar
from datetime import date, datetime, timedelta

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
            core.logger.web_server.error("Не установить дефолтный диапазон дат", exc_info=True)

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
