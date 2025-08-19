import os
import sys
import traceback
from datetime import datetime, timedelta
import configparser

version = '0.6.3.4'
config_path = 'source/config.ini'
db_path = 'source/{dbname}.db'
log_folder = 'source/logs'

def create_confgi_ini():
    try:
        # Создание объекта парсера
        config = configparser.ConfigParser()

        # Создание секций
        config['global'] = {}
        config['webserver'] = {}
        config['ftp-connect'] = {}
        config['db-update'] = {}

        # Запись значения в секцию и ключ
        config['global']['logs-autoclear-days'] = '14'
        config['webserver']['port'] = '30005'
        config['webserver']['user'] = 'user'
        config['webserver']['pass'] = '1234'
        config['webserver']['admin'] = 'admin'
        config['webserver']['admin_pass'] = '4321'
        config['ftp-connect']['ftpHost'] = ''
        config['ftp-connect']['ftpUser'] = ''
        config['ftp-connect']['ftpPass'] = ''
        config['db-update']['db-name'] = 'dbpos'
        config['db-update']['dbupdate-period-sec'] = '900'
        config['db-update']['day_filter_expire'] = '5'

        # Запись изменений в файл
        with open(config_path, 'w') as configfile:
            config.write(configfile)

        log_console_out("Создан 'config.ini' по умолчанию", "webs")
    except Exception as e:
        log_console_out("Error: при создании файла конфигурации произошло исключение", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

def read_config_ini(ini_file):
    try:
        config = configparser.ConfigParser()
        config.read(ini_file)
        return config
    except FileNotFoundError:
        return None
    except Exception as e:
        log_console_out("Error: при чтении файла конфигурации произошло исключение", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

def log_with_timestamp(message, name):
    try:
        ini_file = config_path
        config = read_config_ini(ini_file)

        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        # Получаем текущую дату
        current_date = datetime.now()

        days = int(config.get("global", "logs-autoclear-days", fallback=None))
        # Определяем дату, старше которой логи будут удаляться
        old_date_limit = current_date - timedelta(days=days)

        # Удаляем логи старше 14 дней
        for file_name in os.listdir(log_folder):
            file_path = os.path.join(log_folder, file_name)
            file_creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if file_creation_time < old_date_limit:
                os.remove(file_path)

        timestamp = datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(log_folder, f"{timestamp}-{name}.log")
        default_stdout = sys.stdout
        sys.stdout = open(log_file, 'a')

        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S.%f")[:-3]+"]"
        print(f"{timestamp} {message}")
        sys.stdout.close()
        sys.stdout = default_stdout
    except:
        pass


def log_console_out(message, name):
    try:
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S.%f")[:-3]+"]"
        print(f"{timestamp} {message}")
        log_with_timestamp(message, name)
    except:
        pass
    
    
def exception_handler(exc_type, exc_value, exc_traceback, name):
    try:
        error_message = f"ERROR: An exception occurred + \n"
        error_message += ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        log_with_timestamp(error_message, name)
        # Вызываем стандартный обработчик исключений для вывода на экран
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
    except:
        pass

   
# пример использования   
# try:
#     print("Hello!")
# except Exception as e:
#     exception_handler(type(e), e, e.__traceback__)
