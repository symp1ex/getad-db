import core.logger
import about
import configparser


def create_confgi_ini():
    try:
        # Создание объекта парсера
        config = configparser.ConfigParser()

        # Создание секций
        config['global'] = {}
        config['webserver'] = {}
        config['db-update'] = {}
        config['ftp-connect'] = {}

        # Запись значения в секцию и ключ
        config['global']['log-level'] = 'info'
        config['global']['logs-autoclear-days'] = '14'
        config['webserver']['port'] = '30005'
        config['webserver']['user'] = 'user'
        config['webserver']['pass'] = '1234'
        config['webserver']['admin'] = 'admin'
        config['webserver']['admin_pass'] = '4321'
        config['db-update']['reference'] = '0'
        config['db-update']['db-name'] = 'getad'
        config['db-update']['host'] = ''
        config['db-update']['port'] = ''
        config['db-update']['user'] = ''
        config['db-update']['password'] = ''
        config['db-update']['dbupdate-period-sec'] = '900'
        config['db-update']['day_filter_expire'] = '5'
        config['ftp-connect']['ftpHost'] = ''
        config['ftp-connect']['ftpUser'] = ''
        config['ftp-connect']['ftpPass'] = ''

        # Запись изменений в файл
        with open(about.config_path, 'w') as configfile:
            config.write(configfile)

        core.logger.web_server.info("Создан 'config.ini' по умолчанию")
    except Exception:
        core.logger.web_server.error("При создании файла конфигурации произошло исключение", exc_info=True)

def read_config_ini(ini_file):
    try:
        config = configparser.ConfigParser()
        config.read(ini_file)
        return config
    except FileNotFoundError:
        return None
    except Exception:
        pass