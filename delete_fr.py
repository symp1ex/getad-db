import core.logger
import core.configs
import about
from ftplib import FTP
import sqlite3
from datetime import datetime
import threading

config = core.configs.read_config_ini(about.config_path)

def messages_append(messages, message):
    try:
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S.%f")[:-3]+"]"
        messages.append(f"{timestamp} {message}")
    except Exception:
        core.logger.web_server.error(
            "Возникло исключение при попытки добавить метку времени к сообщению в модальном окне", exc_info=True)


def ftp_con():
    try:
        config = core.configs.read_config_ini(about.config_path)
        FTP_HOST = config.get("ftp-connect", "ftpHost", fallback=None)
        FTP_USER = config.get("ftp-connect", "ftpUser", fallback=None)
        FTP_PASS = config.get("ftp-connect", "ftpPass", fallback=None)

        # Подключение к FTP и чтение файлов JSON
        ftp = FTP(FTP_HOST)
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        return ftp
    except Exception:
        core.logger.web_server.error(
            "Не удалось подключиться к FTP-серверу, проверьте параметры подключения", exc_info=True)

def ftp_delete_json(json_name):
    messages = []
    try:
        # Подключаемся к FTP-серверу
        ftp = ftp_con()  # Предполагается, что ftp_con возвращает объект FTP
        if ftp is None:
            core.logger.web_server.warning(f"Не удалось подключиться к FTP-серверу")
            messages_append(messages, "Error: Не удалось подключиться к FTP-серверу")
            return messages

        # Проверяем наличие файла на сервере
        files = ftp.nlst()  # Получаем список файлов в текущей директории
        if f"{json_name}.json" in files:
            # Удаляем файл
            ftp.delete(f"{json_name}.json")
            core.logger.web_server.info(f"Файл '{json_name}.json' успешно удален с FTP-сервера")
            messages_append(messages, f"Файл '{json_name}.json' успешно удален с FTP-сервера")
        else:
            core.logger.web_server.info(f"Файл '{json_name}.json' не найден на FTP-сервере")
            messages_append(messages, f"Файл '{json_name}.json' не найден на FTP-сервере")
    except Exception:
        core.logger.web_server.error(f"Не удалось удалить файл '{json_name}.json' с FTP-сервера", exc_info=True)
    return messages

def ftp_delete_db():
    messages = []
    ftp_dbname = "fiscals.db"
    try:
        # Подключаемся к FTP-серверу
        ftp = ftp_con()  # Предполагается, что ftp_con возвращает объект FTP
        if ftp is None:
            core.logger.web_server.warning(f"Не удалось подключиться к FTP-серверу")
            messages_append(messages, "Не удалось подключиться к FTP-серверу")
            return messages

        # Проверяем наличие файла на сервере
        files = ftp.nlst()  # Получаем список файлов в текущей директории
        if ftp_dbname in files:
            # Удаляем файл
            ftp.delete(ftp_dbname)
            core.logger.web_server.info(f"Файл '{ftp_dbname}' успешно удален с FTP-сервера.")
            messages_append(messages, f"Файл '{ftp_dbname}' успешно удален с FTP-сервера")
        else:
            core.logger.web_server.info(f"Файл '{ftp_dbname}' не найден на FTP-сервере")
            messages_append(messages, f"Файл '{ftp_dbname}' не найден на FTP-сервере")
    except Exception:
        core.logger.web_server.error(f"Не удалось удалить файл '{ftp_dbname}' с FTP-сервера", exc_info=True)
    return messages

def delete_record_by_serial_number(json_name):
    messages = []
    try:
        dbname = config.get("db-update", "db-name", fallback=None)
        format_db_path = about.db_path.format(dbname=dbname)

        # Подключение к базе данных
        connection = sqlite3.connect(format_db_path)
        cursor = connection.cursor()

        # Выполнение запроса на удаление
        cursor.execute("DELETE FROM pos_fiscals WHERE serialNumber = ?", (json_name,))

        # Проверка количества затронутых строк
        if cursor.rowcount > 0:
            core.logger.web_server.info(f"Запись с 'serialNumber = {json_name}' в '{dbname}.db' успешно удалена")
            messages_append(messages, f"Запись с 'serialNumber = {json_name}' в {dbname}.db успешно удалена")
        else:
            core.logger.web_server.info(f"Запись с 'serialNumber = {json_name}' в '{dbname}.db' не найдена")
            messages_append(messages, f"Запись с 'serialNumber = {json_name}' в '{dbname}.db' не найдена")

        # Сохранение изменений
        connection.commit()

    except Exception:
        core.logger.web_server.error(
            f"Не удалось удалить запись c serialNumber = {json_name} из базы данных", exc_info=True)

    finally:
        if 'connection' in locals():
            connection.close()  # Закрываем соединение с базой данных
    return messages


def delete_fr(json_name):
    messages = []
    try:
        # Создаем новый поток и передаем в него функцию get_db_data()
        ftp_delete_json_thread = threading.Thread(target=messages.extend(ftp_delete_json(json_name)))
        # Запускаем поток
        ftp_delete_json_thread.start()

        ftp_delete_db_thread = threading.Thread(target=messages.extend(ftp_delete_db()))
        # Запускаем поток
        ftp_delete_db_thread.start()

        delete_record_by_serial_number_theard = threading.Thread(target=messages.extend(delete_record_by_serial_number(json_name)))
        # Запускаем поток
        delete_record_by_serial_number_theard.start()

    except Exception:
        core.logger.web_server.error("Не удалось удалить всю информацию об ФР с FTP-сервера", exc_info=True)
    finally:
        if 'ftp' in locals():
            ftp.quit()  # Закрываем соединение с FTP-сервером

    return messages


