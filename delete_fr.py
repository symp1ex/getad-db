from ftplib import FTP
import sqlite3
from datetime import datetime
import threading
from logger import log_console_out, exception_handler, read_config_ini

config = read_config_ini("source/config.ini")

def messages_append(messages, message):
    try:
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S.%f")[:-3]+"]"
        messages.append(f"{timestamp} {message}")
    except Exception as e:
        log_console_out("Error: Возникло исключение при попытки добавить метку времени к сообщению в модальном окне", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")


def ftp_con():
    try:
        config = read_config_ini("source/config.ini")
        FTP_HOST = config.get("ftp-connect", "ftpHost", fallback=None)
        FTP_USER = config.get("ftp-connect", "ftpUser", fallback=None)
        FTP_PASS = config.get("ftp-connect", "ftpPass", fallback=None)

        # Подключение к FTP и чтение файлов JSON
        ftp = FTP(FTP_HOST)
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        return ftp
    except Exception as e:
        log_console_out("Error: Не удалось подключиться к FTP-серверу, проверьте параметры подключения", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

def ftp_delete_json(json_name):
    messages = []
    try:
        # Подключаемся к FTP-серверу
        ftp = ftp_con()  # Предполагается, что ftp_con возвращает объект FTP
        if ftp is None:
            log_console_out(f"Error: Не удалось подключиться к FTP-серверу.", "webs")
            messages_append(messages, "Error: Не удалось подключиться к FTP-серверу.")
            return messages

        # Проверяем наличие файла на сервере
        files = ftp.nlst()  # Получаем список файлов в текущей директории
        if f"{json_name}.json" in files:
            # Удаляем файл
            ftp.delete(f"{json_name}.json")
            log_console_out(f"Файл '{json_name}.json' успешно удален с FTP-сервера.", "webs")
            messages_append(messages, f"Файл '{json_name}.json' успешно удален с FTP-сервера.")
        else:
            log_console_out(f"Файл '{json_name}.json' не найден на FTP-сервере.", "webs")
            messages_append(messages, f"Файл '{json_name}.json' не найден на FTP-сервере.")
    except Exception as e:
        log_console_out(f"Error: Не удалось удалить файл '{json_name}.json' с FTP-сервера", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")
    return messages

def ftp_delete_db():
    messages = []
    ftp_dbname = "fiscals.db"
    try:
        # Подключаемся к FTP-серверу
        ftp = ftp_con()  # Предполагается, что ftp_con возвращает объект FTP
        if ftp is None:
            log_console_out(f"Error: Не удалось подключиться к FTP-серверу.", "webs")
            messages_append(messages, "Error: Не удалось подключиться к FTP-серверу.")
            return messages

        # Проверяем наличие файла на сервере
        files = ftp.nlst()  # Получаем список файлов в текущей директории
        if ftp_dbname in files:
            # Удаляем файл
            ftp.delete(ftp_dbname)
            log_console_out(f"Файл '{ftp_dbname}' успешно удален с FTP-сервера.", "webs")
            messages_append(messages, f"Файл '{ftp_dbname}' успешно удален с FTP-сервера.")
        else:
            log_console_out(f"Файл '{ftp_dbname}' не найден на FTP-сервере.", "webs")
            messages_append(messages, f"Файл '{ftp_dbname}' не найден на FTP-сервере.")
    except Exception as e:
        log_console_out(f"Error: Не удалось удалить файл '{ftp_dbname}' с FTP-сервера", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")
    return messages

def delete_record_by_serial_number(json_name):
    messages = []
    try:
        dbname = config.get("db-update", "db-name", fallback=None)

        # Подключение к базе данных
        connection = sqlite3.connect(f'source/{dbname}.db')
        cursor = connection.cursor()

        # Выполнение запроса на удаление
        cursor.execute("DELETE FROM pos_fiscals WHERE serialNumber = ?", (json_name,))

        # Проверка количества затронутых строк
        if cursor.rowcount > 0:
            log_console_out(f"Запись с 'serialNumber = {json_name}' в '{dbname}.db' успешно удалена.", "webs")
            messages_append(messages, f"Запись с 'serialNumber = {json_name}' в {dbname}.db успешно удалена.")
        else:
            log_console_out(f"Запись с 'serialNumber = {json_name}' в '{dbname}.db' не найдена.", "webs")
            messages_append(messages, f"Запись с 'serialNumber = {json_name}' в '{dbname}.db' не найдена.")

        # Сохранение изменений
        connection.commit()

    except Exception as e:
        log_console_out(f"Error: Не удалось удалить запись c serialNumber = {json_name} из базы данных", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

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

    except Exception as e:
        log_console_out("Error: Не удалось удалить всю информацию об ФР с FTP-сервера", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")
    finally:
        if 'ftp' in locals():
            ftp.quit()  # Закрываем соединение с FTP-сервером

    return messages


