import core.logger
import core.configs
from core.connectors import FtpContextManager
from core.sys_manager import DatabaseContextManager
import about
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

def ftp_delete_json(json_name):
    messages = []
    try:
        with core.connectors.FtpContextManager() as ftp:
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

def delete_record_by_serial_number(json_name):
    messages = []
    try:
        with DatabaseContextManager() as db:
            # Выполнение запроса на удаление
            db.cursor.execute('DELETE FROM pos_fiscals WHERE "serialNumber" = %s', (json_name,))

            # Проверка количества затронутых строк
            if db.cursor.rowcount > 0:
                core.logger.web_server.info(
                    f"Запись с serialNumber = '{json_name}' из таблицы 'pos_fiscals' успешно удалена")
                messages_append(
                    messages, f"Запись с serialNumber = '{json_name}' из таблицы 'pos_fiscals' успешно удалена")
            else:
                core.logger.web_server.info(f"Запись с serialNumber = '{json_name}' в таблице 'pos_fiscals' не найдена")
                messages_append(
                    messages, f"Запись с serialNumber = '{json_name}' в таблице 'pos_fiscals' не найдена")

                filename = f"{json_name}.json"
                db.cursor.execute('DELETE FROM pos_not_fiscals WHERE "filename" = %s', (filename,))
                if db.cursor.rowcount > 0:
                    core.logger.web_server.info(
                        f"Запись с fileName = '{filename}' из таблицы 'pos_not_fiscals' успешно удалена")
                    messages_append(
                        messages, f"Запись с fileName = '{filename}' из таблицы 'pos_not_fiscals' успешно удалена")
                else:
                    core.logger.web_server.info(
                        f"Запись с fileName = '{filename}' в таблице 'pos_not_fiscals' не найдена")
                    messages_append(
                        messages, f"Запись с fileName = '{filename}' в таблице 'pos_not_fiscals' не найдена")

    except Exception:
        core.logger.web_server.error(
            f"Не удалось удалить запись '{json_name}' из базы данных", exc_info=True)
    return messages


def delete_fr(json_name):
    messages = []
    try:
        # Создаем новый поток и передаем в него функцию get_db_data()
        ftp_delete_json_thread = threading.Thread(target=messages.extend(ftp_delete_json(json_name)))
        # Запускаем поток
        ftp_delete_json_thread.start()

        delete_record_by_serial_number_theard = threading.Thread(target=messages.extend(delete_record_by_serial_number(json_name)))
        # Запускаем поток
        delete_record_by_serial_number_theard.start()

    except Exception:
        core.logger.web_server.error("Не удалось удалить всю информацию об ФР с FTP-сервера", exc_info=True)

    return messages


