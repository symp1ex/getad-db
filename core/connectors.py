import core.logger
import core.sys_manager
import core.dbmanagement
import requests
import time
import ftplib
import queue
import threading
import json
import os
from functools import wraps
from flask import request, jsonify

dbquerie = core.dbmanagement.DbQueries()

class ApiConnector(core.sys_manager.ResourceManagement):
    def __init__(self):
        super().__init__()
        # Создаем очередь для обработки JSON-запросов
        self.json_queue = queue.Queue()
        self.user_api_key = None
        self.admin_api_key = None

        try: self.ftp_backup = int(self.config.get("ftp-connect", "ftp_backup", fallback=0))
        except: self.ftp_backup = 0

        # Запускаем обработчик очереди в отдельном потоке
        self.queue_processor = threading.Thread(target=self.process_queue, daemon=True)
        self.queue_processor.start()

    def update_api_keys(self):
        self.user_api_key = dbquerie.get_api_key(0)
        self.admin_api_key = dbquerie.get_api_key(1)
        core.logger.connectors.info("Обновлён список API-ключей")

    def requires_api_key(self, f):
        # Декоратор для проверки API-ключа в запросах
        @wraps(f)
        def decorated(*args, **kwargs):
            # Получаем API ключ из заголовка
            api_key = request.headers.get('X-API-Key')
            if not api_key or (api_key not in self.user_api_key and api_key not in self.admin_api_key):
                return jsonify({'status': 'error', 'message': 'Invalid API key'}), 403

            return f(*args, **kwargs)

        return decorated

    def requires_admin_api_key(self, f):
        # Декоратор для проверки API-ключа в запросах
        @wraps(f)
        def decorated(*args, **kwargs):
            # Получаем API ключ из заголовка
            api_key = request.headers.get('X-API-Key')
            if not api_key or api_key not in self.admin_api_key:
                return jsonify({'status': 'error', 'message': 'Invalid API key'}), 403

            return f(*args, **kwargs)

        return decorated

    def process_queue(self):
        # Обработчик очереди JSON, работает в отдельном потоке
        while True:
            try:
                # Если очередь не пуста, обрабатываем данные
                if not self.json_queue.empty():
                    json_data = self.json_queue.get()
                    # Проверяем тип данных: фискальный или обычный
                    if isinstance(json_data, dict) and "serialNumber" in json_data:
                        dbquerie.save_fiscals({json_data["serialNumber"]: json_data})
                        filename = f"{json_data.get('serialNumber')}.json"
                    else:
                        teamviever_id = json_data.get("teamviewer_id")
                        anydesk_id = json_data.get("anydesk_id")
                        filename = f"TV{teamviever_id}_AD{anydesk_id}.json"
                        dbquerie.save_not_fiscal(json_data, filename)

                    # Подтверждаем завершение задачи
                    self.json_queue.task_done()
                    core.logger.connectors.info(
                        f"Обработан JSON через API: {filename}")

                    if self.ftp_backup == 1:
                        self.ftp_upload(json_data, filename)
                else:
                    # Если очередь пуста, делаем небольшую паузу чтобы не загружать процессор
                    time.sleep(0.1)
            except Exception as e:
                core.logger.connectors.error(f"Ошибка в обработчике очереди: {str(e)}", exc_info=True)
                time.sleep(1)  # Пауза после ошибки

    def ftp_upload(self,  json_data, json_name, send_timeout=10, max_attempts=5, attempt=1):
        try:
            # Создаем временную директорию, если ее нет
            temp_dir = 'temp_files/ftp_backup'
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)

            # Путь к временному файлу
            temp_file_path = os.path.join(temp_dir, json_name)

            # Сохраняем JSON-данные во временный файл
            with open(temp_file_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=4)

            # Отправляем файл на FTP-сервер
            with FtpContextManager() as ftp:
                with open(temp_file_path, 'rb') as f:
                    ftp.storbinary('STOR ' + json_name, f)
                core.logger.connectors.info(f"Передача файла '{json_name}' на FTP-сервер завершена")

            # Удаляем временный файл после отправки
            os.remove(temp_file_path)
            return True
        except Exception:
            if attempt < max_attempts:
                core.logger.connectors.warn(
                    f"Попытка ({attempt}) отправки данных не удалась. Повторная попытка через ({send_timeout}) секунд...")
                attempt += 1
                time.sleep(send_timeout)
                return self.ftp_upload(json_data, json_name, send_timeout, max_attempts, attempt)
            else:
                core.logger.connectors.error(f"Отправка данных на FTP-сервер не удалась после ({max_attempts}) попыток",
                                     exc_info=True)


class ApiMethod(ApiConnector):
    def __init__(self):
        super().__init__()

    def submit_json(self):
        try:
            # Получаем JSON из запроса
            core.logger.connectors.info("Получен запрос к '/api/submit_json'")

            json_data = request.get_json()
            core.logger.connectors.debug(f"{request} - {json_data}")

            if not json_data:
                core.logger.connectors.warning({'status': 'error', 'message': 'No JSON data provided'})
                return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400
            if not (isinstance(json_data, dict) and "url_rms" in json_data and "current_time" in json_data):
                core.logger.connectors.warning("Полученный json не соответствует требуемому формату")
                return jsonify({'status': 'error', 'message': 'Invalid JSON-data'}), 400

            # Добавляем в очередь
            self.json_queue.put(json_data)
            core.logger.connectors.info({'status': 'success', 'message': 'Data queued for processing'})
            return jsonify({'status': 'success', 'message': 'Data queued for processing'})
        except Exception as e:
            core.logger.connectors.warning({'status': 'error', 'message': str(e)})
            core.logger.connectors.error("Ошибка при обработке JSON через API", exc_info=True)
            return jsonify({'status': 'error', 'message': str(e)}), 500

    def get_serial_numbers(self):
        try:
            # Получаем JSON из запроса
            core.logger.connectors.info("Получен запрос к '/api/get_serial_numbers'")

            json_data = request.get_json()
            core.logger.connectors.debug(f"{request} - {json_data}")

            if not json_data:
                core.logger.connectors.warning({'status': 'error', 'message': 'No JSON data provided'})
                return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400

            # получаем параметр info
            info = json_data.get('clients_info', False)
            result = dbquerie.get_serial_numbers_info(info)

            core.logger.connectors.info(jsonify(result))
            core.logger.connectors.debug(result)
            return jsonify(result)
        except Exception as e:
            core.logger.connectors.warning({'status': 'error', 'message': str(e)})
            core.logger.connectors.error("Ошибка при получении данных о серийных номерах через API", exc_info=True)
            return jsonify({'status': 'error', 'message': str(e)}), 500

    def get_fiscals_data(self):
        try:
            core.logger.connectors.info("Получен запрос к '/api/get_fiscals_data'")

            # Получаем JSON из запроса
            json_data = request.get_json()
            core.logger.connectors.debug(f"{request} - {json_data}")

            if not json_data or not isinstance(json_data, list):
                core.logger.connectors.warning(
                    {'status': 'error', 'message': 'Request must be a JSON array of serial numbers'})
                return jsonify({'status': 'error', 'message': 'Request must be a JSON array of serial numbers'}), 400

            # Проверяем, что в запросе пришел список серийных номеров
            serial_numbers = json_data

            # Если список пуст, возвращаем пустой результат
            if not serial_numbers:
                core.logger.connectors.warning("Получен пустой список серийных номеров")
                return jsonify([])

            # Получаем данные из БД
            fiscals_data = dbquerie.get_fiscals_by_serial_numbers(serial_numbers)

            core.logger.connectors.info(jsonify(fiscals_data))
            core.logger.connectors.debug(fiscals_data)
            return jsonify(fiscals_data)
        except Exception as e:
            core.logger.connectors.warning({'status': 'error', 'message': str(e)})
            core.logger.connectors.error("Ошибка при получении данных о ККТ по серийным номерам через API",
                                         exc_info=True)
            return jsonify({'status': 'error', 'message': str(e)}), 500

    def get_pos_data(self):
        try:
            core.logger.connectors.info("Получен запрос к '/api/get_pos_data'")
            data, columns = dbquerie.get_only_pos()

            pos_data = []

            for row in data:
                pos_dict = {}
                for i, column in enumerate(columns):
                    # Для полей типа JSON или списков преобразуем в строковый формат
                    if isinstance(row[i], (dict, list)):
                        pos_dict[column] = json.dumps(row[i], ensure_ascii=False)
                    else:
                        pos_dict[column] = row[i]
                pos_data.append(pos_dict)
            return jsonify(pos_data)
        except Exception as e:
            core.logger.connectors.warning({'status': 'error', 'message': str(e)})
            core.logger.connectors.error("Ошибка при получении данных о POS через API",
                                         exc_info=True)
            return jsonify({'status': 'error', 'message': str(e)}), 500


class IikoRms(core.sys_manager.ResourceManagement):
    def __init__(self):
        super().__init__()

    def get_rms_name(self, url_rms):
        try:
            # Формируем URL для запроса
            monitoring_url = f"{url_rms.rstrip('/')}/getServerMonitoringInfo.jsp"

            # Делаем запрос
            core.logger.connectors.debug(f"Сделан запрос к {monitoring_url}")
            response = requests.get(monitoring_url, timeout=20)

            if response.status_code == 200:
                core.logger.connectors.debug(f"Код ответа {response.status_code}")
                json_data = response.json()
                server_name = json_data.get('serverName', '')
                version = json_data.get('version', '')
                return server_name, version
            else:
                raise Exception(f"Код ответа {response.status_code}")
        except Exception:
            core.logger.connectors.error(f"Не удалось сделать запрос к {url_rms}", exc_info=True)
            raise

class FtpContextManager(core.sys_manager.ResourceManagement):
    def __init__(self):
        super().__init__()
        try:
            self.server = self.config.get("ftp-connect", "ftpHost", fallback=None)
            self.username = self.config.get("ftp-connect", "ftpUser", fallback=None)
            self.password = self.config.get("ftp-connect", "ftpPass", fallback=None)
        except Exception:
            core.logger.connectors.error(
                "Не удалось параметры подключения к FTP-серверу, проверьте настройки", exc_info=True)

        self.ftp = None

    def __enter__(self):
        try:
            self.ftp = ftplib.FTP(self.server)
            self.ftp.login(self.username, self.password)
            return self.ftp
        except Exception:
            core.logger.connectors.error(
                "Не удалось подключиться к FTP-серверу, проверьте параметры подключения", exc_info=True)

    def __exit__(self, exc_type, exc_value, traceback):
        if self.ftp:
            try: self.ftp.quit()
            except: pass


