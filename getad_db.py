import core.logger
import core.configs
import core.sys_manager
import integrations.bitrix24
import about
import os, json
import time
import core.dbmanagment
from flask import Flask, render_template, request, jsonify
import multiprocessing
import eventlet
from eventlet import wsgi
from functools import wraps
from core.delete_fr import delete_fr
from flask import send_from_directory
from flask import make_response
import configparser
import threading


db_update = core.dbmanagment.DbUpdate()
db_queries = core.dbmanagment.DbQueries()
bitrix24 = integrations.bitrix24.Bitrix24Task()


class WebServerSetup(core.sys_manager.ResourceManagement):
    app = Flask(__name__, static_folder='static')

    def __init__(self):
        super().__init__()
        try:
            self.port = int(self.config.getint("webserver", "port", fallback=None))
        except Exception:
            self.port = 30005

        self.server_process = None

    def webserver(self):
        eventlet.wsgi.server(eventlet.listen(('0.0.0.0', self.port)), self.app, debug=False)

    def crash_server(self):
        time.sleep(5)
        if self.server_process:
            self.server_process.terminate()
            self.server_process.join(timeout=5)  # Ждём максимум 5 секунд
            if self.server_process.is_alive():  # Если процесс всё ещё жив
                self.server_process.kill()
        os._exit(1)

    def subprocess_run(self):
        bitrix24_thread = threading.Thread(target=bitrix24.task_manager, daemon=True)
        bitrix24_thread.start()

        db_update.pos_tables_update()

    # Функция для проверки аутентификации
    def check_auth(self, username, password):
        USERNAME = self.config.get("webserver", "user", fallback="user")
        PASSWORD = self.config.get("webserver", "pass", fallback="1234")

        ADMIN = self.config.get("webserver", "admin", fallback="admin")
        ADMIN_PASSWORD = self.config.get("webserver", "admin_pass", fallback="4321")

        return (username == USERNAME and password == PASSWORD) or (
                username == ADMIN and password == ADMIN_PASSWORD)

    def admin_auth(self, username, password):
        USERNAME = self.config.get("webserver", "admin", fallback="admin")
        PASSWORD = self.config.get("webserver", "admin_pass", fallback="4321")
        return username == USERNAME and password == PASSWORD

    # Функция для отображения страницы запроса аутентификации
    def authenticate(self):
        return ('Вы должны ввести правильные учетные данные.', 401,
                {'WWW-Authenticate': 'Basic realm="Login Required"'})

    # Декоратор для защиты маршрутов аутентификацией
    def requires_auth(self, f):
        @wraps(f)
        def decorated(*args, **kwargs):
            auth = request.authorization
            if not auth or not self.check_auth(auth.username, auth.password):
                return self.authenticate()
            return f(*args, **kwargs)

        return decorated

    def requires_auth_admin(self, f):
        @wraps(f)
        def decorated(*args, **kwargs):
            auth = request.authorization
            if not auth or not self.admin_auth(auth.username, auth.password):
                return self.authenticate()
            return f(*args, **kwargs)

        return decorated


class WebServerRoute(WebServerSetup):
    default_visible_columns = ['serialNumber', 'modelName', 'RNM', 'organizationName', 'fn_serial', 'dateTime_end',
                               'bootVersion', 'ffdVersion', 'INN', 'attribute_excise', 'attribute_marked',
                               'installed_driver', 'url_rms', 'teamviewer_id', 'anydesk_id', 'litemanager_id']
    def __init__(self):
        super().__init__()
        self.register_routes()

    def register_routes(self):
        # Регистрация всех маршрутов
        self.app.add_url_rule('/', 'index', self.requires_auth(self.index), methods=['GET'])
        self.app.add_url_rule('/fiscals', 'fiscals', self.requires_auth(self.fiscals), methods=['GET'])
        self.app.add_url_rule('/onlypos', 'pos', self.requires_auth(self.pos), methods=['GET'])
        self.app.add_url_rule('/search', 'search', self.requires_auth(self.search), methods=['GET', 'POST'])
        self.app.add_url_rule('/dont-update', 'dont_update', self.requires_auth(self.dont_update),
                              methods=['GET', 'POST'])
        self.app.add_url_rule('/dont-validation', 'dont_validation', self.requires_auth(self.dont_validation),
                              methods=['GET', 'POST'])
        self.app.add_url_rule('/del_fr', 'del_fr', self.requires_auth_admin(self.del_fr), methods=['POST'])
        self.app.add_url_rule('/expire_fn', 'expire_fn', self.requires_auth(self.expire_fn), methods=['GET', 'POST'])
        self.app.add_url_rule('/toggle_task', 'toggle_task_action', self.requires_auth(self.toggle_task_action),
                              methods=['POST'])
        self.app.add_url_rule('/settings', 'settings', self.requires_auth_admin(self.settings), methods=['GET'])
        self.app.add_url_rule('/download_file/<path:filename>', 'download_file',
                              self.requires_auth_admin(self.download_file), methods=['GET'])
        self.app.add_url_rule('/save_settings', 'save_settings', self.requires_auth_admin(self.save_settings),
                              methods=['POST'])
        self.app.add_url_rule('/edit_client_name', 'edit_client_name', self.requires_auth(self.edit_client_name),
                              methods=['POST'])
        self.app.add_url_rule('/download_license/<int:index>', 'download_license',
                              self.requires_auth(self.download_license), methods=['GET'])
        self.app.add_url_rule('/logout', 'logout', self.logout, methods=['GET'])

    def index(self):
        return render_template('index.html')

    def fiscals(self):
        data, columns = db_queries.get_data_pos_fiscals()

        return render_template('fiscals.html',
                               data=data,
                               columns=columns,
                               default_visible_columns=self.default_visible_columns,
                               enumerate=enumerate)

    def pos(self):
        data, columns = db_queries.only_pos()
        return render_template('pos.html', data=data, columns=columns)

    def search(self):
        try:
            if request.method == 'POST':
                search_query = request.form['search_query']

            modified_data, columns = db_queries.search_querie(search_query)

            return render_template('search.html', search_query=search_query,
                                   search_results=modified_data, columns=columns,
                                   default_visible_columns=self.default_visible_columns, enumerate=enumerate)
        except Exception:
            core.logger.web_server.error("Не удалось сделать поисковый запрос", exc_info=True)

    def dont_update(self):
        if request.method == 'POST':
            search_query = request.form['search_query']
            days = int(search_query)

        field = "current_time"
        modified_data, columns = db_queries.search_dont_update(field, days)

        return render_template('search.html', search_query=search_query,
                               search_results=modified_data, columns=columns,
                               default_visible_columns=self.default_visible_columns, enumerate=enumerate)

    def dont_validation(self):
        if request.method == 'POST':
            search_query = request.form['search_query']
            days = int(search_query)

        field = "v_time"
        modified_data, columns = db_queries.search_dont_update(field, days)

        return render_template('search.html', search_query=search_query,
                               search_results=modified_data, columns=columns,
                               default_visible_columns=self.default_visible_columns, enumerate=enumerate)

    def del_fr(self):
        results = []
        try:
            if request.method == 'POST':
                json_name = request.form['search_query']
                results.extend(delete_fr(json_name))  # Добавляем результаты в массив
        except Exception:
            core.logger.web_server.error("Не удалось удалить запись о ФР из БД", exc_info=True)
            results.append("Ошибка: не удалось удалить запись о ФР из БД")

        return jsonify(results)  # Возвращаем массив строк

    def expire_fn(self):
        if request.method == 'POST':
            start_date = request.form.get('start_date')
            end_date = request.form.get('end_date')
        else:
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            if not start_date or not end_date:
                start_date, end_date = db_queries.get_default_dates()

        show_marked = request.args.get('show_marked', 'false') == 'true'

        records = db_queries.get_expire_fn(start_date, end_date, show_marked)
        core.logger.web_server.debug(f"Получен список ФН, заканчивающихся от '{start_date}' до '{end_date}':")
        core.logger.web_server.debug(records)

        return render_template('expirefn.html',
                               records=records,
                               start_date=start_date,
                               end_date=end_date,
                               show_marked_only=show_marked)

    def toggle_task_action(self):
        try:
            serial_number = request.form.get('serialNumber')
            fn_serial = request.form.get('fnSerial')
            checked = request.form.get('checked') == 'true'

            result = db_queries.toggle_task(serial_number, fn_serial, checked, bitrix24)
            return jsonify(result)
        except Exception:
            core.logger.web_server.error(
                "Неожиданное исключение при проверке ФР, на которые заведены задачи", exc_info=True)
            return jsonify({'status': 'error', 'message': 'Внутренняя ошибка сервера'})

    def settings(self):
        try:
            # Получаем список файлов и папок из директории source
            source_dir = 'source'
            files_and_dirs = []
            for item in os.listdir(source_dir):
                item_path = os.path.join(source_dir, item)
                is_dir = os.path.isdir(item_path)

                # Проверяем, что файл не имеет расширение .ini
                if not (not is_dir and (item.lower().endswith('.ini') or item.lower().endswith('.json'))):
                    if is_dir:
                        # Если это директория, получаем список файлов в ней
                        subfiles = []
                        for subitem in os.listdir(item_path):
                            subitem_path = os.path.join(item_path, subitem)
                            # Пропускаем .ini файлы в поддиректориях также
                            if not (subitem.lower().endswith('.ini') or subitem.lower().endswith('.json')):
                                subfiles.append({
                                    'name': subitem,
                                    'path': os.path.join(item, subitem)
                                })
                        files_and_dirs.append({
                            'name': item,
                            'is_dir': True,
                            'files': subfiles
                        })
                    else:
                        # Это файл
                        files_and_dirs.append({
                            'name': item,
                            'is_dir': False,
                            'path': item
                        })

            # Читаем конфигурацию
            config = core.configs.read_config_ini(self.config_path)

            return render_template('settings.html', config=config, files=files_and_dirs)
        except Exception:
            core.logger.web_server.error("Не удалось открыть страницу настроек", exc_info=True)

    def download_file(self, filename):
        try:
            normalized_filename = filename.replace('\\', '/')
            return send_from_directory('source', normalized_filename, as_attachment=True)
        except Exception:
            core.logger.web_server.error(f"Не удалось скачать файл {filename}", exc_info=True)
            return "Ошибка при скачивании файла", 404

    def save_settings(self):
        try:
            settings = request.get_json()
            config = configparser.ConfigParser()

            # Записываем новые настройки
            for section, options in settings.items():
                if not config.has_section(section):
                    config.add_section(section)
                for option, value in options.items():
                    config.set(section, option, str(value))

            # Сохраняем в файл
            with open(self.config_path, 'w') as configfile:
                config.write(configfile)

            shutdown_thread = threading.Thread(target=self.crash_server)
            shutdown_thread.daemon = True  # Делаем поток демоном
            shutdown_thread.start()
            return jsonify({'success': True})
        except Exception as e:
            core.logger.web_server.error("Не удалось сохранить настройки", exc_info=True)
            return jsonify({'success': False, 'error': str(e)})

    def edit_client_name(self):
        try:
            data = request.get_json()
            url_rms = data['url_rms']
            server_name = data['server_name']

            result = db_queries.edit_client_name(url_rms, server_name)
            return jsonify(result)
        except Exception as e:
            core.logger.web_server.error("Ошибка при обновлении имени клиента", exc_info=True)
            return jsonify({'success': False, 'error': str(e)})

    def download_license(self, index):
        try:
            # Получите данные из БД для указанного индекса строки
            license_data = [...]  # Получите данные из БД для указанного индекса
            # Создание и отправка JSON-файла
            response = json.dumps(license_data, ensure_ascii=False, indent=4)
            response.headers['Content-Disposition'] = 'attachment; filename=license.json'
            response.headers['Content-Type'] = 'text/plain'
            return response
        except Exception:
            core.logger.web_server.error("Не удалось сохранить данные о лицензиях", exc_info=True)

    def logout(self):
        response = make_response('', 401)  # Код 401 = Unauthorized
        response.headers['WWW-Authenticate'] = 'Basic realm="Login Required"'
        return response


if __name__ == "__main__":
    if not os.path.exists(about.config_path):
        core.configs.create_confgi_ini()

    webserver = WebServerRoute()

    core.logger.web_server.info(f"Версия: {about.version}")

    server_process = multiprocessing.Process(target=webserver.subprocess_run)
    server_process.daemon = True
    server_process.start()

    webserver.webserver()
