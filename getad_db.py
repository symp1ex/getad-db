import core.logger
import core.configs
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

config = core.configs.read_config_ini(about.config_path)
try: port = int(config.getint("webserver", "port", fallback=None))
except Exception: port = 30005

db_queries = core.dbmanagment.DbQueries()


app = Flask(__name__, static_folder='static')

server_process = None

def crash_server():
    time.sleep(5)
    global server_process

    if server_process:
        server_process.terminate()
        server_process.join(timeout=5)  # Ждём максимум 5 секунд
        if server_process.is_alive():   # Если процесс всё ещё жив
            server_process.kill()
    os._exit(1)

def webserver():
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app, debug=False)

# Функция для проверки аутентификации
def check_auth(username, password):
    USERNAME = config.get("webserver", "user", fallback="user")
    PASSWORD = config.get("webserver", "pass", fallback="1234")

    ADMIN = config.get("webserver", "admin", fallback="admin")
    ADMIN_PASSWORD = config.get("webserver", "admin_pass", fallback="4321")

    return (username == USERNAME and password == PASSWORD) or (
                username == ADMIN and password == ADMIN_PASSWORD)


def admin_auth(username, password):
    USERNAME = config.get("webserver", "admin", fallback="admin")
    PASSWORD = config.get("webserver", "admin_pass", fallback="4321")
    return username == USERNAME and password == PASSWORD

# Функция для отображения страницы запроса аутентификации
def authenticate():
    return ('Вы должны ввести правильные учетные данные.', 401,
            {'WWW-Authenticate': 'Basic realm="Login Required"'})

# Декоратор для защиты маршрутов аутентификацией
def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)

    return decorated

def requires_auth_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not admin_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)

    return decorated


@app.route('/')
@requires_auth
def index():
    return render_template('index.html')


@app.route('/fiscals')
@requires_auth
def fiscals():
    data, columns = db_queries.get_data_pos_fiscals()
    # Определяем столбцы, которые должны быть видимы по умолчанию
    default_visible_columns = ['serialNumber', 'modelName', 'RNM', 'organizationName', 'fn_serial', 'dateTime_end',
                               'bootVersion', 'ffdVersion', 'INN', 'attribute_excise', 'attribute_marked',
                               'installed_driver', 'url_rms', 'teamviewer_id', 'anydesk_id', 'litemanager_id']
    return render_template('fiscals.html',
                         data=data,
                         columns=columns,
                         default_visible_columns=default_visible_columns,
                         enumerate=enumerate)

@app.route('/download_license/<int:index>')
def download_license(index):
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

@app.route('/onlypos')
@requires_auth
def pos():
    data, columns = db_queries.only_pos()
    return render_template('pos.html', data=data, columns=columns)


@app.route('/search', methods=['GET', 'POST'])
@requires_auth
def search():
    try:
        if request.method == 'POST':
            search_query = request.form['search_query']

        modified_data, columns = db_queries.search_querie(search_query)

        default_visible_columns = ['serialNumber', 'modelName', 'RNM', 'organizationName', 'fn_serial',
                                   'dateTime_end', 'bootVersion', 'ffdVersion', 'INN', 'attribute_excise',
                                   'attribute_marked', 'installed_driver', 'url_rms', 'teamviewer_id',
                                   'anydesk_id', 'litemanager_id']

        return render_template('search.html', search_query=search_query,
                               search_results=modified_data, columns=columns,
                               default_visible_columns=default_visible_columns, enumerate=enumerate)
    except Exception:
        core.logger.web_server.error("Не удалось сделать поисковый запрос", exc_info=True)


@app.route('/dont-update', methods=['GET', 'POST'])
@requires_auth
def dont_update():
    if request.method == 'POST':
        search_query = request.form['search_query']
        days = int(search_query)

    field = "current_time"
    modified_data, columns = db_queries.search_dont_update(field, days)

    default_visible_columns = ['serialNumber', 'modelName', 'RNM', 'organizationName', 'fn_serial',
                               'dateTime_end',
                               'bootVersion', 'ffdVersion', 'INN', 'attribute_excise', 'attribute_marked',
                               'installed_driver', 'url_rms', 'teamviewer_id', 'anydesk_id', 'litemanager_id']

    return render_template('search.html', search_query=search_query,
                           search_results=modified_data, columns=columns,
                           default_visible_columns=default_visible_columns, enumerate=enumerate)

@app.route('/dont-validation', methods=['GET', 'POST'])
@requires_auth
def dont_validation():
    if request.method == 'POST':
        search_query = request.form['search_query']
        days = int(search_query)

    field = "v_time"
    modified_data, columns = db_queries.search_dont_update(field, days)

    default_visible_columns = ['serialNumber', 'modelName', 'RNM', 'organizationName', 'fn_serial',
                               'dateTime_end',
                               'bootVersion', 'ffdVersion', 'INN', 'attribute_excise', 'attribute_marked',
                               'installed_driver', 'url_rms', 'teamviewer_id', 'anydesk_id', 'litemanager_id']

    return render_template('search.html', search_query=search_query,
                           search_results=modified_data, columns=columns,
                           default_visible_columns=default_visible_columns, enumerate=enumerate)

@app.route('/del_fr', methods=['POST'])
@requires_auth_admin
def del_fr():
    results = []
    try:
        if request.method == 'POST':
            json_name = request.form['search_query']
            results.extend(delete_fr(json_name))  # Добавляем результаты в массив
    except Exception:
        core.logger.web_server.error("Не удалось удалить запись о ФР из БД", exc_info=True)
        results.append("Ошибка: не удалось удалить запись о ФР из БД")

    return jsonify(results)  # Возвращаем массив строк

@app.route('/expire_fn', methods=['GET', 'POST'])
@requires_auth
def expire_fn():
    if request.method == 'POST':
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
    else:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        if not start_date or not end_date:
            start_date, end_date = db_queries.get_default_dates()

    show_marked = request.args.get('show_marked_only', 'true') == 'true'

    records = db_queries.get_expire_fn(start_date, end_date, show_marked)
    core.logger.web_server.debug(f"Получен список ФН, заканчивающихся от '{start_date}' до '{end_date}':")
    core.logger.web_server.debug(records)

    return render_template('expirefn.html',
                         records=records,
                         start_date=start_date,
                         end_date=end_date,
                         show_marked_only=show_marked)


@app.route('/toggle_task', methods=['POST'])
@requires_auth
def toggle_task():
    try:
        serial_number = request.form.get('serialNumber')
        fn_serial = request.form.get('fnSerial')
        checked = request.form.get('checked') == 'true'

        result = db_queries.toggle_task(serial_number, fn_serial, checked)
        return jsonify(result)
    except Exception:
        core.logger.web_server.error(
            "Неожиданное исключение при проверке ФР, на которые заведены задачи", exc_info=True)
        return jsonify({'status': 'error', 'message': 'Внутренняя ошибка сервера'})

@app.route('/settings')
@requires_auth_admin
def settings():
    try:
        # Получаем список файлов и папок из директории source
        source_dir = 'source'
        files_and_dirs = []
        for item in os.listdir(source_dir):
            item_path = os.path.join(source_dir, item)
            is_dir = os.path.isdir(item_path)

            # Проверяем, что файл не имеет расширение .ini
            if not (not is_dir and item.lower().endswith('.ini')):
                if is_dir:
                    # Если это директория, получаем список файлов в ней
                    subfiles = []
                    for subitem in os.listdir(item_path):
                        subitem_path = os.path.join(item_path, subitem)
                        # Пропускаем .ini файлы в поддиректориях также
                        if not subitem.lower().endswith('.ini'):
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
        config = core.configs.read_config_ini(about.config_path)

        return render_template('settings.html', config=config, files=files_and_dirs)
    except Exception:
        core.logger.web_server.error("Не удалось открыть страницу настроек", exc_info=True)


@app.route('/download_file/<path:filename>')
@requires_auth_admin
def download_file(filename):
    try:
        normalized_filename = filename.replace('\\', '/')
        return send_from_directory('source', normalized_filename, as_attachment=True)
    except Exception:
        core.logger.web_server.error(f"Не удалось скачать файл {filename}", exc_info=True)
        return "Ошибка при скачивании файла", 404


@app.route('/save_settings', methods=['POST'])
@requires_auth_admin
def save_settings():
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
        with open(about.config_path, 'w') as configfile:
            config.write(configfile)

        shutdown_thread = threading.Thread(target=crash_server)
        shutdown_thread.daemon = True  # Делаем поток демоном
        shutdown_thread.start()
        return jsonify({'success': True})
    except Exception as e:
        core.logger.web_server.error("Не удалось сохранить настройки", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/update_client_name', methods=['POST'])
@requires_auth
def update_client_name():
    try:
        data = request.get_json()
        url_rms = data['url_rms']
        server_name = data['server_name']

        result = db_queries.update_client_name(url_rms, server_name)
        return jsonify(result)
    except Exception as e:
        core.logger.web_server.error("Ошибка при обновлении имени клиента", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/logout')
def logout():
    response = make_response('', 401)  # Код 401 = Unauthorized
    response.headers['WWW-Authenticate'] = 'Basic realm="Login Required"'
    return response


if __name__ == "__main__":
    db_update = core.dbmanagment.DbUpdate()

    core.logger.web_server.info(f"Версия: {about.version}")

    if not os.path.exists(about.config_path):
        core.configs.create_confgi_ini()

    server_process = multiprocessing.Process(target=db_update.pos_tables_update)
    server_process.daemon = True
    server_process.start()

    webserver()
