import os, json
import time
from datetime import datetime, timedelta, date
from pfb import ftp_connect
from logger import log_console_out, exception_handler, create_confgi_ini, read_config_ini, config_path, db_path
from flask import Flask, render_template, request, jsonify
import sqlite3
import multiprocessing
import eventlet
from eventlet import wsgi
from functools import wraps
from delete_fr import delete_fr
import calendar
from flask import send_from_directory
from flask import make_response
import configparser
import threading

config = read_config_ini(config_path)
try: port = int(config.getint("webserver", "port", fallback=None))
except Exception: port = 30005


app = Flask(__name__)

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


# Функция для получения данных из базы SQLite
def get_data_pas_fiscals():
    try:
        dbname = config.get("db-update", "db-name", fallback=None)
        format_db_path = db_path.format(dbname=dbname)
        dont_valid_fn = int(config.get("db-update", "day_filter_expire", fallback=5))

        connection = sqlite3.connect(format_db_path)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pos_fiscals")
        data = cursor.fetchall()
        cursor.execute("PRAGMA table_info(pos_fiscals)")
        columns = [column[1] for column in cursor.fetchall()]

        # Создаем новый список для данных с дополнительной информацией об устаревании
        modified_data = []
        for row in data:
            modified_row = list(row)
            licenses_data_index = columns.index('licenses')
            current_time_index = columns.index('current_time')
            v_time_index = columns.index('v_time')

            # Обработка licenses
            licenses_data = row[licenses_data_index]
            if licenses_data:
                modified_row[licenses_data_index] = licenses_data

            # Проверка устаревания записи
            time_to_check = row[v_time_index] if row[v_time_index] not in (None, '', 'None') else row[current_time_index]
            is_expired = not if_show_fn_to_date(time_to_check, dont_valid_fn) if time_to_check else False

            # Добавляем признак устаревания в строку
            modified_row.append(is_expired)

            modified_data.append(modified_row)

        connection.close()
        return modified_data, columns
    except Exception as e:
        log_console_out("Error: при чтении таблицы 'pos_fiscals' произошло исключение", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")


def only_pos():
    try:
        dbname = config.get("db-update", "db-name", fallback=None)
        format_db_path = db_path.format(dbname=dbname)

        connection = sqlite3.connect(format_db_path)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pos_not_fiscals")
        data = cursor.fetchall()
        cursor.execute("PRAGMA table_info(pos_not_fiscals)")
        columns = [column[1] for column in cursor.fetchall()]  # Получаем названия столбцов
        connection.close()
        return data, columns
    except Exception as e:
        log_console_out("Error: при чтении таблицы 'pos_not_fiscals' произошло исключение", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

def search_dont_update(field):
    try:
        dbname = config.get("db-update", "db-name", fallback=None)
        format_db_path = db_path.format(dbname=dbname)

        if request.method == 'POST':
            search_query = request.form['search_query']
            days = int(search_query)
            dont_valid_fn = int(config.get("db-update", "day_filter_expire", fallback=5))

            connection = sqlite3.connect(format_db_path)
            cursor = connection.cursor()
            # Преобразуем текущую дату в формат, который хранится в базе данных
            today_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Вычисляем дату, которая на days дней меньше текущей даты
            past_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute("PRAGMA table_info(pos_fiscals)")  # Получаем названия столбцов из базы данных
            columns = [column[1] for column in cursor.fetchall()]

            # Создаем запрос SQL для выборки строк, удовлетворяющих условиям
            query = f"SELECT * FROM pos_fiscals WHERE strftime('%s', [{field}]) < strftime('%s', '{past_date}')"
            cursor.execute(query)
            search_results = cursor.fetchall()
            # Создаем новый список для данных с замененными значениями в столбце licenses
            modified_data = []
            for row in search_results:
                modified_row = list(row)  # Преобразуем кортеж в список
                licenses_data_index = columns.index('licenses')
                current_time_index = columns.index('current_time')
                v_time_index = columns.index('v_time')

                licenses_data = row[licenses_data_index]
                if licenses_data:  # Если есть данные в столбце licenses
                    # Замена данных в столбце licenses на ссылку
                    modified_row[licenses_data_index] = licenses_data

                time_to_check = row[v_time_index] if row[v_time_index] not in (None, '', 'None') else row[current_time_index]
                is_expired = not if_show_fn_to_date(time_to_check, dont_valid_fn) if time_to_check else False

                modified_data.append(modified_row)

                modified_row.append(is_expired)
            connection.close()
            return search_query, modified_data, columns
    except Exception as e:
        log_console_out("Error: не удалось сделать посиковый запрос", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

def get_default_dates():
    try:
        today = date.today()
        next_month = today.replace(day=1)
        if today.month == 12:
            next_month = next_month.replace(year=today.year + 1, month=1)
        else:
            next_month = next_month.replace(month=today.month + 1)
        last_day = next_month.replace(day=calendar.monthrange(next_month.year, next_month.month)[1])
        return today.strftime('%Y-%m-%d'), last_day.strftime('%Y-%m-%d')
    except Exception as e:
        log_console_out("Error: не установить дефолтный диапозон дат", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

def if_show_fn_to_date(date_string, dont_valid_fn):
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
    except Exception as e:
        log_console_out(f"Error: не удалось вычислить разницу между текущей датой и {date_string}", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

def get_expire_fn():
    try:
        dont_valid_fn = int(config.get("db-update", "day_filter_expire", fallback=5))

        dbname = config.get("db-update", "db-name", fallback=None)
        format_db_path = db_path.format(dbname=dbname)
        conn = sqlite3.connect(format_db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fn_sale_task (
                serialNumber TEXT PRIMARY KEY,
                fn_serial TEXT
            )
        ''')
        conn.commit()
        conn.close()

        if request.method == 'POST':
            start_date = request.form.get('start_date')
            end_date = request.form.get('end_date')
        else:
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            if not start_date or not end_date:
                start_date, end_date = get_default_dates()

        show_marked_only = request.args.get('show_marked_only', 'true') == 'true'

        conn = sqlite3.connect(format_db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT serialNumber FROM fn_sale_task')
        marked_records = {row[0] for row in cursor.fetchall()}

        # Добавляем v_time в SQL запрос
        base_query = """
            SELECT serialNumber, RNM, fn_serial, organizationName, INN, 
                   date(dateTime_end) as dateTime_end, [current_time], [v_time]
            FROM pos_fiscals 
            WHERE date(dateTime_end) >= date(?) AND date(dateTime_end) <= date(?)
        """

        if not show_marked_only:
            base_query += " AND serialNumber NOT IN (SELECT serialNumber FROM fn_sale_task)"

        base_query += " ORDER BY dateTime_end ASC"

        cursor.execute(base_query, (start_date, end_date))
        rows = cursor.fetchall()

        records = []
        for row in rows:
            record = dict(
                zip(['serialNumber', 'RNM', 'fn_serial', 'organizationName', 'INN',
                     'dateTime_end', 'current_time', 'v_time'], row))

            # Определяем, какое время использовать
            time_to_check = record['v_time'] if record['v_time'] not in (None, '', 'None') else record['current_time']

            # Проверяем условие через функцию if_show_fn_to_date
            if if_show_fn_to_date(time_to_check, dont_valid_fn):
                # Удаляем временные поля из словаря
                del record['current_time']
                del record['v_time']
                # Добавляем информацию о том, отмечена ли запись
                record['is_marked'] = record['serialNumber'] in marked_records
                records.append(record)

        conn.close()

        return records, start_date, end_date, show_marked_only
    except Exception as e:
        log_console_out("Error: неожиданное исключение при запросе к заканчивающимся ФН", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")


@app.route('/')
@requires_auth
def index():
    return render_template('index.html')


@app.route('/fiscals')
@requires_auth
def fiscals():
    data, columns = get_data_pas_fiscals()
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
    except Exception as e:
        log_console_out("Error: не удалось сохранить данные о лицензиях", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

@app.route('/onlypos')
@requires_auth
def pos():
    data, columns = only_pos()
    return render_template('pos.html', data=data, columns=columns)


@app.route('/search', methods=['GET', 'POST'])
@requires_auth
def search():
    try:
        dbname = config.get("db-update", "db-name", fallback=None)
        format_db_path = db_path.format(dbname=dbname)

        if request.method == 'POST':
            search_query = request.form['search_query']
            dont_valid_fn = int(config.get("db-update", "day_filter_expire", fallback=5))

            connection = sqlite3.connect(format_db_path)
            cursor = connection.cursor()
            cursor.execute("PRAGMA table_info(pos_fiscals)")
            columns = [column[1] for column in cursor.fetchall()]

            query = "SELECT * FROM pos_fiscals WHERE "
            for column in columns:
                query += f"{column} LIKE '%{search_query}%' OR "
            query = query[:-4]

            cursor.execute(query)
            search_results = cursor.fetchall()

            # Создаем новый список для данных с проверкой на устаревание
            modified_data = []
            for row in search_results:
                modified_row = list(row)
                licenses_data_index = columns.index('licenses')
                current_time_index = columns.index('current_time')
                v_time_index = columns.index('v_time')

                # Обработка licenses
                licenses_data = row[licenses_data_index]
                if licenses_data:
                    modified_row[licenses_data_index] = licenses_data

                # Проверка устаревания записи
                time_to_check = row[v_time_index] if row[v_time_index] not in (None, '', 'None') else row[current_time_index]
                is_expired = not if_show_fn_to_date(time_to_check, dont_valid_fn) if time_to_check else False

                # Добавляем признак устаревания в строку
                modified_row.append(is_expired)

                modified_data.append(modified_row)

            connection.close()
            default_visible_columns = ['serialNumber', 'modelName', 'RNM', 'organizationName', 'fn_serial',
                                       'dateTime_end',
                                       'bootVersion', 'ffdVersion', 'INN', 'attribute_excise', 'attribute_marked',
                                       'installed_driver', 'url_rms', 'teamviewer_id', 'anydesk_id', 'litemanager_id']
            return render_template('search.html', search_query=search_query,
                                   search_results=modified_data, columns=columns,
                                   default_visible_columns=default_visible_columns, enumerate=enumerate)
    except Exception as e:
        log_console_out("Error: не удалось сделать поисковый запрос", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")


@app.route('/dont-update', methods=['GET', 'POST'])
@requires_auth
def dont_update():
    field = "current_time"
    search_query, modified_data, columns = search_dont_update(field)
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
    field = "v_time"
    search_query, modified_data, columns = search_dont_update(field)
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
    except Exception as e:
        log_console_out("Error: не удалось удалить запись о ФР из БД", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")
        results.append("Ошибка: не удалось удалить запись о ФР из БД")

    return jsonify(results)  # Возвращаем массив строк

@app.route('/expire_fn', methods=['GET', 'POST'])
@requires_auth
def expire_fn():
    records, start_date, end_date, show_marked_only = get_expire_fn()

    return render_template('expirefn.html',
                         records=records,
                         start_date=start_date,
                         end_date=end_date,
                         show_marked_only=show_marked_only)


@app.route('/toggle_task', methods=['POST'])
@requires_auth
def toggle_task():
    try:
        serial_number = request.form.get('serialNumber')
        fn_serial = request.form.get('fnSerial')
        checked = request.form.get('checked') == 'true'

        dbname = config.get("db-update", "db-name", fallback=None)
        format_db_path = db_path.format(dbname=dbname)
        conn = sqlite3.connect(format_db_path)
        cursor = conn.cursor()

        try:
            if checked:
                cursor.execute('INSERT OR IGNORE INTO fn_sale_task (serialNumber, fn_serial) VALUES (?, ?)',
                               (serial_number, fn_serial))
            else:
                cursor.execute('DELETE FROM fn_sale_task WHERE serialNumber = ?', (serial_number,))
            conn.commit()
            return jsonify({'status': 'success'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})
        finally:
            conn.close()
    except Exception as e:
        log_console_out("Error: неожиданное исключение при проверке ФР, на которые заведены задачи", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")

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
        config = read_config_ini(config_path)

        return render_template('settings.html', config=config, files=files_and_dirs)
    except Exception as e:
        log_console_out("Error: не удалось открыть страницу настроек", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")


@app.route('/download_file/<path:filename>')
@requires_auth_admin
def download_file(filename):
    try:
        return send_from_directory('source', filename, as_attachment=True)
    except Exception as e:
        log_console_out(f"Error: не удалось скачать файл {filename}", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")
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
        with open(config_path, 'w') as configfile:
            config.write(configfile)

        shutdown_thread = threading.Thread(target=crash_server)
        shutdown_thread.daemon = True  # Делаем поток демоном
        shutdown_thread.start()
        return jsonify({'success': True})
    except Exception as e:
        log_console_out("Error: не удалось сохранить настройки", "webs")
        exception_handler(type(e), e, e.__traceback__, "webs")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/logout')
def logout():
    response = make_response('', 401)  # Код 401 = Unauthorized
    response.headers['WWW-Authenticate'] = 'Basic realm="Login Required"'
    return response


if __name__ == "__main__":
    if not os.path.exists(config_path):
        create_confgi_ini()

    server_process = multiprocessing.Process(target=ftp_connect)
    server_process.daemon = True
    server_process.start()
    webserver()
