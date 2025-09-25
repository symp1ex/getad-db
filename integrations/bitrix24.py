import core.sys_manager
import core.dbmanagment
import core.logger
from datetime import datetime, timedelta
import threading
import requests
import time

db_update = core.dbmanagment.DbUpdate()
db_queries = core.dbmanagment.DbQueries()

class Bitrix24Task(core.sys_manager.ResourceManagement):
    bitrix_json_name = "bitrix24.json"
    bitrix_json_data = {"enabled": 0, "webhook_url": "", "count_attempts": 5, "timeout": 15}

    def __init__(self):
        super().__init__()
        self.bitrix_json = self.read_json_file("source", self.bitrix_json_name, self.bitrix_json_data, create=True)
        if self.bitrix_json == False:
            self.bitrix_json = self.read_json_file("source", self.bitrix_json_name)

        try: self.enabled = int(self.bitrix_json.get("enabled", 0))
        except: self.enabled = 0

        self.create_task_process_flag = 0
        self.webhook_url = self.bitrix_json.get("webhook_url")

        try: self.count_attempts = int(self.bitrix_json.get("count_attempts", 5))
        except: self.count_attempts = 5

        try: self.timeout = int(self.bitrix_json.get("timeout", 15))
        except: self.timeout = 15

        self.responsible_employees = None
        self.groups_observers = None
        self.author_task = None

    def get_bitrix_employees(self):
        employees = []
        start = 0
        try:
            while True:
                data = {
                    "FILTER": {"ACTIVE": "Y"},
                    "SORT": "ID",
                    "ORDER": "asc",
                    "start": start
                }
                attempt = 0
                while attempt < self.count_attempts:
                    try:
                        response = requests.post(
                            f"{self.webhook_url}user.get",
                            json=data,
                            headers={
                                "Content-Type": "application/json",
                                "Accept": "application/json"
                            }
                        )
                        break
                    except Exception:
                        core.logger.bitrix24.warning(
                            f"Ошибка при запросе ({attempt+1}) к списку сотрудников Bitrix24, "
                            f"следующая попытка через '{self.timeout}' секунд", exc_info=True)
                    attempt += 1
                    if attempt < self.count_attempts:
                        time.sleep(self.timeout)
                    else:
                        break

                core.logger.bitrix24.info(f"Отправлен запрос на получение списка сотрудников Битрикс24")
                core.logger.bitrix24.info(f"Status Code: {response.status_code}")
                core.logger.bitrix24.debug("Response:")
                core.logger.bitrix24.debug(response.text)

                if response.status_code != 200:
                    break
                result = response.json().get('result', [])
                if not result:
                    break
                employees.extend(result)
                if len(result) < 50:  # 50 — стандартный лимит
                    break
                start += 50
                time.sleep(15)
            return employees
        except Exception:
            core.logger.bitrix24.error("Не удалось сделать запрос к списку сотрудников Битрикс24", exc_info=True)
            return False

    def get_bitrix_projects(self):
        group_project = []
        try:
            data = {
                "FILTER": {
                    "ACTIVE": "Y"  # Только активные группы
                },
                "SORT": "ID",
                "ORDER": "asc"
            }
            attempt = 0
            while attempt < self.count_attempts:
                try:
                    response = requests.post(
                        f"{self.webhook_url}sonet_group.get",
                        json=data,
                        headers={
                            "Content-Type": "application/json",
                            "Accept": "application/json"
                        }
                    )
                    break
                except Exception:
                    core.logger.bitrix24.warning(
                        f"Ошибка при запросе ({attempt + 1}) к списку проектов Bitrix24, "
                        f"следующая попытка через '{self.timeout}' секунд", exc_info=True)
                attempt += 1
                if attempt < self.count_attempts:
                    time.sleep(self.timeout)
                else:
                    break

            core.logger.bitrix24.info(f"Отправлен запрос на получение списка рабочих групп в Битрикс24")
            core.logger.bitrix24.info(f"Status Code: {response.status_code}")
            core.logger.bitrix24.debug("Response:")
            core.logger.bitrix24.debug(f"{response.text}")

            if response.status_code == 200:
                result = response.json().get('result', [])
                group_project.extend(result)
                return group_project

            return False
        except Exception:
            core.logger.bitrix24.error(f"Не удалось сделать запрос к списку проектов Битрикс24", exc_info=True)
            return False

    def create_task_sale_fn(self, task_data):
        try:
            client = task_data.get('client', None)
            serial_number = task_data.get('serialNumber', None)
            rnm = task_data.get('RNM', None)
            fn_serial = task_data.get('fn_serial', None)
            organization_name = task_data.get('organizationName', None)
            inn = task_data.get('INN', None)
            datetime_end = task_data.get('dateTime_end', None)
        except Exception:
            core.logger.bitrix24.error(
                f"Не удалось импортировать необходимые данные о ККТ для создания задачи", exc_info=True)
            return

        try:
            task_title = f"Кончается ФН {datetime_end}, {client}"

            task_description = (f"Клиент: {client}\n"
                                f"Серийный номер: {serial_number}\n"
                                f"РНМ: {rnm}\n"
                                f"Номер ФН: {fn_serial}\n"
                                f"Юр.лицо: {organization_name}\n"
                                f"ИНН: {inn}\n"
                                f"Дата окончания: {datetime_end}\n")


            task_fields = {
                'TITLE': task_title,
                'DESCRIPTION': task_description,
                'RESPONSIBLE_ID': self.responsible_employees,
                'CREATED_BY': self.author_task
            }

            # Добавляем группу к задаче, если указана
            if self.groups_observers:
                task_fields['GROUP_ID'] = self.groups_observers
            
            task_data = {'fields': task_fields}
            
            attempt = 0
            while attempt < self.count_attempts:
                try:
                    response = requests.post(
                        f"{self.webhook_url}tasks.task.add",
                        json=task_data,
                        headers={
                            "Content-Type": "application/json",
                            "Accept": "application/json"
                        }
                    )
                    break
                except Exception:
                    core.logger.bitrix24.warning(
                        f"Ошибка при запросе ({attempt + 1}) на создание задачи Битрикс24, "
                        f"следующая попытка через '{self.timeout}' секунд", exc_info=True)
                attempt += 1
                if attempt < self.count_attempts:
                    time.sleep(self.timeout)
                else:
                    break
            
            core.logger.bitrix24.info(f"Отправлен запрос на создание задачи для клиента: '{client}'")
            core.logger.bitrix24.info(f"Status Code: {response.status_code}")
            core.logger.bitrix24.debug("Response:")
            core.logger.bitrix24.debug(response.json())
            core.logger.bitrix24.debug(f"Задача успешно создана:\n{task_title}\n\n{task_description}")
            if response.status_code == 200:
                db_queries.toggle_task(serial_number, fn_serial, True, self)
        except Exception:
            core.logger.bitrix24.error(f"Создание задачи задачи в Битрикс24 завершилось неудачей", exc_info=True)

    def create_task_process(self):
        core.logger.bitrix24.info(f"Запущен процесс создания задач в Битрикс24")

        try:
            self.responsible_employees = int(db_queries.get_bitrix_contractors(
                "bitrix_employees", "responsible", "LAST_NAME")[0])
            self.groups_observers = int(db_queries.get_bitrix_contractors(
                "bitrix_projects", "observers", "SUBJECT_NAME")[0])
        except:
            core.logger.bitrix24.error(
                f"Не удалось получить ID ответственного или группы наблюдателей, процесс будет прерван", exc_info=True)
            return

        try:
            self.author_task = int(self.webhook_url.split('/rest/')[1].split('/')[0])
            core.logger.bitrix24.debug(f"Получен ID для автора задач: '{self.author_task}'")
        except:
            core.logger.bitrix24.error(f"Не удалось получить ID для автора задач, процесс будет прерван", exc_info=True)
            return

        try:
            while True:
                core.logger.bitrix24.info(
                    f"Производится поиск клиентов, которым потребуется замена ФН в ближайшие '30' дней")

                start_date = datetime.now().date()
                end_date = start_date + timedelta(days=30)
                fn_task_list = db_queries.get_expire_fn(start_date, end_date, show_marked=False)

                if len(fn_task_list) == 0:
                    core.logger.bitrix24.info(
                        f"Не найдено клиентов которым требуется замена ФН, следующая проверка через '24' часа")
                    time.sleep(86400)
                    continue

                core.logger.bitrix24.info(
                    f"Найдено ({len(fn_task_list)}) клиентов, которым потребуется замена ФН в ближайшие '30' дней")
                for task_data in fn_task_list:
                    self.create_task_sale_fn(task_data)
                    time.sleep(600)

                core.logger.bitrix24.info(f"Создание задач в Битрикс24 завершено, следующая проверка через '24' часа")
                time.sleep(86400)
        except Exception:
            core.logger.bitrix24.error(f"Не удалось запустить процесс создания задач в Битрикс24:", exc_info=True)

    def task_manager(self):
        if not self.enabled == 1:
            return

        core.logger.bitrix24.info(f"Запущен процесс обновления базы сотрудников Битрикс24")
        try:
            while True:
                employees_list = self.get_bitrix_employees()
                employees_table_success = db_update.update_bitrix_employees_table(employees_list)
                time.sleep(10) # тайм-аут между запросами к api bitrix
                observers_list = self.get_bitrix_projects()
                projects_table_success = db_update.update_bitrix_projects_table(observers_list)

                if not (employees_table_success == False) or (projects_table_success == False):
                    core.logger.bitrix24.info(
                        f"Процесс обновления базы сотрудников Битрикс24 завершён, следующее обновление через '4' часа")

                if self.create_task_process_flag == 0:
                    update_clients_info_thread = threading.Thread(target=self.create_task_process, daemon=True)
                    update_clients_info_thread.start()
                    self.create_task_process_flag = 1

                time.sleep(14400)
        except Exception:
            core.logger.bitrix24.error(
                f"Произошло нештатное прерывание основного потока интеграции с Битрикс24:", exc_info=True)
