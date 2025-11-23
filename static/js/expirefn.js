function showNotification(message, type) {
	const notification = document.getElementById('taskNotification');
	notification.innerHTML = message;
	notification.className = `notification ${type}`;
	notification.style.display = 'block';

	setTimeout(() => {
		notification.style.display = 'none';
	}, 3000);
}

function toggleTask(checkbox, serialNumber, fnSerial) {
	fetch('/toggle_task', {
		method: 'POST',
		headers: {
			'Content-Type': 'application/x-www-form-urlencoded',
		},
		body: `serialNumber=${serialNumber}&fnSerial=${fnSerial}&checked=${checkbox.checked}`
	})
	.then(response => response.json())
	.then(data => {
		if (data.status === 'success') {
			showNotification('Информация о создании задачи успешно обновлена', 'success');
			// Если галочка "показать отмеченные" не стоит и мы отметили запись
			if (!document.querySelector('.marked-filter input[type="checkbox"]').checked && checkbox.checked) {
				// Находим родительский элемент записи и удаляем его
				checkbox.closest('.record').remove();

				const counter = document.querySelector('.record-counter');
				let count = parseInt(counter.textContent, 10);
				if (!isNaN(count) && count > 0) {
					counter.textContent = count - 1;
				}
			}
		} 
		else {
			checkbox.checked = !checkbox.checked;
			showNotification('Функция заблокирована интеграцией Битрикс24', 'error');
		}
	});
}

function toggleMarkedOnly(checkbox) {
	document.cookie = `show_marked=${checkbox.checked}; path=/; max-age=${60*60*24*180}`;
	window.location.href = `/expire_fn?show_marked=${checkbox.checked}&start_date=${START_DATE}&end_date=${END_DATE}`;
}

function loadMarkedOnlyState() {
	const cookies = document.cookie.split(';');
	for(let cookie of cookies) {
		const [name, value] = cookie.trim().split('=');
		if(name === 'show_marked') {
			const checkbox = document.querySelector('.marked-filter input[type="checkbox"]');
			checkbox.checked = value === 'true';
			break;
		}
	}
}

document.addEventListener('DOMContentLoaded', loadMarkedOnlyState);

function copyRecordToClipboard(element) {
	const cells = Array.from(element.children).slice(0, -2); // Исключаем чекбокс и скрытый адрес
	const addressElement = element.querySelector('.address-data');
	const address = addressElement ? addressElement.textContent.trim() : 'Неизвестно';
	
	// Собираем данные из ячеек
	const client = cells[0].textContent.trim();
	const serialNumber = cells[1].textContent.trim();
	const rnm = cells[2].textContent.trim();
	const fnSerial = cells[3].textContent.trim();
	const organizationName = cells[4].textContent.trim();
	const inn = cells[5].textContent.trim();
	const dateTimeEnd = cells[6].textContent.trim();
	
	// Формируем текст для копирования с нужным порядком полей
	const text = 
		`Клиент: ${client}\n\n` +
		`Серийный номер: ${serialNumber}\n` +
		`РНМ: ${rnm}\n` +
		`Номер ФН: ${fnSerial}\n\n` +
		`Юр.лицо: ${organizationName}\n` +
		`ИНН: ${inn}\n` +
		`Адрес места расчётов: ${address}\n\n` +
		`Дата окончания: ${dateTimeEnd}`;

	// Проверяем доступность Clipboard API
	if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
		navigator.clipboard.writeText(text)
			.then(showSuccess)
			.catch(() => fallbackCopy(text));
	} else {
		fallbackCopy(text);
	}

	function showSuccess() {
		element.style.backgroundColor = '#8dcc93';
		setTimeout(() => {
			element.style.backgroundColor = '';
		}, 200);

		const notification = document.getElementById('notification');
		notification.style.display = 'block';
		setTimeout(() => {
			notification.style.display = 'none';
		}, 850);
	}

	function fallbackCopy(text) {
		const textarea = document.createElement('textarea');
		textarea.value = text;
		textarea.style.position = 'fixed';  // Предотвращаем прокрутку до элемента
		textarea.style.opacity = '0';       // Делаем невидимым
		document.body.appendChild(textarea);
		textarea.select();
		try {
			document.execCommand('copy');
			showSuccess();
		} catch (err) {
			console.error('Ошибка копирования:', err);
		}
		document.body.removeChild(textarea);
	}
}

let currentUrlRms = '';

function showClientModal(element, urlRms, serverName, address) {
	currentUrlRms = urlRms;
	document.getElementById('urlRms').textContent = urlRms;
	document.getElementById('serverName').value = serverName || '';
	document.getElementById('addressText').textContent = address ? address : 'Неизвестно';
	document.getElementById('clientModal').style.display = 'block';
}

function closeModal() {
	document.getElementById('clientModal').style.display = 'none';
}

function saveClientName() {
	const newServerName = document.getElementById('serverName').value;
	
	fetch('/edit_client_name', {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
		},
		body: JSON.stringify({
			url_rms: currentUrlRms,
			server_name: newServerName
		})
	})
	.then(response => response.json())
	.then(data => {
		if (data.success) {
			location.reload(); // Перезагружаем страницу для отображения изменений
		} else {
			alert('Ошибка при сохранении: ' + data.error);
		}
	});
	
	closeModal();
}        