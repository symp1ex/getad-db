function switchTab(tabId) {
	document.querySelectorAll('.tab-content').forEach(content => {
		content.classList.remove('active');
	});
	document.querySelectorAll('.tab').forEach(tab => {
		tab.classList.remove('active');
	});

	document.getElementById(tabId).classList.add('active');
	event.currentTarget.classList.add('active');
}

function toggleFiles(folderName) {
	const files = document.querySelectorAll(`.file-item[data-parent="${folderName}"]`);
	const toggleIcon = document.getElementById(`toggle-${folderName}`);

	const isHidden = files[0].style.display === 'none';

	files.forEach(file => {
		file.style.display = isHidden ? 'flex' : 'none';
	});

	if (isHidden) {
		toggleIcon.textContent = '▼';
		toggleIcon.style.transform = 'rotate(0deg)';
	} else {
		toggleIcon.textContent = '▶';
		toggleIcon.style.transform = 'rotate(-90deg)';
	}
}

function showNotification(message, type) {
	const notification = document.getElementById('notification');
	notification.innerHTML = message;
	notification.className = `notification ${type}`;
	notification.style.display = 'block';

	setTimeout(() => {
		notification.style.display = 'none';
	}, 3000);
}

document.getElementById('settingsForm').addEventListener('submit', function(e) {
	e.preventDefault();

	const formData = new FormData(this);
	const settings = {};

	// Добавим проверку для чек-бокса reference
	if (!formData.has('db-update.reference')) {
		formData.append('db-update.reference', '0');
	}

	formData.forEach((value, key) => {
		const [section, option] = key.split('.');
		if (!settings[section]) settings[section] = {};
		settings[section][option] = value;
	});

	fetch('/save_settings', {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
		},
		body: JSON.stringify(settings)
	})
	.then(response => response.json())
	.then(data => {
		if (data.success) {
			showNotification('Настройки успешно сохранены<br>Сервер будет перезапущен', 'success');
			setTimeout(() => location.reload(), 3000);
		} else {
			showNotification('Ошибка при сохранении настроек', 'error');
		}
	})
	.catch(error => {
		showNotification('Ошибка при сохранении настроек', 'error');
		console.error('Error:', error);
	});
});

document.getElementById('integrationsForm').addEventListener('submit', function(e) {
	e.preventDefault();

	const formData = new FormData(this);
	const settings = {};
	const responsibleId = document.getElementById('responsible-employee').value;
	const observersId = document.getElementById('observers-group').value;

	// Добавляем проверку для чек-бокса enabled
	if (!formData.has('bitrix24.enabled')) {
		formData.append('bitrix24.enabled', '0');
	}

	formData.forEach((value, key) => {
		const [section, option] = key.split('.');
		if (!settings[section]) settings[section] = {};
		settings[section][option] = value;
	});

	fetch('/save_settings', {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
		},
		body: JSON.stringify({
			settings: settings,
			responsibleId: responsibleId, 
			observersId: observersId,
		})
	})
	.then(response => response.json())
	.then(data => {
		if (data.success) {
			showNotification('Настройки интеграции успешно сохранены', 'success');
			setTimeout(() => location.reload(), 3000);
		} else {
			showNotification('Ошибка при сохранении настроек интеграции', 'error');
		}
	})
	.catch(error => {
		showNotification('Ошибка при сохранении настроек', 'error');
		console.error('Error:', error);
	});
});