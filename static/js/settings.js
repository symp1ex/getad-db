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

    if (!formData.has('ftp-connect.ftp_update')) {
		formData.append('ftp-connect.ftp_update', '0');
	}

    if (!formData.has('ftp-connect.ftp_backup')) {
		formData.append('ftp-connect.ftp_backup', '0');
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

// Функционал для управления API-ключами
document.addEventListener('DOMContentLoaded', function() {
    const apiKeyModal = document.getElementById('apiKeyModal');
    const createApiKeyBtn = document.getElementById('createApiKey');
    const closeModal = document.querySelector('.close-modal');
    const createApiKeySubmit = document.getElementById('createApiKeySubmit');
    const showDeletedKeysCheckbox = document.getElementById('show-deleted-keys');

    // Загрузка API-ключей при открытии вкладки API
    document.querySelector('.tab[onclick="switchTab(\'api\')"]').addEventListener('click', function() {
        loadApiKeys();
    });

    // Показать/скрыть удаленные API-ключи
    if (showDeletedKeysCheckbox) {
        showDeletedKeysCheckbox.addEventListener('change', function() {
            loadApiKeys();
        });
    }

    // Открыть модальное окно
    if (createApiKeyBtn) {
        createApiKeyBtn.addEventListener('click', function() {
            apiKeyModal.style.display = 'block';
        });
    }

    // Закрыть модальное окно
    if (closeModal) {
        closeModal.addEventListener('click', function() {
            apiKeyModal.style.display = 'none';
            document.getElementById('api-key-name').value = '';
        });
    }

    // Создать API-ключ
    if (createApiKeySubmit) {
        createApiKeySubmit.addEventListener('click', function() {
            const name = document.getElementById('api-key-name').value;
            const privilege = document.getElementById('api-key-privilege').value;

            if (!name) {
                showNotification('Имя не может быть пустым', 'error');
                return;
            }

            fetch('/add_api_key', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    name: name,
                    admin_tag: parseInt(privilege)
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showNotification(`API-ключ успешно создан для "${name}"`, 'success');
                    apiKeyModal.style.display = 'none';
                    document.getElementById('api-key-name').value = '';
                    
                    // Показываем новый API-ключ
                    const apiKeyResult = document.createElement('div');
                    apiKeyResult.id = 'api-key-created';
                    apiKeyResult.innerHTML = `
                        <strong>Новый API-ключ:</strong> 
                        <span id="new-api-key">${data.api_key}</span>
                        <button class="copy-button" onclick="copyToClipboard('${data.api_key}')">Копировать</button>
                        <p><small>Сохраните этот ключ. После закрытия уведомления полный ключ больше не будет показан.</small></p>
                    `;
                    document.querySelector('.api-controls').appendChild(apiKeyResult);
                    apiKeyResult.style.display = 'block';

                    setTimeout(() => {
                        apiKeyResult.style.display = 'none';
                        setTimeout(() => apiKeyResult.remove(), 300);
                        loadApiKeys();
                    }, 10000);
                } else {
                    showNotification(data.error || 'Ошибка при создании API-ключа', 'error');
                }
            })
            .catch(error => {
                showNotification('Ошибка при создании API-ключа', 'error');
                console.error('Error:', error);
            });
        });
    }

    // Загрузка списка API-ключей
    function loadApiKeys() {
        const showDeleted = showDeletedKeysCheckbox && showDeletedKeysCheckbox.checked;
        
        fetch('/get_api_keys?show_deleted=' + showDeleted)
            .then(response => response.json())
            .then(data => {
                const tableBody = document.querySelector('#apiKeysTable tbody');
                tableBody.innerHTML = '';

                if (data.keys && data.keys.length > 0) {
                    data.keys.forEach(key => {
                        const row = document.createElement('tr');
                        if (key.active === 0) {
                            row.classList.add('api-key-inactive');
                        }
                        
                        // Форматируем API-ключ, показывая начало и конец
                        const apiKeyStart = key.api_key.substring(0, 8);
                        const apiKeyEnd = key.api_key.substring(key.api_key.length - 8);
                        const formattedApiKey = `${apiKeyStart}...${apiKeyEnd}`;
                        
                        row.innerHTML = `
                            <td>${key.name}</td>
                            <td>${formattedApiKey}</td>
                            <td>${key.admin_tag === 1 ? 'Администратор' : 'Пользователь'}</td>
                            <td>
                                ${key.active === 1 
                                    ? `<button class="action-button" onclick="toggleApiKey('${key.api_key}', 0, '${key.name}')">❌</button>` 
                                    : `<button class="action-button restore" onclick="toggleApiKey('${key.api_key}', 1, '${key.name}')">✔️</button>`}
                            </td>
                        `;
                        tableBody.appendChild(row);
                    });
                } else {
                    const row = document.createElement('tr');
                    row.innerHTML = '<td colspan="4" style="text-align: center;">Нет доступных API-ключей</td>';
                    tableBody.appendChild(row);
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showNotification('Ошибка при загрузке API-ключей', 'error');
            });
    }
});

// Функция для копирования текста в буфер обмена
function copyToClipboard(text) {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand('copy');
    document.body.removeChild(textarea);
    showNotification('API-ключ скопирован в буфер обмена', 'success');
}

// Функция для активации/деактивации API-ключа
function toggleApiKey(apiKey, active, name) {
    fetch('/toggle_api_key', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            api_key: apiKey,
            active: active,
            name: name
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification(active === 1 ? 'API-ключ восстановлен' : 'API-ключ деактивирован', 'success');
            // Перезагрузка списка ключей
            document.querySelector('.tab[onclick="switchTab(\'api\')"]').click();
        } else {
            showNotification(data.error || 'Ошибка при изменении статуса API-ключа', 'error');
        }
    })
    .catch(error => {
        showNotification('Ошибка при изменении статуса API-ключа', 'error');
        console.error('Error:', error);
    });
}