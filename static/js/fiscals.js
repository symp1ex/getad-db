// Объект для хранения текущего направления сортировки каждого столбца
const sortDirections = {};

function sortTable(columnIndex) {
	var table = document.getElementById("data-table");
	var tbody = table.tBodies[0];
	var rows = Array.from(tbody.rows);
	
	// Получаем или устанавливаем направление сортировки для этого столбца
	// true = по возрастанию, false = по убыванию
	sortDirections[columnIndex] = !sortDirections[columnIndex];
	const isAscending = sortDirections[columnIndex];
	
	// Сортировка массива строк
	rows.sort(function(a, b) {
		var aValue = a.getElementsByTagName("td")[columnIndex].innerHTML.toLowerCase();
		var bValue = b.getElementsByTagName("td")[columnIndex].innerHTML.toLowerCase();
		
		if (isAscending) {
			return aValue > bValue ? 1 : -1;
		} else {
			return aValue < bValue ? 1 : -1;
		}
	});
	
	// Перемещение строк в таблице в соответствии с отсортированным массивом
	rows.forEach(function(row) {
		tbody.appendChild(row);
	});
	
	updateRowCounter(); // Обновляем счётчик строк после сортировки
}

function updateRowCounter() {
	var table = document.getElementById("data-table");
	var rowCount = table.rows.length - 1; // Учитываем только строки данных, исключая заголовок
	document.getElementById("row-counter").innerText = " " + rowCount;
}

function setColumnClickHandlers() {
    var headers = document.querySelectorAll("th");
    headers.forEach(function(header, index) {
        if (index === 0) return; // Пропускаем столбец с чекбоксами
        header.addEventListener("click", function() {
            sortTable(index);
        });
    });
}

window.addEventListener("load", function() {
    setColumnClickHandlers();
});

function downloadTXT(data, filename) {
	var text = data.toString();
	var blob = new Blob([text], { type: "text/plain" });
	var url = URL.createObjectURL(blob);
	var a = document.createElement("a");
	a.href = url;
	a.download = filename;
	document.body.appendChild(a);
	a.click();
	document.body.removeChild(a);
	URL.revokeObjectURL(url);
}

// Функция для генерации и загрузки CSV-файла
function downloadCSV() {
	var table = document.getElementById("data-table");
	var rows = table.rows;
	var csvContent = "data:text/csv;charset=utf-8,";

	// Получаем только видимые заголовки (начиная с индекса 1, чтобы пропустить столбец с чекбоксами)
	var headers = [];
	var visibleColumnIndexes = []; // Сохраняем индексы видимых столбцов
	for (var j = 1; j < rows[0].cells.length; j++) {
		var cell = rows[0].cells[j];
		if (!cell.classList.contains('hidden-column')) {
			// Извлекаем текст заголовка без иконки фильтра
			var headerText = cell.textContent.replace(/[▼⌘⯆]/g, '').trim();
			headers.push(headerText);
			visibleColumnIndexes.push(j);
		}
	}
	csvContent += headers.join(";") + "\r\n";;

	// Добавляем выбранные строки данных, только для видимых столбцов
	for (var i = 1; i < rows.length; i++) {
		var row = rows[i];
		var checkbox = row.querySelector('input[type="checkbox"]');
		if (checkbox && checkbox.checked) {
			var rowData = [];
			// Используем только индексы видимых столбцов
			visibleColumnIndexes.forEach(function(columnIndex) {
				rowData.push(row.cells[columnIndex].innerText);
			});
			csvContent += rowData.join(";") + "\r\n";
		}
	}

	var encodedUri = encodeURI(csvContent);
	var link = document.createElement("a");
	link.setAttribute("href", encodedUri);
	link.setAttribute("download", "selected_data.csv");
	document.body.appendChild(link);
	link.click();
	document.body.removeChild(link);
}

function toggleCheckboxes(masterCheckbox) {
	var checkboxes = document.querySelectorAll('input[type="checkbox"]');
	checkboxes.forEach(function(checkbox) {
		checkbox.checked = masterCheckbox.checked;
	});
}

window.onload = function() {
	updateRowCounter(); // Обновляем счётчик строк при загрузке страницы
};
// Добавляем новые функции для работы с контекстным меню
document.addEventListener('DOMContentLoaded', function() {
	const table = document.getElementById('data-table');
	const headers = table.getElementsByTagName('th');
	
	// Создаем контекстное меню
	const contextMenu = document.createElement('div');
	contextMenu.className = 'context-menu';
	document.body.appendChild(contextMenu);

// Функции для работы с cookies
function setCookie(name, value, days) {
	const expires = new Date();
	expires.setTime(expires.getTime() + (days * 24 * 60 * 60 * 1000));
	document.cookie = name + '=' + JSON.stringify(value) + ';expires=' + expires.toUTCString() + ';path=/';
}

function getCookie(name) {
	const nameEQ = name + "=";
	const ca = document.cookie.split(';');
	for(let i = 0; i < ca.length; i++) {
		let c = ca[i];
		while (c.charAt(0) === ' ') c = c.substring(1, c.length);
		if (c.indexOf(nameEQ) === 0) {
			try {
				return JSON.parse(c.substring(nameEQ.length, c.length));
			} catch (e) {
				return null;
			}
		}
	}
	return null;
}

// Сохраняем состояние видимости столбцов
let columnStates = {};
const savedStates = getCookie('columnStates');

			// Обработчик правого клика по заголовку
Array.from(headers).forEach((header, index) => {
	if (index === 0) return; // Пропускаем столбец с чекбоксами
	
	header.addEventListener('contextmenu', function(e) {
		e.preventDefault();
		showContextMenu(e.pageX, e.pageY);
	});

	// Инициализируем состояния столбцов
	columnStates[index] = true;
});

Array.from(headers).forEach((header, index) => {
	if (index === 0) return; // Пропускаем столбец с чекбоксами
	
	const columnName = header.textContent.replace(/[▼⌘⯆]/g, '').trim();
	
	// Используем сохраненное состояние или значение по умолчанию
	if (savedStates && savedStates[index] !== undefined) {
		columnStates[index] = savedStates[index];
	} else {
		columnStates[index] = defaultVisibleColumns.includes(columnName);
	}
	
	// Применяем начальное состояние видимости
	const cells = table.querySelectorAll(`th:nth-child(${index + 1}), td:nth-child(${index + 1})`);
	cells.forEach(cell => {
		cell.classList.toggle('hidden-column', !columnStates[index]);
	});

	// Добавляем обработчик правого клика
	header.addEventListener('contextmenu', function(e) {
		e.preventDefault();
		showContextMenu(e.pageX, e.pageY);
	});
});

// Функция создания и показа контекстного меню
function showContextMenu(x, y) {
	contextMenu.innerHTML = '';
	
	// Добавляем опцию "Выбрать всё"
	const selectAllItem = document.createElement('div');
	selectAllItem.className = 'context-menu-item';
	
	const selectAllCheckbox = document.createElement('input');
	selectAllCheckbox.type = 'checkbox';
	selectAllCheckbox.id = 'select-all';
	selectAllCheckbox.checked = Object.values(columnStates).every(state => state);
	
	const selectAllLabel = document.createElement('label');
	selectAllLabel.appendChild(selectAllCheckbox);
	selectAllLabel.appendChild(document.createTextNode(' Выбрать всё'));
	
	selectAllItem.appendChild(selectAllLabel);
	contextMenu.appendChild(selectAllItem);

	// Добавляем разделитель
	const divider = document.createElement('hr');
	contextMenu.appendChild(divider);
	
	// Добавляем чекбоксы для каждого столбца
	Array.from(headers).forEach((header, index) => {
		if (index === 0) return; // Пропускаем столбец с чекбоксами
		
		const item = document.createElement('div');
		item.className = 'context-menu-item';
		
		const checkbox = document.createElement('input');
		checkbox.type = 'checkbox';
		checkbox.checked = columnStates[index];
		checkbox.dataset.columnIndex = index;
		
		const label = document.createElement('label');
		label.appendChild(checkbox);
		// Получаем только текст заголовка, исключая иконку
		const headerText = header.textContent.replace('⯆', '').trim();
		label.appendChild(document.createTextNode(' ' + headerText));
		
		item.appendChild(label);
		contextMenu.appendChild(item);
	});

	// Добавляем кнопки
	const buttonContainer = document.createElement('div');
	buttonContainer.className = 'context-menu-buttons';
	
	const okButton = document.createElement('button');
	okButton.textContent = 'ОК';
	okButton.onclick = applyColumnVisibility;
	
	const cancelButton = document.createElement('button');
	cancelButton.textContent = 'Отмена';
	cancelButton.onclick = hideContextMenu;
	
	buttonContainer.appendChild(okButton);
	buttonContainer.appendChild(cancelButton);
	contextMenu.appendChild(buttonContainer);

	// Обработчик для чекбокса "Выбрать всё"
	selectAllCheckbox.addEventListener('change', function() {
		const checkboxes = contextMenu.querySelectorAll('input[type="checkbox"]');
		checkboxes.forEach(checkbox => {
			if (checkbox !== selectAllCheckbox) {
				checkbox.checked = selectAllCheckbox.checked;
			}
		});
	});

	// Обработчик для отслеживания состояния всех чекбоксов
	const columnCheckboxes = contextMenu.querySelectorAll('input[type="checkbox"]:not(#select-all)');
	columnCheckboxes.forEach(checkbox => {
		checkbox.addEventListener('change', function() {
			const allChecked = Array.from(columnCheckboxes).every(cb => cb.checked);
			selectAllCheckbox.checked = allChecked;
		});
	});

	// Позиционируем меню
	contextMenu.style.display = 'block';
	contextMenu.style.left = x + 'px';
	contextMenu.style.top = y + 'px';
}

// Модифицируем функцию применения видимости столбцов
function applyColumnVisibility() {
	const checkboxes = contextMenu.querySelectorAll('input[type="checkbox"]:not(#select-all)');
	
	checkboxes.forEach(checkbox => {
		const columnIndex = parseInt(checkbox.dataset.columnIndex);
		columnStates[columnIndex] = checkbox.checked;
		
		const cells = table.querySelectorAll(`th:nth-child(${columnIndex + 1}), td:nth-child(${columnIndex + 1})`);
		cells.forEach(cell => {
			cell.classList.toggle('hidden-column', !checkbox.checked);
		});
	});
	
	// Сохраняем состояния в cookies на 180 дней
	setCookie('columnStates', columnStates, 180);
	
	hideContextMenu();
}

// Функция скрытия контекстного меню
function hideContextMenu() {
	contextMenu.style.display = 'none';
}

// Скрываем контекстное меню при клике вне его
document.addEventListener('click', function(e) {
	if (!contextMenu.contains(e.target)) {
		hideContextMenu();
	}
});
});

document.addEventListener('DOMContentLoaded', function() {
	// Найти все ячейки с данными о лицензиях
	const licenseCells = document.querySelectorAll('td a[onclick*="downloadTXT"]');
	
	licenseCells.forEach(function(cell) {
		// Получить данные о лицензиях
		const onclick = cell.getAttribute('onclick');
		const licenseData = onclick.split("'")[1]; // Извлекаем JSON-строку из onclick
		
		try {
			const licenses = JSON.parse(licenseData);
			// Проверяем наличие лицензии 17
			const hasLicense17 = licenses.hasOwnProperty('17');
			
			// Проверяем срок действия лицензии 17, если она есть
			let licenseValid = false;
			if (hasLicense17) {
				const dateUntil = new Date(licenses['17'].dateUntil);
				const today = new Date();
				licenseValid = dateUntil >= today;
			}
			
			// Создаем элемент индикатора
			const indicator = document.createElement('span');
			indicator.style.marginLeft = '5px';
			
			if (hasLicense17 && licenseValid) {
				// Лицензия 17 существует и не просрочена
				indicator.innerHTML = '✔️';
				indicator.style.color = 'green';
			} else {
				// Лицензия 17 отсутствует или просрочена
				indicator.innerHTML = '❌';
				indicator.style.color = 'red';
			}
			
			// Добавляем индикатор рядом с ссылкой
			cell.parentNode.appendChild(indicator);
		} catch (e) {
			console.error('Ошибка при разборе данных о лицензиях:', e);
		}
	});
});

// Объект для хранения текущих фильтров
const activeFilters = {};
let currentColumnIndex = -1;

// Функция для отображения фильтр-меню
function showFilterMenu(event, columnIndex) {
    event.stopPropagation(); // Предотвращаем всплытие события
    
    const filterMenu = document.getElementById('filter-menu');
    const filterInput = document.getElementById('filter-input');
    const filterType = document.getElementById('filter-type');
    
    // Получаем позицию для меню
    const rect = event.target.getBoundingClientRect();
    
    // Определяем размеры окна и меню
    const windowWidth = window.innerWidth;
    const menuWidth = 250; // Ширина меню из CSS
    
    // Рассчитываем позицию слева
    let leftPos = rect.left;
    
    // Проверяем, не выходит ли меню за правый край экрана
    if (leftPos + menuWidth > windowWidth) {
        // Если выходит, смещаем меню влево на необходимую величину
        leftPos = windowWidth - menuWidth - 20; // 20px - отступ от края
    }
    
    // Применяем позицию
    filterMenu.style.left = leftPos + 'px';
    filterMenu.style.top = (rect.bottom + window.scrollY) + 'px';
    
    // Устанавливаем текущий индекс столбца
    currentColumnIndex = columnIndex;
    
    // Если для этого столбца уже есть фильтр, заполняем поля
    if (activeFilters[columnIndex]) {
        filterInput.value = activeFilters[columnIndex].text;
        filterType.value = activeFilters[columnIndex].type;
    } else {
        filterInput.value = '';
        filterType.value = 'include';
    }
    
    // Показываем меню
    filterMenu.style.display = 'block';
}

// Функция для скрытия фильтр-меню
function hideFilterMenu() {
    const filterMenu = document.getElementById('filter-menu');
    filterMenu.style.display = 'none';
}

// Функция применения фильтра
function applyFilter() {
    const filterText = document.getElementById('filter-input').value.toLowerCase();
    const filterType = document.getElementById('filter-type').value;
    
    if (currentColumnIndex >= 0) {
        // Сохраняем фильтр
        activeFilters[currentColumnIndex] = {
            text: filterText,
            type: filterType
        };
        
        // Применяем все фильтры
        applyAllFilters();
        
        // Помечаем столбец как отфильтрованный
        const headers = document.querySelectorAll('th');
        if (headers[currentColumnIndex]) {
            headers[currentColumnIndex].classList.add('filtered-column');
        }
    }
    
    hideFilterMenu();
    updateRowCounter(); // Обновляем счётчик строк
}

// Функция сброса текущего фильтра
function clearFilter() {
    if (currentColumnIndex >= 0) {
        // Удаляем фильтр
        delete activeFilters[currentColumnIndex];
        
        // Применяем оставшиеся фильтры
        applyAllFilters();
        
        // Удаляем метку с отфильтрованного столбца
        const headers = document.querySelectorAll('th');
        if (headers[currentColumnIndex]) {
            headers[currentColumnIndex].classList.remove('filtered-column');
        }
    }
    
    hideFilterMenu();
    updateRowCounter(); // Обновляем счётчик строк
}

// Функция применения всех активных фильтров
function applyAllFilters() {
    const table = document.getElementById('data-table');
    const rows = table.querySelectorAll('tbody tr');
    
    rows.forEach(row => {
        let shouldShow = true;
        
        // Проверяем каждый активный фильтр
        Object.keys(activeFilters).forEach(columnIndex => {
            const filter = activeFilters[columnIndex];
            const cellText = row.cells[parseInt(columnIndex)].textContent.toLowerCase();
            
            if (filter.type === 'include') {
                // Включающий фильтр
                if (!cellText.includes(filter.text)) {
                    shouldShow = false;
                }
            } else {
                // Исключающий фильтр
                if (cellText.includes(filter.text)) {
                    shouldShow = false;
                }
            }
        });
        
        // Показываем или скрываем строку
        row.classList.toggle('filtered-out', !shouldShow);
    });
}

// Обновляем функцию обновления счётчика строк
function updateRowCounter() {
    var table = document.getElementById("data-table");
    var visibleRows = table.querySelectorAll('tbody tr:not(.filtered-out)').length;
    document.getElementById("row-counter").innerText = " " + visibleRows;
}

// Закрываем фильтр-меню при клике вне его
document.addEventListener('click', function(e) {
    const filterMenu = document.getElementById('filter-menu');
    if (filterMenu && filterMenu.style.display === 'block' && !filterMenu.contains(e.target) && !e.target.classList.contains('filter-icon')) {
        hideFilterMenu();
    }
});