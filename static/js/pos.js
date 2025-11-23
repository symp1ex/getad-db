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