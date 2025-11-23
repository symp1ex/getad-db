function deleteFR() {
	const form = document.querySelector('.delete-form');
	const formData = new FormData(form);

	fetch('/del_fr', {
		method: 'POST',
		body: formData
	})
	.then(response => response.json())
	.then(data => {
		showModal(data);
	})
	.catch(error => {
		console.error('Ошибка:', error);
	});
}

function showModal(messages) {
	const modal = document.createElement('div');
	modal.className = 'modal';

	const messageElement = document.createElement('div');
	messageElement.innerHTML = messages.join('<br>');

	const button = document.createElement('button');
	button.textContent = 'OK';
	button.className = 'button button-primary';
	button.onclick = () => document.body.removeChild(modal);

	modal.appendChild(messageElement);
	modal.appendChild(document.createElement('br'));
	modal.appendChild(button);
	document.body.appendChild(modal);
}

function submitForm(action) {
	event.preventDefault(); // Предотвращаем стандартную отправку формы
	const form = document.getElementById('searchForm');
	form.action = action;
	form.submit();
}