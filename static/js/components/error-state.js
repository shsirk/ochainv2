export function showError(container, message, retryFn) {
    clearError(container);
    const div = document.createElement('div');
    div.className = 'error-state';
    div.innerHTML = `<div class="error-icon">&#9888;</div><div class="error-msg">${message}</div>`;
    if (retryFn) {
        const btn = document.createElement('button');
        btn.className = 'error-retry';
        btn.textContent = 'Retry';
        btn.addEventListener('click', retryFn);
        div.appendChild(btn);
    }
    container.appendChild(div);
}

export function clearError(container) {
    container.querySelector('.error-state')?.remove();
}
