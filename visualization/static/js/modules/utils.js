/**
 * Funciones utilitarias del Dashboard COVID-19
 */

export function formatNumber(num) {
    return new Intl.NumberFormat('es-PE').format(num);
}

export function formatDateTime(isoString) {
    return new Date(isoString).toLocaleString('es-PE', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

export function showConnectionStatus(connected) {
    const indicator = document.getElementById('connection-status');
    if (indicator) {
        indicator.className = connected ? 'status-connected' : 'status-disconnected';
        indicator.title = connected ? 'Conectado - Tiempo real activo' : 'Desconectado';
    }
}

export function showNotification(message) {
    const container = document.getElementById('notifications');
    if (!container) return;

    const notification = document.createElement('div');
    notification.className = 'notification';
    notification.textContent = message;
    container.appendChild(notification);

    setTimeout(() => {
        notification.classList.add('fade-out');
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}

export function filterValidDepts(data) {
    return (data || []).filter(d =>
        d.departamento && d.departamento !== 'Sin especificar'
    );
}

export function filterValidSex(data) {
    return (data || []).filter(d =>
        d.sexo && d.sexo !== 'Sin especificar' && d.sexo.trim() !== ''
    );
}

export function formatDateForInput(dateStr) {
    if (!dateStr || dateStr.length !== 8) return '';
    return `${dateStr.slice(0,4)}-${dateStr.slice(4,6)}-${dateStr.slice(6,8)}`;
}

export function formatDateFromInput(inputValue) {
    if (!inputValue) return null;
    return inputValue.replace(/-/g, '');
}
