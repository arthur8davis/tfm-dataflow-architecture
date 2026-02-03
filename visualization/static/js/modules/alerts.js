/**
 * Sistema de Alertas del Dashboard COVID-19
 */

import { state } from './config.js';
import { formatNumber, formatDateTime, formatDateForInput, formatDateFromInput } from './utils.js';

export function showAlertNotification(alert) {
    const container = document.getElementById('notifications');
    if (!container) return;

    const notification = document.createElement('div');
    notification.className = `notification alert-notification ${alert.level}`;
    notification.innerHTML = `
        <strong>⚠️ ${alert.name}</strong><br>
        <span>${formatNumber(alert.current_value)} ${alert.context ? `(${alert.context})` : ''}</span>
    `;
    notification.style.background = alert.level === 'critical'
        ? 'rgba(231, 76, 60, 0.95)'
        : 'rgba(243, 156, 18, 0.95)';

    container.appendChild(notification);

    setTimeout(() => {
        notification.classList.add('fade-out');
        setTimeout(() => notification.remove(), 300);
    }, 5000);
}

export function renderActiveAlerts() {
    const container = document.getElementById('active-alerts');
    if (!container) return;

    if (!state.activeAlerts || state.activeAlerts.length === 0) {
        container.innerHTML = '<p class="no-alerts">Sin alertas activas</p>';
        return;
    }

    container.innerHTML = state.activeAlerts.map(alert => `
        <div class="alert-item ${alert.level}" data-alert-id="${alert.id}">
            <span class="alert-icon">${alert.level === 'critical' ? '🚨' : '⚠️'}</span>
            <div class="alert-content">
                <div class="alert-title">${alert.name}</div>
                <div class="alert-details">
                    Valor actual: <span class="alert-value">${formatNumber(alert.current_value)}</span>
                    (umbral: <span class="alert-threshold">${formatNumber(alert.threshold)}</span>)
                    ${alert.context ? `<br><span class="alert-context">${alert.context}</span>` : ''}
                    ${alert.date ? `<br><span class="alert-date-range">Fecha: ${alert.date}</span>` : ''}
                </div>
                <div class="alert-time">${formatDateTime(alert.timestamp)}</div>
            </div>
            <button class="btn-dismiss" onclick="dismissAlert('${alert.id}')">Descartar</button>
        </div>
    `).join('');
}

export function renderAlertsConfig() {
    const container = document.getElementById('thresholds-config');
    if (!container) return;

    let html = `
        <div class="config-global-date">
            <label>Fecha para alertas:</label>
            <input type="date" id="alerts-global-date"
                   value="${state.alertsGlobalDate ? formatDateForInput(state.alertsGlobalDate) : ''}"
                   onchange="saveGlobalDate()">
            <button class="btn-clear-dates" onclick="clearGlobalDate()" title="Limpiar fecha (usar totales)">
                ✕
            </button>
        </div>
        <p class="config-date-hint">${state.alertsGlobalDate ? `Filtrando por: ${formatDateForInput(state.alertsGlobalDate)}` : 'Sin filtro de fecha (totales acumulados)'}</p>
        <hr class="config-divider">
    `;

    html += Object.entries(state.alertsConfig).map(([key, config]) => `
        <div class="config-item" data-metric="${key}">
            <div class="config-item-header">
                <span class="config-item-name">${config.name}</span>
                <label class="toggle-switch">
                    <input type="checkbox" ${config.enabled ? 'checked' : ''}
                           onchange="toggleAlertEnabled('${key}', this.checked)">
                    <span class="toggle-slider"></span>
                </label>
            </div>
            <div class="config-item-input">
                <label>Umbral:</label>
                <input type="number" id="threshold-${key}" value="${config.threshold}" min="0"
                       onkeypress="if(event.key === 'Enter') saveThreshold('${key}')">
                <button class="btn-save-threshold" onclick="saveThreshold('${key}')">
                    Guardar
                </button>
            </div>
        </div>
    `).join('');

    container.innerHTML = html;
}

export function toggleAlertsConfig() {
    const sidebar = document.getElementById('alerts-sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    if (sidebar && overlay) {
        sidebar.classList.toggle('hidden');
        sidebar.classList.toggle('open');
        overlay.classList.toggle('hidden');
        overlay.classList.toggle('open');
    }
}

export function closeSidebar() {
    const sidebar = document.getElementById('alerts-sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    if (sidebar && overlay) {
        sidebar.classList.add('hidden');
        sidebar.classList.remove('open');
        overlay.classList.add('hidden');
        overlay.classList.remove('open');
    }
}

// Funciones que necesitan socket (se registran desde main.js)
export function createAlertActions(socket) {
    return {
        saveGlobalDate() {
            const dateInput = document.getElementById('alerts-global-date');
            const date = formatDateFromInput(dateInput?.value);
            socket.emit('set_alerts_global_date', { date });
        },

        clearGlobalDate() {
            const dateInput = document.getElementById('alerts-global-date');
            if (dateInput) dateInput.value = '';
            socket.emit('set_alerts_global_date', { date: null });
        },

        toggleAlertEnabled(metric, enabled) {
            socket.emit('update_alert_threshold', { metric, enabled });
        },

        saveThreshold(metric) {
            const input = document.getElementById(`threshold-${metric}`);
            if (input) {
                const threshold = parseInt(input.value, 10);
                if (!isNaN(threshold) && threshold >= 0) {
                    socket.emit('update_alert_threshold', { metric, threshold });
                }
            }
        },

        dismissAlert(alertId) {
            socket.emit('dismiss_alert', { alert_id: alertId });
        },

        clearAllAlerts() {
            socket.emit('clear_all_alerts');
        }
    };
}
