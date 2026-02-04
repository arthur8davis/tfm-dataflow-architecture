/**
 * COVID-19 Dashboard - Main Entry Point
 * Integra todos los módulos ES6
 */

import { state } from './modules/config.js';
import { showConnectionStatus, showNotification, filterValidDepts } from './modules/utils.js';
import {
    showAlertNotification,
    renderActiveAlerts,
    renderAlertsConfig,
    toggleAlertsConfig,
    closeSidebar,
    createAlertActions
} from './modules/alerts.js';
import {
    renderDepartmentList,
    renderSexList,
    updateFilterIndicator,
    filterDepartmentList,
    updateSummaryCards,
    updateSummaryCardsFromServer,
    createFilterActions
} from './modules/filters.js';
import { renderDepartmentChart, renderDemisesDeptChart } from './charts/department.js';
import { renderSexChart, renderDemisesSexChart } from './charts/sex.js';
import { renderTimelineChart, renderDemisesTimelineChart } from './charts/timeline.js';
import { renderAgeChart } from './charts/age.js';
import { renderHeatmap, renderDemisesHeatmap, renderHospitalizationsHeatmap } from './charts/heatmaps.js';

// Conexión WebSocket
const socket = io();

// Crear acciones de alertas y filtros
const alertActions = createAlertActions(socket);
const filterActions = createFilterActions(socket, {
    renderDepartmentChart,
    renderDemisesDeptChart,
    renderHeatmap,
    renderDemisesHeatmap,
    renderHospitalizationsHeatmap,
    renderSexChart,
    renderDemisesSexChart,
    renderTimelineChart,
    renderDemisesTimelineChart,
    renderAgeChart
});

// Exponer funciones globalmente para onclick en HTML
window.selectDepartment = filterActions.selectDepartment;
window.selectSex = filterActions.selectSex;
window.clearDeptFilter = filterActions.clearDeptFilter;
window.clearAllFilters = filterActions.clearAllFilters;
window.saveGlobalDate = alertActions.saveGlobalDate;
window.clearGlobalDate = alertActions.clearGlobalDate;
window.toggleAlertEnabled = alertActions.toggleAlertEnabled;
window.saveThreshold = alertActions.saveThreshold;
window.dismissAlert = alertActions.dismissAlert;
window.clearAllAlerts = alertActions.clearAllAlerts;
window.toggleAlertsConfig = toggleAlertsConfig;
window.closeSidebar = closeSidebar;
window.filterDepartmentList = filterDepartmentList;

// ============== WebSocket Events ==============

socket.on('connect', () => {
    console.log('[WebSocket] Conectado');
    state.isConnected = true;
    showConnectionStatus(true);
    showNotification('Conectado - Actualizaciones en tiempo real');
    socket.emit('get_alerts_config');
    socket.emit('get_active_alerts');
});

socket.on('disconnect', () => {
    console.log('[WebSocket] Desconectado');
    state.isConnected = false;
    showConnectionStatus(false);
    showNotification('Desconectado del servidor');
});

socket.on('data_changed', (data) => {
    console.log(`[WebSocket] Cambio detectado: ${data.collection} - ${data.operation}`);
    showNotification(`Nuevo ${data.operation} en ${data.collection}`);
    socket.emit('request_refresh');
});

// Eventos de actualización de datos
socket.on('update_summary', (data) => {
    console.log('[WebSocket] Actualizando resumen');
    updateSummaryCards(data);
});

socket.on('update_department', (data) => {
    console.log('[WebSocket] Actualizando departamentos');
    state.allDepartmentsData = data || [];
    renderDepartmentList();
    filterActions.applyFilters();
});

socket.on('update_sex', (data) => {
    console.log('[WebSocket] Actualizando distribución por sexo');
    state.allSexData = data || [];
    renderSexList();
    filterActions.applyFilters();
});

socket.on('update_timeline', (data) => {
    console.log('[WebSocket] Actualizando timeline');
    state.allTimelineData = data || [];
    renderTimelineChart(data);
});

socket.on('update_age', (data) => {
    console.log('[WebSocket] Actualizando grupos de edad');
    state.allAgeData = data || [];
    renderAgeChart(data);
});

socket.on('update_demises_dept', (data) => {
    console.log('[WebSocket] Actualizando fallecidos por departamento');
    state.allDemisesDeptData = data || [];
    filterActions.applyFilters();
});

socket.on('update_demises_sex', (data) => {
    console.log('[WebSocket] Actualizando fallecidos por sexo');
    state.allDemisesSexData = data || [];
    filterActions.applyFilters();
});

socket.on('update_heatmap', (data) => {
    console.log('[WebSocket] Actualizando mapa de calor de casos');
    state.allHeatmapData = data || [];
    filterActions.applyFilters();
});

socket.on('update_demises_heatmap', (data) => {
    console.log('[WebSocket] Actualizando mapa de calor de fallecidos');
    state.allDemisesHeatmapData = data || [];
    filterActions.applyFilters();
});

socket.on('update_hospitalizations_dept', (data) => {
    console.log('[WebSocket] Actualizando hospitalizaciones por departamento');
    state.allHospitalizationsDeptData = data || [];
    filterActions.applyFilters();
});

socket.on('update_hospitalizations_heatmap', (data) => {
    console.log('[WebSocket] Actualizando mapa de calor de hospitalizaciones');
    state.allHospitalizationsHeatmapData = data || [];
    filterActions.applyFilters();
});

socket.on('update_demises_timeline', (data) => {
    console.log('[WebSocket] Actualizando timeline de fallecidos');
    state.allDemisesTimelineData = data || [];
    renderDemisesTimelineChart(data);
});

// Eventos para datos filtrados
socket.on('filtered_summary', (data) => {
    console.log('[WebSocket] Resumen filtrado recibido');
    updateSummaryCardsFromServer(data);
});

socket.on('filtered_department', (data) => {
    console.log('[WebSocket] Departamentos filtrados recibidos');
    renderDepartmentChart(filterValidDepts(data));
});

socket.on('filtered_demises_dept', (data) => {
    console.log('[WebSocket] Fallecidos por depto filtrados recibidos');
    renderDemisesDeptChart(filterValidDepts(data));
});

socket.on('filtered_heatmap', (data) => {
    console.log('[WebSocket] Heatmap filtrado recibido');
    renderHeatmap(filterValidDepts(data));
});

socket.on('filtered_demises_heatmap', (data) => {
    console.log('[WebSocket] Heatmap fallecidos filtrado recibido');
    renderDemisesHeatmap(filterValidDepts(data));
});

socket.on('filtered_hospitalizations_dept', (data) => {
    console.log('[WebSocket] Hospitalizaciones por depto filtradas recibidas');
    state.allHospitalizationsDeptData = filterValidDepts(data);
});

socket.on('filtered_hospitalizations_heatmap', (data) => {
    console.log('[WebSocket] Heatmap hospitalizaciones filtrado recibido');
    renderHospitalizationsHeatmap(filterValidDepts(data));
});

socket.on('filtered_sex', (data) => {
    console.log('[WebSocket] Distribución por sexo filtrada recibida');
    renderSexChart(data);
    renderSexList();
});

socket.on('filtered_demises_sex', (data) => {
    console.log('[WebSocket] Fallecidos por sexo filtrados recibidos');
    renderDemisesSexChart(data);
});

socket.on('filtered_timeline', (data) => {
    console.log('[WebSocket] Timeline filtrado recibido');
    renderTimelineChart(data);
});

socket.on('filtered_demises_timeline', (data) => {
    console.log('[WebSocket] Timeline fallecidos filtrado recibido');
    renderDemisesTimelineChart(data);
});

socket.on('filtered_age', (data) => {
    console.log('[WebSocket] Grupos de edad filtrados recibidos');
    renderAgeChart(data);
});

// Eventos de Alertas
socket.on('alerts_config', (data) => {
    console.log('[WebSocket] Configuración de alertas recibida');
    state.alertsConfig = data.config || data;
    state.alertsGlobalDate = data.global_date || null;
    renderAlertsConfig();
});

socket.on('alerts_config_updated', (data) => {
    console.log('[WebSocket] Configuración de alertas actualizada');
    state.alertsConfig = data.config || data;
    state.alertsGlobalDate = data.global_date || null;
    renderAlertsConfig();
    showNotification('Configuración de alertas actualizada');
});

socket.on('alerts_global_date_updated', (data) => {
    console.log('[WebSocket] Fecha global de alertas actualizada');
    state.alertsGlobalDate = data.global_date || null;
    renderAlertsConfig();
});

socket.on('active_alerts', (alerts) => {
    console.log('[WebSocket] Alertas activas recibidas:', alerts.length);
    state.activeAlerts = alerts;
    renderActiveAlerts();
});

socket.on('threshold_alert', (alert) => {
    console.log('[WebSocket] Nueva alerta:', alert);
    state.activeAlerts.push(alert);
    renderActiveAlerts();
    showAlertNotification(alert);
});

socket.on('alert_dismissed', (data) => {
    console.log('[WebSocket] Alerta descartada:', data.alert_id);
    state.activeAlerts = state.activeAlerts.filter(a => a.id !== data.alert_id);
    renderActiveAlerts();
});

// ============== Responsive ==============

let resizeTimeout;
window.addEventListener('resize', () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
        socket.emit('request_refresh');
    }, 10000);
});

// ============== Inicialización ==============

document.addEventListener('DOMContentLoaded', () => {
    console.log('[Dashboard] Inicializando...');

    const toggleBtn = document.getElementById('toggle-alerts-config');
    if (toggleBtn) {
        toggleBtn.addEventListener('click', toggleAlertsConfig);
    }

    const closeBtn = document.getElementById('close-sidebar');
    if (closeBtn) {
        closeBtn.addEventListener('click', closeSidebar);
    }

    const overlay = document.getElementById('sidebar-overlay');
    if (overlay) {
        overlay.addEventListener('click', closeSidebar);
    }

    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            closeSidebar();
        }
    });

    const deptSearchInput = document.getElementById('dept-search-input');
    if (deptSearchInput) {
        deptSearchInput.addEventListener('input', (e) => {
            filterDepartmentList(e.target.value);
        });
    }
});
