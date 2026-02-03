/**
 * COVID-19 Dashboard - D3.js Charts con WebSockets
 * Actualizaciones en tiempo real via Socket.IO
 */

// Conexión WebSocket
const socket = io();

const COLORS = {
    masculine: '#50aae5ff',
    feminine: '#d497acff',
    primary: '#4a90d9',
    positive: '#e74c3c',
    success: '#27ae60',
    warning: '#f39c12',
    info: '#3498db',
    dark: '#2c3e50'
};

// Estado de conexión
let isConnected = false;

// ============== Estado de Alertas ==============
let alertsConfig = {};
let activeAlerts = [];

// ============== Estado de Filtros ==============
let selectedDepartment = 'TODOS';
let selectedSex = 'TODOS';
let allDepartmentsData = [];
let allDemisesDeptData = [];
let allHeatmapData = [];
let allDemisesHeatmapData = [];
let allTimelineData = [];
let allDemisesTimelineData = [];
let allAgeData = [];
let allSexData = [];
let allDemisesSexData = [];

// ============== Utilidades ==============

function formatNumber(num) {
    return new Intl.NumberFormat('es-PE').format(num);
}

function formatDateTime(isoString) {
    return new Date(isoString).toLocaleString('es-PE', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function showConnectionStatus(connected) {
    const indicator = document.getElementById('connection-status');
    if (indicator) {
        indicator.className = connected ? 'status-connected' : 'status-disconnected';
        indicator.title = connected ? 'Conectado - Tiempo real activo' : 'Desconectado';
    }
}

function showNotification(message) {
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

// ============== WebSocket Events ==============

socket.on('connect', () => {
    console.log('[WebSocket] Conectado');
    isConnected = true;
    showConnectionStatus(true);
    showNotification('Conectado - Actualizaciones en tiempo real');
    // Solicitar configuración de alertas
    socket.emit('get_alerts_config');
    socket.emit('get_active_alerts');
});

socket.on('disconnect', () => {
    console.log('[WebSocket] Desconectado');
    isConnected = false;
    showConnectionStatus(false);
    showNotification('Desconectado del servidor');
});

socket.on('data_changed', (data) => {
    console.log(`[WebSocket] Cambio detectado: ${data.collection} - ${data.operation}`);
    showNotification(`Nuevo ${data.operation} en ${data.collection}`);
    // actualizar el dashboard
    // Emitir evento para refrescar todos los gráficos
    socket.emit('request_refresh');
});

// Eventos de actualización de datos
socket.on('update_summary', (data) => {
    console.log('[WebSocket] Actualizando resumen');
    updateSummaryCards(data);
});

socket.on('update_department', (data) => {
    console.log('[WebSocket] Actualizando departamentos');
    allDepartmentsData = data || [];
    renderDepartmentList();
    applyFilters();
});

socket.on('update_sex', (data) => {
    console.log('[WebSocket] Actualizando distribución por sexo');
    allSexData = data || [];
    renderSexList();
    applyFilters();
});

socket.on('update_timeline', (data) => {
    console.log('[WebSocket] Actualizando timeline');
    allTimelineData = data || [];
    renderTimelineChart(data);
});

socket.on('update_age', (data) => {
    console.log('[WebSocket] Actualizando grupos de edad');
    allAgeData = data || [];
    renderAgeChart(data);
});

socket.on('update_demises_dept', (data) => {
    console.log('[WebSocket] Actualizando fallecidos por departamento');
    allDemisesDeptData = data || [];
    applyFilters();
});

socket.on('update_demises_sex', (data) => {
    console.log('[WebSocket] Actualizando fallecidos por sexo');
    allDemisesSexData = data || [];
    applyFilters();
});

socket.on('update_heatmap', (data) => {
    console.log('[WebSocket] Actualizando mapa de calor de casos');
    allHeatmapData = data || [];
    applyFilters();
});

socket.on('update_demises_heatmap', (data) => {
    console.log('[WebSocket] Actualizando mapa de calor de fallecidos');
    allDemisesHeatmapData = data || [];
    applyFilters();
});

// Eventos para datos filtrados desde el servidor
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

socket.on('update_demises_timeline', (data) => {
    console.log('[WebSocket] Actualizando timeline de fallecidos');
    allDemisesTimelineData = data || [];
    renderDemisesTimelineChart(data);
});

// Eventos de Alertas
socket.on('alerts_config', (config) => {
    console.log('[WebSocket] Configuración de alertas recibida');
    alertsConfig = config;
    renderAlertsConfig();
});

socket.on('alerts_config_updated', (config) => {
    console.log('[WebSocket] Configuración de alertas actualizada');
    alertsConfig = config;
    renderAlertsConfig();
    showNotification('Configuración de alertas actualizada');
});

socket.on('active_alerts', (alerts) => {
    console.log('[WebSocket] Alertas activas recibidas:', alerts.length);
    activeAlerts = alerts;
    renderActiveAlerts();
});

socket.on('threshold_alert', (alert) => {
    console.log('[WebSocket] Nueva alerta:', alert);
    activeAlerts.push(alert);
    renderActiveAlerts();
    showAlertNotification(alert);
});

socket.on('alert_dismissed', (data) => {
    console.log('[WebSocket] Alerta descartada:', data.alert_id);
    activeAlerts = activeAlerts.filter(a => a.id !== data.alert_id);
    renderActiveAlerts();
});

// ============== Sistema de Alertas ==============

function showAlertNotification(alert) {
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

function renderActiveAlerts() {
    const container = document.getElementById('active-alerts');
    if (!container) return;

    if (!activeAlerts || activeAlerts.length === 0) {
        container.innerHTML = '<p class="no-alerts">Sin alertas activas</p>';
        return;
    }

    container.innerHTML = activeAlerts.map(alert => `
        <div class="alert-item ${alert.level}" data-alert-id="${alert.id}">
            <span class="alert-icon">${alert.level === 'critical' ? '🚨' : '⚠️'}</span>
            <div class="alert-content">
                <div class="alert-title">${alert.name}</div>
                <div class="alert-details">
                    Valor actual: <span class="alert-value">${formatNumber(alert.current_value)}</span>
                    (umbral: <span class="alert-threshold">${formatNumber(alert.threshold)}</span>)
                    ${alert.context ? `<br><span class="alert-context">${alert.context}</span>` : ''}
                </div>
                <div class="alert-time">${formatDateTime(alert.timestamp)}</div>
            </div>
            <button class="btn-dismiss" onclick="dismissAlert('${alert.id}')">Descartar</button>
        </div>
    `).join('');
}

function renderAlertsConfig() {
    const container = document.getElementById('thresholds-config');
    if (!container) return;

    container.innerHTML = Object.entries(alertsConfig).map(([key, config]) => `
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
}

function toggleAlertEnabled(metric, enabled) {
    socket.emit('update_alert_threshold', { metric, enabled });
}

function saveThreshold(metric) {
    const input = document.getElementById(`threshold-${metric}`);
    if (input) {
        const threshold = parseInt(input.value, 10);
        if (!isNaN(threshold) && threshold >= 0) {
            socket.emit('update_alert_threshold', { metric, threshold });
        }
    }
}

function dismissAlert(alertId) {
    socket.emit('dismiss_alert', { alert_id: alertId });
}

function clearAllAlerts() {
    socket.emit('clear_all_alerts');
}

function toggleAlertsConfig() {
    const sidebar = document.getElementById('alerts-sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    if (sidebar && overlay) {
        sidebar.classList.toggle('hidden');
        sidebar.classList.toggle('open');
        overlay.classList.toggle('hidden');
        overlay.classList.toggle('open');
    }
}

function closeSidebar() {
    const sidebar = document.getElementById('alerts-sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    if (sidebar && overlay) {
        sidebar.classList.add('hidden');
        sidebar.classList.remove('open');
        overlay.classList.add('hidden');
        overlay.classList.remove('open');
    }
}

// ============== Filtrado por Departamento ==============

function renderDepartmentList() {
    const container = document.getElementById('dept-list');
    if (!container || !allDepartmentsData.length) return;

    // Filtrar "Sin especificar" y calcular total
    const validDepts = allDepartmentsData.filter(d =>
        d.departamento &&
        d.departamento !== 'Sin especificar' &&
        d.departamento.trim() !== ''
    );
    const total = validDepts.reduce((sum, d) => sum + d.total, 0);

    let html = `
        <div class="dept-item ${selectedDepartment === 'TODOS' ? 'active' : ''}"
             data-dept="TODOS" onclick="selectDepartment('TODOS')">
            <span class="dept-name">Todos los departamentos</span>
            <span class="dept-count">${formatNumber(total)}</span>
        </div>
    `;

    validDepts.forEach(dept => {
        const isActive = selectedDepartment === dept.departamento;
        html += `
            <div class="dept-item ${isActive ? 'active' : ''}"
                 data-dept="${dept.departamento}" onclick="selectDepartment('${dept.departamento}')">
                <span class="dept-name">${dept.departamento}</span>
                <span class="dept-count">${formatNumber(dept.total)}</span>
            </div>
        `;
    });

    container.innerHTML = html;
}

function selectDepartment(dept) {
    selectedDepartment = dept;

    // Actualizar UI del sidebar
    document.querySelectorAll('.dept-item').forEach(item => {
        item.classList.toggle('active', item.dataset.dept === dept);
    });

    // Actualizar indicador de filtro
    updateFilterIndicator();

    // Aplicar filtro a todas las gráficas
    applyFilters();
}

function clearDeptFilter() {
    selectDepartment('TODOS');
}

function updateFilteredSummaryCards(deptData, demisesDeptData) {
    const casesTotal = document.getElementById('cases-total');
    const casesPositive = document.getElementById('cases-positive');
    const demisesTotal = document.getElementById('demises-total');
    const hospitalizationsTotal = document.getElementById('hospitalizations-total');

    // Calcular totales de los datos filtrados
    const totalCases = deptData.reduce((sum, d) => sum + (d.total || 0), 0);
    const totalPositive = deptData.reduce((sum, d) => sum + (d.positivos || 0), 0);
    const totalDemises = demisesDeptData.reduce((sum, d) => sum + (d.total || 0), 0);

    // Animación de actualización
    [casesTotal, casesPositive, demisesTotal, hospitalizationsTotal].forEach(el => {
        if (el) {
            el.classList.add('updating');
            setTimeout(() => el.classList.remove('updating'), 500);
        }
    });

    if (casesTotal) casesTotal.textContent = formatNumber(totalCases);
    if (casesPositive) casesPositive.textContent = formatNumber(totalPositive);
    if (demisesTotal) demisesTotal.textContent = formatNumber(totalDemises);
    if (hospitalizationsTotal) hospitalizationsTotal.textContent = formatNumber(0);
}

function filterDepartmentList(searchTerm) {
    const items = document.querySelectorAll('.dept-item');
    const term = searchTerm.toLowerCase();

    items.forEach(item => {
        const deptName = item.dataset.dept.toLowerCase();
        if (deptName === 'todos' || deptName.includes(term)) {
            item.style.display = 'flex';
        } else {
            item.style.display = 'none';
        }
    });
}

// ============== Filtrado por Sexo ==============

function renderSexList() {
    const container = document.getElementById('sex-list');
    if (!container || !allSexData.length) return;

    // Filtrar valores sin especificar
    const validSexData = allSexData.filter(d =>
        d.sexo &&
        d.sexo !== 'Sin especificar' &&
        d.sexo.trim() !== ''
    );

    const total = validSexData.reduce((sum, d) => sum + d.total, 0);

    let html = `
        <div class="sex-item ${selectedSex === 'TODOS' ? 'active' : ''}"
             data-sex="TODOS" onclick="selectSex('TODOS')">
            <span class="sex-name">Todos</span>
            <span class="sex-count">${formatNumber(total)}</span>
        </div>
    `;

    validSexData.forEach(item => {
        const isActive = selectedSex === item.sexo;
        html += `
            <div class="sex-item ${isActive ? 'active' : ''}"
                 data-sex="${item.sexo}" onclick="selectSex('${item.sexo}')">
                <span class="sex-name">${item.sexo}</span>
                <span class="sex-count">${formatNumber(item.total)}</span>
            </div>
        `;
    });

    container.innerHTML = html;
}

function selectSex(sex) {
    selectedSex = sex;

    // Actualizar UI del sidebar
    document.querySelectorAll('.sex-item').forEach(item => {
        item.classList.toggle('active', item.dataset.sex === sex);
    });

    // Actualizar indicador de filtro
    updateFilterIndicator();

    // Aplicar filtro
    applyFilters();
}

function clearAllFilters() {
    selectedDepartment = 'TODOS';
    selectedSex = 'TODOS';

    // Actualizar UI
    document.querySelectorAll('.dept-item').forEach(item => {
        item.classList.toggle('active', item.dataset.dept === 'TODOS');
    });
    document.querySelectorAll('.sex-item').forEach(item => {
        item.classList.toggle('active', item.dataset.sex === 'TODOS');
    });

    // Ocultar indicador
    const indicator = document.getElementById('filter-indicator');
    if (indicator) {
        indicator.style.display = 'none';
    }

    // Aplicar filtro
    applyFilters();
}

function updateFilterIndicator() {
    const indicator = document.getElementById('filter-indicator');
    const sexLabel = document.getElementById('filter-sex-label');
    const deptName = document.getElementById('filter-dept-name');

    if (!indicator) return;

    const hasSexFilter = selectedSex !== 'TODOS';
    const hasDeptFilter = selectedDepartment !== 'TODOS';

    if (!hasSexFilter && !hasDeptFilter) {
        indicator.style.display = 'none';
    } else {
        indicator.style.display = 'inline-flex';

        if (sexLabel) {
            sexLabel.textContent = hasSexFilter ? `${selectedSex}` : '';
        }
        if (deptName) {
            if (hasSexFilter && hasDeptFilter) {
                deptName.textContent = ` + ${selectedDepartment}`;
            } else if (hasDeptFilter) {
                deptName.textContent = selectedDepartment;
            } else {
                deptName.textContent = '';
            }
        }
    }
}

// Función helper para filtrar departamentos válidos
function filterValidDepts(data) {
    return (data || []).filter(d =>
        d.departamento && d.departamento !== 'Sin especificar'
    );
}

// Función helper para filtrar sexos válidos
function filterValidSex(data) {
    return (data || []).filter(d =>
        d.sexo && d.sexo !== 'Sin especificar' && d.sexo.trim() !== ''
    );
}

// Actualizar cards desde datos del servidor
function updateSummaryCardsFromServer(data) {
    const casesTotal = document.getElementById('cases-total');
    const casesPositive = document.getElementById('cases-positive');
    const demisesTotal = document.getElementById('demises-total');
    const hospitalizationsTotal = document.getElementById('hospitalizations-total');

    // Animación de actualización
    [casesTotal, casesPositive, demisesTotal, hospitalizationsTotal].forEach(el => {
        if (el) {
            el.classList.add('updating');
            setTimeout(() => el.classList.remove('updating'), 500);
        }
    });

    if (casesTotal) casesTotal.textContent = formatNumber(data.cases_total || 0);
    if (casesPositive) casesPositive.textContent = formatNumber(data.cases_positive || 0);
    if (demisesTotal) demisesTotal.textContent = formatNumber(data.demises_total || 0);
    if (hospitalizationsTotal) hospitalizationsTotal.textContent = formatNumber(data.hospitalizations_total || 0);
}

function applyFilters() {
    // Solicitar datos filtrados al servidor si hay algún filtro activo
    if (selectedDepartment !== 'TODOS' || selectedSex !== 'TODOS') {
        console.log(`[Filtros] Solicitando datos filtrados: dept=${selectedDepartment}, sexo=${selectedSex}`);
        socket.emit('request_filtered_data', {
            departamento: selectedDepartment,
            sexo: selectedSex
        });
        // Los gráficos se actualizarán cuando lleguen los eventos filtered_*
    } else {
        // Sin filtros, usar datos locales
        applyLocalFilters();
    }

    // Edad siempre con datos completos (por ahora)
    renderAgeChart(allAgeData);
}

function applyLocalFilters() {
    // Filtrar "Sin especificar" de todos los datos
    const validDepts = filterValidDepts(allDepartmentsData);
    const validDemisesDepts = filterValidDepts(allDemisesDeptData);
    const validHeatmap = filterValidDepts(allHeatmapData);
    const validDemisesHeatmap = filterValidDepts(allDemisesHeatmapData);
    const validSexData = filterValidSex(allSexData);
    const validDemisesSexData = filterValidSex(allDemisesSexData);

    // Renderizar gráficos con datos completos
    renderDepartmentChart(validDepts);
    renderDemisesDeptChart(validDemisesDepts);
    renderHeatmap(validHeatmap);
    renderDemisesHeatmap(validDemisesHeatmap);
    renderSexChart(validSexData);
    renderDemisesSexChart(validDemisesSexData);
    renderTimelineChart(allTimelineData);
    renderDemisesTimelineChart(allDemisesTimelineData);

    // Actualizar cards con totales generales
    const totalCases = validDepts.reduce((sum, d) => sum + (d.total || 0), 0);
    const totalPositive = validDepts.reduce((sum, d) => sum + (d.positivos || 0), 0);
    const totalDemises = validDemisesDepts.reduce((sum, d) => sum + (d.total || 0), 0);

    updateSummaryCardsFromServer({
        cases_total: totalCases,
        cases_positive: totalPositive,
        demises_total: totalDemises
    });
}

// ============== Actualizar Cards ==============

function updateSummaryCards(data) {
    const casesTotal = document.getElementById('cases-total');
    const casesPositive = document.getElementById('cases-positive');
    const demisesTotal = document.getElementById('demises-total');
    const hospitalizationsTotal = document.getElementById('hospitalizations-total');
    const lastUpdated = document.getElementById('last-updated');

    // Animación de actualización
    [casesTotal, casesPositive, demisesTotal, hospitalizationsTotal].forEach(el => {
        if (el) {
            el.classList.add('updating');
            setTimeout(() => el.classList.remove('updating'), 500);
        }
    });

    if (casesTotal) casesTotal.textContent = formatNumber(data.cases_total);
    if (casesPositive) casesPositive.textContent = formatNumber(data.cases_positive);
    if (demisesTotal) demisesTotal.textContent = formatNumber(data.demises_total);
    if (hospitalizationsTotal) hospitalizationsTotal.textContent = formatNumber(data.hospitalizations_total);
    if (lastUpdated) lastUpdated.textContent = new Date(data.last_updated).toLocaleString('es-PE');
}

// ============== Gráficas D3.js ==============

function renderDepartmentChart(data) {
    const container = d3.select('#chart-department');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

    // Leyenda
    const legend = container.append('div').attr('class', 'chart-legend');
    const legendItems = [
        { label: 'Total', color: COLORS.primary, opacity: 0.6 },
        { label: 'Confirmados', color: COLORS.positive, opacity: 1 }
    ];
    legendItems.forEach(item => {
        const li = legend.append('div').attr('class', 'legend-item');
        li.append('span')
            .attr('class', 'legend-swatch')
            .style('background', item.color)
            .style('opacity', item.opacity);
        li.append('span').text(item.label);
    });

    const margin = { top: 20, right: 30, bottom: 40, left: 120 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = Math.max(400, data.length * 25);

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.total)])
        .range([0, width]);

    const y = d3.scaleBand()
        .domain(data.map(d => d.departamento))
        .range([0, height])
        .padding(0.2);

    // Barras totales con transición
    svg.selectAll('.bar-total')
        .data(data)
        .join('rect')
        .attr('class', 'bar-total')
        .attr('x', 0)
        .attr('y', d => y(d.departamento))
        .attr('height', y.bandwidth())
        .attr('fill', COLORS.primary)
        .attr('opacity', 0.6)
        .attr('width', 0)
        .transition()
        .duration(750)
        .attr('width', d => x(d.total));

    // Barras positivos
    svg.selectAll('.bar-positive')
        .data(data)
        .join('rect')
        .attr('class', 'bar-positive')
        .attr('x', 0)
        .attr('y', d => y(d.departamento))
        .attr('height', y.bandwidth())
        .attr('fill', COLORS.positive)
        .attr('width', 0)
        .transition()
        .duration(750)
        .delay(100)
        .attr('width', d => x(d.positivos));

    // Ejes
    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(5).tickFormat(d3.format('.2s')));

    svg.append('g')
        .call(d3.axisLeft(y));
}

function renderSexChart(data) {
    const container = d3.select('#chart-sex');
    container.selectAll('*').remove();

    // Filtrar "Sin especificar"
    const filteredData = (data || []).filter(d =>
        d.sexo && d.sexo !== 'Sin especificar' && d.sexo.trim() !== ''
    );

    if (!filteredData || !filteredData.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

    // Usar datos filtrados
    data = filteredData;

    const width = container.node().clientWidth;
    const height = 300;
    const radius = Math.min(width, height) / 2 - 40;

    const svg = container.append('svg')
        .attr('width', width)
        .attr('height', height)
        .append('g')
        .attr('transform', `translate(${width / 2},${height / 2})`);

    const color = d3.scaleOrdinal()
        .domain(data.map(d => d.sexo))
        .range([COLORS.masculine, COLORS.feminine, COLORS.warning]);

    const pie = d3.pie()
        .value(d => d.total)
        .sort(null);

    const arc = d3.arc()
        .innerRadius(radius * 0.5)
        .outerRadius(radius);

    const arcs = svg.selectAll('arc')
        .data(pie(data))
        .join('g')
        .attr('class', 'arc');

    // Arcos con transición
    arcs.append('path')
        .attr('fill', d => color(d.data.sexo))
        .attr('stroke', 'white')
        .attr('stroke-width', 2)
        .transition()
        .duration(750)
        .attrTween('d', function (d) {
            const interpolate = d3.interpolate({ startAngle: 0, endAngle: 0 }, d);
            return t => arc(interpolate(t));
        });

    // Labels
    const labelArc = d3.arc()
        .innerRadius(radius * 0.8)
        .outerRadius(radius * 0.8);



    // Leyenda
    const legend = svg.selectAll('.legend')
        .data(data)
        .join('g')
        .attr('class', 'legend')
        .attr('transform', (d, i) => `translate(${radius + 20},${-height / 4 + i * 25})`);

    legend.append('rect')
        .attr('width', 18)
        .attr('height', 18)
        .attr('fill', d => color(d.sexo));

    legend.append('text')
        .attr('x', 24)
        .attr('y', 14)
        .attr('font-size', '12px')
        .text(d => `${d.sexo}: ${formatNumber(d.total)}`);
}

function renderDemisesSexChart(data) {
    const container = d3.select('#chart-demises-sex');
    container.selectAll('*').remove();

    // Filtrar "Sin especificar"
    const filteredData = (data || []).filter(d =>
        d.sexo && d.sexo !== 'Sin especificar' && d.sexo.trim() !== ''
    );

    if (!filteredData || !filteredData.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

    // Usar datos filtrados
    data = filteredData;

    const width = container.node().clientWidth;
    const height = 300;
    const radius = Math.min(width, height) / 2 - 40;

    const svg = container.append('svg')
        .attr('width', width)
        .attr('height', height)
        .append('g')
        .attr('transform', `translate(${width / 2},${height / 2})`);

    const color = d3.scaleOrdinal()
        .domain(data.map(d => d.sexo))
        .range([COLORS.masculine, COLORS.feminine, COLORS.warning]);

    const pie = d3.pie()
        .value(d => d.total)
        .sort(null);

    const arc = d3.arc()
        .innerRadius(radius * 0.45)
        .outerRadius(radius);

    const arcs = svg.selectAll('arc')
        .data(pie(data))
        .join('g')
        .attr('class', 'arc');

    arcs.append('path')
        .attr('fill', d => color(d.data.sexo))
        .attr('stroke', 'white')
        .attr('stroke-width', 2)
        .transition()
        .duration(750)
        .attrTween('d', function (d) {
            const interpolate = d3.interpolate({ startAngle: 0, endAngle: 0 }, d);
            return t => arc(interpolate(t));
        });

    const labelArc = d3.arc()
        .innerRadius(radius * 0.75)
        .outerRadius(radius * 0.75);


    // Leyenda
    const legend = svg.selectAll('.legend')
        .data(data)
        .join('g')
        .attr('class', 'legend')
        .attr('transform', (d, i) => `translate(${radius + 20},${-height / 4 + i * 25})`);

    legend.append('rect')
        .attr('width', 18)
        .attr('height', 18)
        .attr('fill', d => color(d.sexo));

    legend.append('text')
        .attr('x', 24)
        .attr('y', 14)
        .attr('font-size', '12px')
        .text(d => `${d.sexo || 'Sin especificar'}: ${formatNumber(d.total)}`);
}

function renderTimelineChart(data) {
    const container = d3.select('#chart-timeline');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

    // Parsear fechas y ordenar
    const parseDate = d3.timeParse('%Y%m%d');
    data.forEach(d => {
        d.date = parseDate(d.fecha) || new Date(d.fecha);
    });
    data.sort((a, b) => a.date - b.date);

    const margin = { top: 20, right: 30, bottom: 60, left: 70 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 300;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    // Usar positivos (casos confirmados)
    const maxPositivos = d3.max(data, d => d.positivos) || 1;

    const x = d3.scaleBand()
        .domain(data.map(d => d.fecha))
        .range([0, width])
        .padding(0.1);

    const y = d3.scaleLinear()
        .domain([0, maxPositivos])
        .nice()
        .range([height, 0]);

    // Barras de casos confirmados (positivos)
    svg.selectAll('.bar')
        .data(data)
        .join('rect')
        .attr('class', 'bar')
        .attr('x', d => x(d.fecha))
        .attr('width', x.bandwidth())
        .attr('fill', COLORS.positive)
        .attr('y', height)
        .attr('height', 0)
        .transition()
        .duration(750)
        .delay((d, i) => i * 10)
        .attr('y', d => y(d.positivos))
        .attr('height', d => height - y(d.positivos));

    // Tooltip en hover
    svg.selectAll('.bar')
        .on('mouseover', function (event, d) {
            d3.select(this).attr('fill', COLORS.warning);
            // Mostrar tooltip
            const tooltip = svg.append('g')
                .attr('class', 'tooltip-group')
                .attr('transform', `translate(${x(d.fecha) + x.bandwidth() / 2}, ${y(d.positivos) - 10})`);

            tooltip.append('text')
                .attr('text-anchor', 'middle')
                .attr('fill', '#fff')
                .attr('font-size', '11px')
                .text(formatNumber(d.positivos));
        })
        .on('mouseout', function () {
            d3.select(this).attr('fill', COLORS.positive);
            svg.selectAll('.tooltip-group').remove();
        });

    // Eje X con fechas
    const xAxis = svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x)
            .tickValues(x.domain().filter((d, i) => i % Math.ceil(data.length / 10) === 0))
            .tickFormat(d => {
                const date = parseDate(d);
                return date ? d3.timeFormat('%d/%m/%y')(date) : d;
            }));

    xAxis.selectAll('text')
        .attr('transform', 'rotate(-45)')
        .style('text-anchor', 'end');

    svg.append('g')
        .call(d3.axisLeft(y).ticks(5).tickFormat(d3.format('.2s')));

    svg.append('text')
        .attr('transform', 'rotate(-90)')
        .attr('y', -50)
        .attr('x', -height / 2)
        .attr('text-anchor', 'middle')
        .attr('font-size', '12px')
        .text('Casos');
}

function renderDemisesTimelineChart(data) {
    const container = d3.select('#chart-demises-timeline');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

    // Parsear fechas y ordenar
    const parseDate = d3.timeParse('%Y%m%d');
    data.forEach(d => {
        d.date = parseDate(d.fecha) || new Date(d.fecha);
    });
    data.sort((a, b) => a.date - b.date);

    const margin = { top: 20, right: 30, bottom: 60, left: 70 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 300;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const maxTotal = d3.max(data, d => d.total) || 1;

    const x = d3.scaleBand()
        .domain(data.map(d => d.fecha))
        .range([0, width])
        .padding(0.1);

    const y = d3.scaleLinear()
        .domain([0, maxTotal])
        .nice()
        .range([height, 0]);

    // Barras de fallecidos (color morado/rojo oscuro)
    svg.selectAll('.bar')
        .data(data)
        .join('rect')
        .attr('class', 'bar')
        .attr('x', d => x(d.fecha))
        .attr('width', x.bandwidth())
        .attr('fill', '#983f3fff')
        .attr('y', height)
        .attr('height', 0)
        .transition()
        .duration(750)
        .delay((d, i) => i * 10)
        .attr('y', d => y(d.total))
        .attr('height', d => height - y(d.total));

    // Tooltip en hover
    svg.selectAll('.bar')
        .on('mouseover', function (event, d) {
            d3.select(this).attr('fill', '#e74c3c');
            const tooltip = svg.append('g')
                .attr('class', 'tooltip-group')
                .attr('transform', `translate(${x(d.fecha) + x.bandwidth() / 2}, ${y(d.total) - 10})`);

            tooltip.append('text')
                .attr('text-anchor', 'middle')
                .attr('fill', '#fff')
                .attr('font-size', '11px')
                .text(formatNumber(d.total));
        })
        .on('mouseout', function () {
            d3.select(this).attr('fill', '#a84847ff');
            svg.selectAll('.tooltip-group').remove();
        });

    // Eje X con fechas
    const xAxis = svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x)
            .tickValues(x.domain().filter((d, i) => i % Math.ceil(data.length / 10) === 0))
            .tickFormat(d => {
                const date = parseDate(d);
                return date ? d3.timeFormat('%d/%m/%y')(date) : d;
            }));

    xAxis.selectAll('text')
        .attr('transform', 'rotate(-45)')
        .style('text-anchor', 'end');

    // Eje Y
    svg.append('g')
        .call(d3.axisLeft(y).ticks(5).tickFormat(d3.format('.2s')));

    // Etiqueta Y
    svg.append('text')
        .attr('transform', 'rotate(-90)')
        .attr('y', -55)
        .attr('x', -height / 2)
        .attr('text-anchor', 'middle')
        .attr('font-size', '12px')
        .text('Fallecidos');
}

function renderAgeChart(data) {
    const container = d3.select('#chart-age');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

    const margin = { top: 20, right: 30, bottom: 60, left: 60 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 300;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleBand()
        .domain(data.map(d => d.grupo))
        .range([0, width])
        .padding(0.2);

    const y = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.total)])
        .nice()
        .range([height, 0]);

    // Barras con transición
    svg.selectAll('.bar')
        .data(data)
        .join('rect')
        .attr('class', 'bar')
        .attr('x', d => x(d.grupo))
        .attr('width', x.bandwidth())
        .attr('fill', COLORS.info)
        .attr('y', height)
        .attr('height', 0)
        .transition()
        .duration(750)
        .delay((d, i) => i * 50)
        .attr('y', d => y(d.total))
        .attr('height', d => height - y(d.total));

    // Ejes
    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x))
        .selectAll('text')
        .attr('transform', 'rotate(-45)')
        .style('text-anchor', 'end');

    svg.append('g')
        .call(d3.axisLeft(y).ticks(5).tickFormat(d3.format('.2s')));

    svg.append('text')
        .attr('x', width / 2)
        .attr('y', height + 50)
        .attr('text-anchor', 'middle')
        .attr('font-size', '12px')
        .text('Grupo de Edad');
}

// Variable global para el mapa
let peruMap = null;
let heatLayer = null;
let markersLayer = null;

function renderHeatmap(data) {
    const container = document.getElementById('chart-heatmap');

    if (!data || !data.length) {
        container.innerHTML = '<p class="no-data">Sin datos disponibles</p>';
        return;
    }

    // Filtrar datos con coordenadas válidas (verificar que sean números válidos)
    const validData = data.filter(d =>
        d.lat !== null && d.lon !== null &&
        !isNaN(d.lat) && !isNaN(d.lon) &&
        d.lat >= -90 && d.lat <= 90 &&
        d.lon >= -180 && d.lon <= 180
    );

    console.log('[Heatmap] Datos recibidos:', data.length, 'Válidos:', validData.length);
    if (validData.length > 0) {
        console.log('[Heatmap] Ejemplo:', validData[0]);
    }

    if (!validData.length) {
        container.innerHTML = '<p class="no-data">Sin coordenadas disponibles</p>';
        return;
    }

    // Crear mapa si no existe
    if (!peruMap) {
        container.innerHTML = '';
        container.style.height = '500px';

        peruMap = L.map('chart-heatmap').setView([-9.19, -75.0152], 5);

        // Capa de mapa oscuro
        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; OpenStreetMap &copy; CARTO',
            maxZoom: 19
        }).addTo(peruMap);
    }

    // Limpiar capas anteriores
    if (heatLayer) {
        peruMap.removeLayer(heatLayer);
    }
    if (markersLayer) {
        peruMap.removeLayer(markersLayer);
    }

    const maxTotal = d3.max(validData, d => d.total) || 1;

    // Capa de calor con radio más pequeño para mayor precisión
    const heatData = validData.map(d => {
        const intensity = Math.pow(d.total / maxTotal, 0.5);
        return [d.lat, d.lon, intensity];
    });

    heatLayer = L.heatLayer(heatData, {
        radius: 45,
        blur: 35,
        minOpacity: 0.25,
        maxZoom: 12,
        max: 1.0,
        gradient: {
            0.0: '#313695',
            0.2: '#4575b4',
            0.4: '#74add1',
            0.5: '#fee090',
            0.7: '#f46d43',
            0.9: '#d73027',
            1.0: '#a50026'
        }
    }).addTo(peruMap);

    // Agregar marcadores con puntos exactos
    markersLayer = L.layerGroup();

    validData.forEach(d => {
        // Tamaño del marcador basado en total
        const radius = Math.max(5, Math.min(15, Math.sqrt(d.total / maxTotal) * 15));

        const marker = L.circleMarker([d.lat, d.lon], {
            radius: radius,
            fillColor: '#df4634ff',
            color: '#fff',
            weight: 1,
            opacity: 0.8,
            fillOpacity: 0.6
        });

        const location = d.distrito
            ? `${d.distrito}, ${d.provincia}`
            : (d.provincia || d.departamento);

        marker.bindPopup(`
            <div style="text-align: center; min-width: 150px;">
                <strong>${d.departamento}</strong><br>
                ${d.provincia ? `<span style="font-size: 11px;">${d.provincia}</span><br>` : ''}
                ${d.distrito ? `<span style="font-size: 10px; color: #666;">${d.distrito}</span><br>` : ''}
                <hr style="margin: 5px 0; border-color: #ddd;">
                <b>Total:</b> ${formatNumber(d.total)}<br>
                <b>Positivos:</b> ${formatNumber(d.positivos)}<br>
                <small style="color: #888;">Lat: ${d.lat.toFixed(4)}, Lon: ${d.lon.toFixed(4)}</small>
            </div>
        `);

        marker.bindTooltip(location, {
            permanent: false,
            direction: 'top',
            className: 'dept-tooltip'
        });

        markersLayer.addLayer(marker);
    });

    markersLayer.addTo(peruMap);

    // Forzar actualización del mapa
    setTimeout(() => {
        peruMap.invalidateSize();
    }, 100);
}

// Variables para el mapa de fallecidos
let demisesMap = null;
let demisesHeatLayer = null;
let demisesMarkersLayer = null;

function renderDemisesHeatmap(data) {
    const container = document.getElementById('chart-demises-heatmap');

    if (!container) {
        console.log('[Demises Heatmap] Contenedor no encontrado');
        return;
    }

    if (!data || !data.length) {
        container.innerHTML = '<p class="no-data">Sin datos disponibles</p>';
        return;
    }

    // Filtrar datos con coordenadas válidas
    const validData = data.filter(d =>
        d.lat !== null && d.lon !== null &&
        !isNaN(d.lat) && !isNaN(d.lon) &&
        d.lat >= -90 && d.lat <= 90 &&
        d.lon >= -180 && d.lon <= 180
    );

    console.log('[Demises Heatmap] Datos recibidos:', data.length, 'Válidos:', validData.length);

    if (!validData.length) {
        container.innerHTML = '<p class="no-data">Sin coordenadas disponibles</p>';
        return;
    }

    // Crear mapa si no existe
    if (!demisesMap) {
        container.innerHTML = '';
        container.style.height = '500px';

        demisesMap = L.map('chart-demises-heatmap').setView([-9.19, -75.0152], 5);

        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; OpenStreetMap &copy; CARTO',
            maxZoom: 19
        }).addTo(demisesMap);
    }

    // Limpiar capas anteriores
    if (demisesHeatLayer) {
        demisesMap.removeLayer(demisesHeatLayer);
    }
    if (demisesMarkersLayer) {
        demisesMap.removeLayer(demisesMarkersLayer);
    }

    const maxTotal = d3.max(validData, d => d.total) || 1;

    // Capa de calor
    const heatData = validData.map(d => {
        const intensity = Math.pow(d.total / maxTotal, 0.5);
        return [d.lat, d.lon, intensity];
    });

    demisesHeatLayer = L.heatLayer(heatData, {
        radius: 45,
        blur: 35,
        minOpacity: 0.25,
        maxZoom: 12,
        max: 1.0,
        gradient: {
            0.0: '#2c3e50',
            0.2: '#8e44ad',
            0.4: '#9b59b6',
            0.6: '#e74c3c',
            0.8: '#c0392b',
            1.0: '#7b241c'
        }
    }).addTo(demisesMap);

    // Marcadores
    demisesMarkersLayer = L.layerGroup();

    validData.forEach(d => {
        const radius = Math.max(5, Math.min(15, Math.sqrt(d.total / maxTotal) * 15));

        const marker = L.circleMarker([d.lat, d.lon], {
            radius: radius,
            fillColor: '#b65959ff',
            color: '#fff',
            weight: 1,
            opacity: 0.8,
            fillOpacity: 0.6
        });

        const location = d.distrito
            ? `${d.distrito}, ${d.provincia}`
            : (d.provincia || d.departamento);

        marker.bindPopup(`
            <div style="text-align: center; min-width: 150px;">
                <strong>${d.departamento}</strong><br>
                ${d.provincia ? `<span style="font-size: 11px;">${d.provincia}</span><br>` : ''}
                ${d.distrito ? `<span style="font-size: 10px; color: #666;">${d.distrito}</span><br>` : ''}
                <hr style="margin: 5px 0; border-color: #ddd;">
                <b>Fallecidos:</b> ${formatNumber(d.total)}<br>
                <small style="color: #888;">Lat: ${d.lat.toFixed(4)}, Lon: ${d.lon.toFixed(4)}</small>
            </div>
        `);

        marker.bindTooltip(location, {
            permanent: false,
            direction: 'top',
            className: 'dept-tooltip'
        });

        demisesMarkersLayer.addLayer(marker);
    });

    demisesMarkersLayer.addTo(demisesMap);

    setTimeout(() => {
        demisesMap.invalidateSize();
    }, 100);
}

function renderDemisesDeptChart(data) {
    const container = d3.select('#chart-demises-dept');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

    const topData = data.slice(0, 10);

    const margin = { top: 20, right: 50, bottom: 40, left: 120 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 300;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleLinear()
        .domain([0, d3.max(topData, d => d.total)])
        .range([0, width]);

    const y = d3.scaleBand()
        .domain(topData.map(d => d.departamento))
        .range([0, height])
        .padding(0.2);

    // Barras con transición
    svg.selectAll('.bar')
        .data(topData)
        .join('rect')
        .attr('class', 'bar')
        .attr('x', 0)
        .attr('y', d => y(d.departamento))
        .attr('height', y.bandwidth())
        .attr('fill', COLORS.positive)
        .attr('width', 0)
        .transition()
        .duration(750)
        .delay((d, i) => i * 50)
        .attr('width', d => x(d.total));

    // Valores
    svg.selectAll('.value')
        .data(topData)
        .join('text')
        .attr('class', 'value')
        .attr('x', d => x(d.total) + 5)
        .attr('y', d => y(d.departamento) + y.bandwidth() / 2)
        .attr('dy', '0.35em')
        .attr('font-size', '11px')
        .attr('opacity', 0)
        .text(d => formatNumber(d.total))
        .transition()
        .delay(750)
        .attr('opacity', 1);

    // Ejes
    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(5).tickFormat(d3.format('.2s')));

    svg.append('g')
        .call(d3.axisLeft(y));
}

// ============== Responsive ==============

let resizeTimeout;
window.addEventListener('resize', () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
        // Solicitar datos frescos para re-renderizar
        socket.emit('request_refresh');
    }, 10000);
});

// ============== Inicialización ==============

document.addEventListener('DOMContentLoaded', () => {
    console.log('[Dashboard] Inicializando...');
    // Los datos se cargan automáticamente cuando el WebSocket se conecta

    // Botón de configuración de alertas (abrir sidebar)
    const toggleBtn = document.getElementById('toggle-alerts-config');
    if (toggleBtn) {
        toggleBtn.addEventListener('click', toggleAlertsConfig);
    }

    // Botón cerrar sidebar
    const closeBtn = document.getElementById('close-sidebar');
    if (closeBtn) {
        closeBtn.addEventListener('click', closeSidebar);
    }

    // Cerrar sidebar al hacer click en el overlay
    const overlay = document.getElementById('sidebar-overlay');
    if (overlay) {
        overlay.addEventListener('click', closeSidebar);
    }

    // Cerrar sidebar con tecla Escape
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            closeSidebar();
        }
    });

    // Buscador de departamentos
    const deptSearchInput = document.getElementById('dept-search-input');
    if (deptSearchInput) {
        deptSearchInput.addEventListener('input', (e) => {
            filterDepartmentList(e.target.value);
        });
    }
});
