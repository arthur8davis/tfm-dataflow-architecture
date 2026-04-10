/**
 * Métricas Descriptivas - Entry Point
 * Vista dedicada para métricas por ventana temporal
 */

import { COLORS } from './modules/config.js';
import { formatNumber, showConnectionStatus, showNotification } from './modules/utils.js';
import {
    renderMetricsSummaryCards,
    renderPositivityChart,
    renderInstitutionChart,
    renderWindowCountsChart
} from './charts/metrics.js';
import {
    renderAnomaliesSummaryCards,
    renderAnomaliesTimeline,
    renderAnomaliesTable,
    renderPredictionsSummaryCards,
    renderPredictionsRtChart,
    renderPredictionsForecast
} from './charts/analytics.js';

const socket = io();

// ============== Gráficos adicionales para esta vista ==============

function renderMetricsSexChart(data) {
    const container = d3.select('#chart-metrics-sex');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos');
        return;
    }

    const last = [...data].reverse().find(d => d.male_count != null);
    if (!last) return;

    const sexData = [
        { label: 'Masculino', value: last.male_count, color: COLORS.masculine },
        { label: 'Femenino', value: last.female_count, color: COLORS.feminine }
    ];

    const width = container.node().clientWidth;
    const height = 300;
    const radius = Math.min(width, height) / 2 - 20;

    const svg = container.append('svg')
        .attr('width', width).attr('height', height)
        .append('g')
        .attr('transform', `translate(${width / 2},${height / 2})`);

    const pie = d3.pie().value(d => d.value).sort(null);
    const arc = d3.arc().innerRadius(radius * 0.5).outerRadius(radius);

    svg.selectAll('path')
        .data(pie(sexData))
        .join('path')
        .attr('d', arc)
        .attr('fill', d => d.data.color)
        .attr('stroke', '#1a1a2e')
        .attr('stroke-width', 2)
        .transition().duration(750)
        .attrTween('d', function (d) {
            const i = d3.interpolate({ startAngle: 0, endAngle: 0 }, d);
            return t => arc(i(t));
        });

    // Labels
    const total = sexData.reduce((s, d) => s + d.value, 0);
    svg.selectAll('text.label')
        .data(pie(sexData))
        .join('text')
        .attr('class', 'label')
        .attr('transform', d => `translate(${arc.centroid(d)})`)
        .attr('text-anchor', 'middle')
        .attr('fill', '#fff')
        .attr('font-size', '12px')
        .text(d => `${d.data.label}: ${((d.data.value / total) * 100).toFixed(1)}%`);

    // Centro
    svg.append('text')
        .attr('text-anchor', 'middle')
        .attr('fill', '#ccc')
        .attr('font-size', '14px')
        .attr('dy', '-0.3em')
        .text(formatNumber(total));
    svg.append('text')
        .attr('text-anchor', 'middle')
        .attr('fill', '#888')
        .attr('font-size', '11px')
        .attr('dy', '1em')
        .text('registros');
}

function renderMetricsDeptChart(data) {
    const container = d3.select('#chart-metrics-departments');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos');
        return;
    }

    const last = [...data].reverse().find(d => d.top_departments && d.top_departments.length);
    if (!last) return;

    const depts = last.top_departments.map(d => ({
        name: d.department || d[0],
        count: d.count || d[1]
    }));

    const margin = { top: 20, right: 30, bottom: 40, left: 140 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = Math.max(200, depts.length * 45);

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const y = d3.scaleBand()
        .domain(depts.map(d => d.name))
        .range([0, height])
        .padding(0.2);

    const x = d3.scaleLinear()
        .domain([0, d3.max(depts, d => d.count)])
        .nice()
        .range([0, width]);

    svg.selectAll('.bar')
        .data(depts)
        .join('rect')
        .attr('class', 'bar')
        .attr('y', d => y(d.name))
        .attr('height', y.bandwidth())
        .attr('fill', COLORS.primary)
        .attr('x', 0)
        .attr('width', 0)
        .transition().duration(750).delay((_, i) => i * 80)
        .attr('width', d => x(d.count));

    // Labels con valores
    svg.selectAll('.bar-label')
        .data(depts)
        .join('text')
        .attr('class', 'bar-label')
        .attr('y', d => y(d.name) + y.bandwidth() / 2)
        .attr('x', d => x(d.count) + 5)
        .attr('dy', '0.35em')
        .attr('fill', '#ccc')
        .attr('font-size', '11px')
        .text(d => formatNumber(d.count));

    svg.append('g').call(d3.axisLeft(y));
    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(5).tickFormat(d3.format('.2s')));
}

function renderMetricsTable(data) {
    const tbody = document.getElementById('metrics-table-body');
    if (!tbody) return;

    if (!data || !data.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="no-data">Sin datos</td></tr>';
        return;
    }

    const sorted = [...data].sort((a, b) => (b.window_start || '').localeCompare(a.window_start || ''));

    tbody.innerHTML = sorted.map(d => {
        const topDept = d.top_departments && d.top_departments.length
            ? (d.top_departments[0].department || d.top_departments[0][0])
            : '-';
        return `<tr>
            <td>${d.window_start ? d.window_start.substring(11, 19) : '-'}</td>
            <td>${formatNumber(d.total)}</td>
            <td>${d.positivity_rate != null ? (d.positivity_rate * 100).toFixed(1) + '%' : '-'}</td>
            <td>${d.avg_age != null ? d.avg_age.toFixed(1) : '-'}</td>
            <td>${formatNumber(d.male_count || 0)}</td>
            <td>${formatNumber(d.female_count || 0)}</td>
            <td>${topDept}</td>
        </tr>`;
    }).join('');
}

// ============== WebSocket Events ==============

socket.on('connect', () => {
    showConnectionStatus(true);
    showNotification('Conectado - Métricas en tiempo real');
});

socket.on('disconnect', () => {
    showConnectionStatus(false);
});

socket.on('update_metrics_cases', (data) => {
    console.log('[Métricas] Cases:', data.length, 'ventanas');
    renderOrDefer('tab-descriptive', tagged('metrics-cases', () => {
        renderPositivityChart(data);
        renderInstitutionChart(data);
        renderWindowCountsChart(data);
        renderMetricsSexChart(data);
        renderMetricsDeptChart(data);
        renderMetricsTable(data);
    }));
});

socket.on('update_metrics_summary', (data) => {
    console.log('[Métricas] Resumen recibido');
    renderOrDefer('tab-descriptive', tagged('metrics-summary', () => {
        renderMetricsSummaryCards(data);
    }));
});

// Anomalías
socket.on('update_anomalies_cases', (data) => {
    console.log('[Anomalías] Cases:', data.length);
    renderOrDefer('tab-anomalies', tagged('anomalies-cases', () => {
        renderAnomaliesTimeline(data);
        renderAnomaliesTable(data);
    }));
});

socket.on('update_anomalies_summary', (data) => {
    console.log('[Anomalías] Resumen recibido');
    renderOrDefer('tab-anomalies', tagged('anomalies-summary', () => {
        renderAnomaliesSummaryCards(data);
    }));
});

// Predicciones
socket.on('update_predictions_cases', (data) => {
    console.log('[Predicciones] Cases:', data.length);
    renderOrDefer('tab-predictions', tagged('predictions-cases', () => {
        renderPredictionsRtChart(data);
        renderPredictionsForecast(data);
    }));
});

socket.on('update_predictions_summary', (data) => {
    console.log('[Predicciones] Resumen recibido');
    renderOrDefer('tab-predictions', tagged('predictions-summary', () => {
        renderPredictionsSummaryCards(data);
    }));
});

// ============== Carga inicial via REST ==============

async function loadInitialData() {
    try {
        const [casesRes, summaryRes, anomaliesCasesRes, anomaliesSummaryRes, predictionsCasesRes, predictionsSummaryRes] = await Promise.all([
            fetch('/api/metrics/cases'),
            fetch('/api/metrics/summary'),
            fetch('/api/anomalies/cases'),
            fetch('/api/anomalies/summary'),
            fetch('/api/predictions/cases'),
            fetch('/api/predictions/summary'),
        ]);
        const cases = await casesRes.json();
        const summary = await summaryRes.json();
        const anomaliesCases = await anomaliesCasesRes.json();
        const anomaliesSummary = await anomaliesSummaryRes.json();
        const predictionsCases = await predictionsCasesRes.json();
        const predictionsSummary = await predictionsSummaryRes.json();

        renderMetricsSummaryCards(summary);
        renderPositivityChart(cases);
        renderInstitutionChart(cases);
        renderWindowCountsChart(cases);
        renderMetricsSexChart(cases);
        renderMetricsDeptChart(cases);
        renderMetricsTable(cases);

        // Anomalías
        renderAnomaliesSummaryCards(anomaliesSummary);
        renderAnomaliesTimeline(anomaliesCases);
        renderAnomaliesTable(anomaliesCases);

        // Predicciones
        renderPredictionsSummaryCards(predictionsSummary);
        renderPredictionsRtChart(predictionsCases);
        renderPredictionsForecast(predictionsCases);
    } catch (e) {
        console.error('[Métricas] Error cargando datos iniciales:', e);
    }
}

// ============== Tab switching ==============

let activeTab = 'tab-descriptive';
const pendingRenders = {};  // Renderers pendientes para tabs no activos

function initTabs() {
    const buttons = document.querySelectorAll('.tab-btn');
    buttons.forEach(btn => {
        btn.addEventListener('click', () => {
            const tabId = btn.dataset.tab;
            if (tabId === activeTab) return;

            // Desactivar tab anterior
            document.querySelector('.tab-btn.active').classList.remove('active');
            document.getElementById(activeTab).classList.remove('active');

            // Activar nuevo tab
            btn.classList.add('active');
            document.getElementById(tabId).classList.add('active');
            activeTab = tabId;

            // Ejecutar renders pendientes del tab recién activado
            if (pendingRenders[tabId]) {
                console.log(`[Tabs] Ejecutando renders pendientes para ${tabId}`);
                pendingRenders[tabId].forEach(fn => fn());
                delete pendingRenders[tabId];
            }
        });
    });
}

function renderOrDefer(tabId, renderFn) {
    if (activeTab === tabId) {
        renderFn();
    } else {
        if (!pendingRenders[tabId]) pendingRenders[tabId] = [];
        // Reemplazar en vez de acumular (siempre el último dato)
        pendingRenders[tabId] = pendingRenders[tabId].filter(f => f._id !== renderFn._id);
        pendingRenders[tabId].push(renderFn);
    }
}

// Helpers para deferred rendering con ID
function tagged(id, fn) { fn._id = id; return fn; }

document.addEventListener('DOMContentLoaded', () => {
    console.log('[Métricas] Inicializando vista con tabs...');
    initTabs();
    loadInitialData();
});
