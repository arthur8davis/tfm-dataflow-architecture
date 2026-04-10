/**
 * Gráficos D3.js para anomalías y predicciones
 * Sección 5.11.2 (Anomalías) y 5.12.3 (Predicciones)
 */
import { COLORS } from '../modules/config.js';
import { formatNumber } from '../modules/utils.js';

const SEVERITY_COLORS = {
    HIGH: '#ff4444',
    MEDIUM: '#ffaa00',
    LOW: '#44cc44',
};

const METHOD_LABELS = {
    zscore: 'Z-Score',
    iqr: 'IQR',
    cusum: 'CUSUM',
};

// ── Anomalías ────────────────────────────────────────────────

export function renderAnomaliesSummaryCards(summary) {
    const container = d3.select('#anomalies-summary-cards');
    container.selectAll('*').remove();

    if (!summary || (!summary.cases && !summary.demises)) {
        container.append('p').attr('class', 'no-data').text('Sin anomalías detectadas');
        return;
    }

    const cards = [];
    for (const schema of ['cases', 'demises']) {
        const data = summary[schema];
        if (!data) continue;
        cards.push({
            label: `Total ${schema}`,
            value: data.total || 0,
            color: data.total > 0 ? '#ff4444' : '#44cc44',
        });
        if (data.by_method) {
            data.by_method.forEach(m => {
                cards.push({
                    label: `${METHOD_LABELS[m.method] || m.method} (${m.severity})`,
                    value: m.count,
                    color: SEVERITY_COLORS[m.severity] || '#888',
                });
            });
        }
    }

    cards.forEach(card => {
        container.append('div')
            .attr('class', 'metric-card')
            .html(`
                <div class="metric-value" style="color:${card.color}">${formatNumber(card.value)}</div>
                <div class="metric-label">${card.label}</div>
            `);
    });
}

export function renderAnomaliesTimeline(anomalies) {
    const container = d3.select('#chart-anomalies-timeline');
    container.selectAll('*').remove();

    if (!anomalies || !anomalies.length) {
        container.append('p').attr('class', 'no-data').text('Sin anomalías');
        return;
    }

    const margin = { top: 20, right: 30, bottom: 40, left: 60 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 250;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const parseTime = d3.isoParse;
    const data = anomalies.map(d => ({
        ...d,
        date: parseTime(d.detected_at),
    })).filter(d => d.date);

    const x = d3.scaleTime()
        .domain(d3.extent(data, d => d.date))
        .range([0, width]);

    const y = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.actual_value || 0)])
        .nice()
        .range([height, 0]);

    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(6));

    svg.append('g').call(d3.axisLeft(y).ticks(5));

    svg.selectAll('circle')
        .data(data)
        .join('circle')
        .attr('cx', d => x(d.date))
        .attr('cy', d => y(d.actual_value || 0))
        .attr('r', d => d.severity === 'HIGH' ? 8 : 5)
        .attr('fill', d => SEVERITY_COLORS[d.severity] || '#888')
        .attr('opacity', 0.8)
        .attr('stroke', '#fff')
        .attr('stroke-width', 1)
        .append('title')
        .text(d => `${d.method} | ${d.severity} | valor=${d.actual_value}`);

    // Leyenda
    const legend = svg.append('g')
        .attr('transform', `translate(${width - 120}, 0)`);

    ['HIGH', 'MEDIUM'].forEach((sev, i) => {
        legend.append('circle')
            .attr('cx', 0).attr('cy', i * 20)
            .attr('r', 5).attr('fill', SEVERITY_COLORS[sev]);
        legend.append('text')
            .attr('x', 12).attr('y', i * 20 + 4)
            .attr('fill', '#ccc').attr('font-size', '11px')
            .text(sev);
    });
}

export function renderAnomaliesTable(anomalies) {
    const tbody = document.getElementById('anomalies-table-body');
    if (!tbody) return;

    if (!anomalies || !anomalies.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="no-data">Sin anomalías</td></tr>';
        return;
    }

    tbody.innerHTML = anomalies.slice(0, 50).map(a => {
        const details = [];
        if (a.z_score != null) details.push(`z=${a.z_score}`);
        if (a.iqr != null) details.push(`IQR=${a.iqr}`);
        if (a.direction) details.push(a.direction);
        if (a.cusum_pos != null) details.push(`S+=${a.cusum_pos}`);

        return `<tr>
            <td>${a.detected_at ? a.detected_at.substring(0, 19) : '-'}</td>
            <td>${METHOD_LABELS[a.method] || a.method}</td>
            <td style="color:${SEVERITY_COLORS[a.severity] || '#ccc'}">${a.severity}</td>
            <td>${formatNumber(a.actual_value || 0)}</td>
            <td>${details.join(', ') || '-'}</td>
        </tr>`;
    }).join('');
}


// ── Predicciones ─────────────────────────────────────────────

export function renderPredictionsSummaryCards(summary) {
    const container = d3.select('#predictions-summary-cards');
    container.selectAll('*').remove();

    if (!summary || Object.keys(summary).length === 0) {
        container.append('p').attr('class', 'no-data').text('Sin predicciones disponibles');
        return;
    }

    for (const [schema, data] of Object.entries(summary)) {
        const trendColor = data.trend === 'declining' ? '#44cc44'
            : data.trend === 'increasing' ? '#ff4444' : '#ffaa00';
        const trendLabel = data.trend === 'declining' ? 'Declive'
            : data.trend === 'increasing' ? 'Aumento' : 'Estable';

        container.append('div')
            .attr('class', 'metric-card')
            .html(`
                <div class="metric-value" style="color:${trendColor}">${data.current_rt != null ? data.current_rt.toFixed(2) : '-'}</div>
                <div class="metric-label">Rt ${schema}</div>
            `);

        container.append('div')
            .attr('class', 'metric-card')
            .html(`
                <div class="metric-value" style="color:${trendColor}">${trendLabel}</div>
                <div class="metric-label">Tendencia ${schema}</div>
            `);

        container.append('div')
            .attr('class', 'metric-card')
            .html(`
                <div class="metric-value">${data.avg_7d != null ? formatNumber(Math.round(data.avg_7d)) : '-'}</div>
                <div class="metric-label">Promedio 7d ${schema}</div>
            `);

        container.append('div')
            .attr('class', 'metric-card')
            .html(`
                <div class="metric-value">${data.growth_rate_7d != null ? (data.growth_rate_7d * 100).toFixed(1) + '%' : '-'}</div>
                <div class="metric-label">Tasa crecimiento ${schema}</div>
            `);
    }
}

export function renderPredictionsRtChart(predictions) {
    const container = d3.select('#chart-predictions-rt');
    container.selectAll('*').remove();

    if (!predictions || !predictions.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos de Rt');
        return;
    }

    const margin = { top: 20, right: 30, bottom: 40, left: 60 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 250;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const parseTime = d3.isoParse;
    const data = predictions
        .filter(d => d.current_rt != null)
        .map(d => ({ ...d, date: parseTime(d.predicted_at) }))
        .filter(d => d.date)
        .sort((a, b) => a.date - b.date);

    if (!data.length) {
        container.selectAll('*').remove();
        container.append('p').attr('class', 'no-data').text('Sin datos de Rt');
        return;
    }

    const x = d3.scaleTime()
        .domain(d3.extent(data, d => d.date))
        .range([0, width]);

    const y = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.current_rt) * 1.2])
        .nice()
        .range([height, 0]);

    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(6));

    svg.append('g').call(d3.axisLeft(y).ticks(5));

    // Línea Rt = 1.0 (referencia)
    svg.append('line')
        .attr('x1', 0).attr('x2', width)
        .attr('y1', y(1.0)).attr('y2', y(1.0))
        .attr('stroke', '#ffaa00')
        .attr('stroke-dasharray', '5,5')
        .attr('stroke-width', 1.5);

    svg.append('text')
        .attr('x', width - 40).attr('y', y(1.0) - 5)
        .attr('fill', '#ffaa00').attr('font-size', '10px')
        .text('Rt = 1.0');

    // Línea de Rt
    const line = d3.line()
        .x(d => x(d.date))
        .y(d => y(d.current_rt))
        .curve(d3.curveMonotoneX);

    svg.append('path')
        .datum(data)
        .attr('fill', 'none')
        .attr('stroke', COLORS.primary || '#00d4ff')
        .attr('stroke-width', 2)
        .attr('d', line);

    // Puntos
    svg.selectAll('circle')
        .data(data)
        .join('circle')
        .attr('cx', d => x(d.date))
        .attr('cy', d => y(d.current_rt))
        .attr('r', 4)
        .attr('fill', d => d.current_rt > 1.0 ? '#ff4444' : '#44cc44')
        .append('title')
        .text(d => `Rt = ${d.current_rt}`);
}

export function renderPredictionsForecast(predictions) {
    const container = d3.select('#chart-predictions-forecast');
    container.selectAll('*').remove();

    if (!predictions || !predictions.length) {
        container.append('p').attr('class', 'no-data').text('Sin forecast');
        return;
    }

    // Use the latest prediction's forecast_7d
    const latest = predictions.find(d => d.forecast_7d && d.forecast_7d.length);
    if (!latest) {
        container.append('p').attr('class', 'no-data').text('Sin forecast disponible');
        return;
    }

    const forecastData = latest.forecast_7d.map((v, i) => ({ day: i + 1, value: v }));

    const margin = { top: 20, right: 30, bottom: 40, left: 60 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 250;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleBand()
        .domain(forecastData.map(d => `Día ${d.day}`))
        .range([0, width])
        .padding(0.3);

    const y = d3.scaleLinear()
        .domain([0, d3.max(forecastData, d => d.value) * 1.1])
        .nice()
        .range([height, 0]);

    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x));

    svg.append('g').call(d3.axisLeft(y).ticks(5));

    svg.selectAll('.bar')
        .data(forecastData)
        .join('rect')
        .attr('x', d => x(`Día ${d.day}`))
        .attr('width', x.bandwidth())
        .attr('y', height)
        .attr('height', 0)
        .attr('fill', COLORS.primary || '#00d4ff')
        .attr('rx', 3)
        .transition().duration(750).delay((_, i) => i * 80)
        .attr('y', d => y(d.value))
        .attr('height', d => height - y(d.value));

    // Value labels
    svg.selectAll('.bar-label')
        .data(forecastData)
        .join('text')
        .attr('x', d => x(`Día ${d.day}`) + x.bandwidth() / 2)
        .attr('y', d => y(d.value) - 5)
        .attr('text-anchor', 'middle')
        .attr('fill', '#ccc')
        .attr('font-size', '11px')
        .text(d => formatNumber(Math.round(d.value)));

    // Avg line
    const avg = latest.avg_7d;
    if (avg != null) {
        svg.append('line')
            .attr('x1', 0).attr('x2', width)
            .attr('y1', y(avg)).attr('y2', y(avg))
            .attr('stroke', '#ffaa00')
            .attr('stroke-dasharray', '4,4')
            .attr('stroke-width', 1);
        svg.append('text')
            .attr('x', width - 80).attr('y', y(avg) - 5)
            .attr('fill', '#ffaa00').attr('font-size', '10px')
            .text(`Prom. 7d: ${formatNumber(Math.round(avg))}`);
    }
}
