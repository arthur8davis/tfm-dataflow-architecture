/**
 * Gráficos de Métricas Descriptivas por Ventana - D3.js
 * Sección 5.10 - Modelos analíticos y predictivos
 */

import { COLORS } from '../modules/config.js';
import { formatNumber } from '../modules/utils.js';

/**
 * Renderiza las cards de métricas del resumen (última ventana + promedios)
 */
export function renderMetricsSummaryCards(data) {
    const container = document.getElementById('metrics-summary-cards');
    if (!container) return;
    container.innerHTML = '';

    if (!data || !data.cases_latest) {
        container.innerHTML = '<p class="no-data">Sin métricas disponibles</p>';
        return;
    }

    const latest = data.cases_latest;
    const agg = data.cases_aggregated || {};

    const cards = [
        {
            label: 'Tasa Positividad',
            value: `${(latest.positivity_rate * 100).toFixed(1)}%`,
            sub: `Promedio: ${((agg.avg_positivity || 0) * 100).toFixed(1)}%`,
            color: COLORS.positive
        },
        {
            label: 'Edad Promedio',
            value: latest.avg_age != null ? latest.avg_age.toFixed(1) : '-',
            sub: `Rango: ${latest.min_age || 0} - ${latest.max_age || 0}`,
            color: COLORS.info
        },
        {
            label: 'Ratio Masculino',
            value: `${(latest.male_ratio * 100).toFixed(1)}%`,
            sub: `M: ${formatNumber(latest.male_count)} / F: ${formatNumber(latest.female_count)}`,
            color: COLORS.masculine
        },
        {
            label: 'Ventanas Procesadas',
            value: formatNumber(agg.total_windows || 0),
            sub: `Total registros: ${formatNumber(agg.total_records || 0)}`,
            color: COLORS.success
        }
    ];

    cards.forEach(card => {
        const div = document.createElement('div');
        div.className = 'metric-card';
        div.innerHTML = `
            <div class="metric-label">${card.label}</div>
            <div class="metric-value" style="color: ${card.color}">${card.value}</div>
            <div class="metric-sub">${card.sub}</div>
        `;
        container.appendChild(div);
    });

    // Cards de demises si hay datos
    if (data.demises_latest) {
        const dl = data.demises_latest;
        const da = data.demises_aggregated || {};

        const demisesCards = [
            {
                label: 'Edad Promedio Fallecidos',
                value: dl.avg_age != null ? dl.avg_age.toFixed(1) : '-',
                sub: `Rango: ${dl.min_age || 0} - ${dl.max_age || 0}`,
                color: '#983f3f'
            },
            {
                label: 'Total Fallecidos (ventanas)',
                value: formatNumber(da.total_records || 0),
                sub: `${formatNumber(da.total_windows || 0)} ventanas`,
                color: '#983f3f'
            }
        ];

        demisesCards.forEach(card => {
            const div = document.createElement('div');
            div.className = 'metric-card';
            div.innerHTML = `
                <div class="metric-label">${card.label}</div>
                <div class="metric-value" style="color: ${card.color}">${card.value}</div>
                <div class="metric-sub">${card.sub}</div>
            `;
            container.appendChild(div);
        });
    }
}

/**
 * Gráfico de línea: Tasa de positividad por ventana temporal
 */
export function renderPositivityChart(data) {
    const container = d3.select('#chart-positivity');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos de métricas');
        return;
    }

    // Ordenar por ventana
    data.sort((a, b) => (a.window_start || '').localeCompare(b.window_start || ''));

    const margin = { top: 20, right: 80, bottom: 60, left: 70 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 300;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scalePoint()
        .domain(data.map((_, i) => i))
        .range([0, width]);

    const yPositivity = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.positivity_rate || 0) * 1.1])
        .nice()
        .range([height, 0]);

    const yAge = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.avg_age || 0) * 1.2])
        .nice()
        .range([height, 0]);

    // Línea de positividad
    const linePositivity = d3.line()
        .x((_, i) => x(i))
        .y(d => yPositivity(d.positivity_rate || 0))
        .curve(d3.curveMonotoneX);

    svg.append('path')
        .datum(data)
        .attr('fill', 'none')
        .attr('stroke', COLORS.positive)
        .attr('stroke-width', 2.5)
        .attr('d', linePositivity);

    // Puntos de positividad
    svg.selectAll('.dot-positivity')
        .data(data)
        .join('circle')
        .attr('class', 'dot-positivity')
        .attr('cx', (_, i) => x(i))
        .attr('cy', d => yPositivity(d.positivity_rate || 0))
        .attr('r', 4)
        .attr('fill', COLORS.positive)
        .on('mouseover', function (event, d) {
            d3.select(this).attr('r', 7);
            const tooltip = svg.append('g').attr('class', 'tooltip-group');
            const tx = x(data.indexOf(d));
            const ty = yPositivity(d.positivity_rate || 0) - 15;
            tooltip.append('text')
                .attr('x', tx).attr('y', ty)
                .attr('text-anchor', 'middle')
                .attr('fill', '#fff')
                .attr('font-size', '11px')
                .text(`${(d.positivity_rate * 100).toFixed(1)}% | Edad: ${d.avg_age || '-'} | n=${formatNumber(d.total)}`);
        })
        .on('mouseout', function () {
            d3.select(this).attr('r', 4);
            svg.selectAll('.tooltip-group').remove();
        });

    // Línea de edad promedio
    const lineAge = d3.line()
        .x((_, i) => x(i))
        .y(d => yAge(d.avg_age || 0))
        .curve(d3.curveMonotoneX);

    svg.append('path')
        .datum(data)
        .attr('fill', 'none')
        .attr('stroke', COLORS.info)
        .attr('stroke-width', 2)
        .attr('stroke-dasharray', '5,3')
        .attr('d', lineAge);

    // Ejes
    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x)
            .tickValues(x.domain().filter((_, i) => i % Math.max(1, Math.ceil(data.length / 8)) === 0))
            .tickFormat(i => `V${i + 1}`));

    svg.append('g')
        .call(d3.axisLeft(yPositivity).ticks(5).tickFormat(d => `${(d * 100).toFixed(0)}%`));

    svg.append('g')
        .attr('transform', `translate(${width},0)`)
        .call(d3.axisRight(yAge).ticks(5));

    // Labels de ejes
    svg.append('text')
        .attr('transform', 'rotate(-90)')
        .attr('y', -50).attr('x', -height / 2)
        .attr('text-anchor', 'middle')
        .attr('font-size', '12px')
        .attr('fill', COLORS.positive)
        .text('Tasa Positividad');

    svg.append('text')
        .attr('transform', 'rotate(90)')
        .attr('y', -width - 55).attr('x', height / 2)
        .attr('text-anchor', 'middle')
        .attr('font-size', '12px')
        .attr('fill', COLORS.info)
        .text('Edad Promedio');

    // Leyenda
    const legend = svg.append('g').attr('transform', `translate(${width - 180}, -5)`);
    legend.append('line').attr('x1', 0).attr('x2', 20).attr('y1', 0).attr('y2', 0)
        .attr('stroke', COLORS.positive).attr('stroke-width', 2.5);
    legend.append('text').attr('x', 25).attr('y', 4)
        .attr('font-size', '11px').attr('fill', '#ccc').text('Positividad');

    legend.append('line').attr('x1', 100).attr('x2', 120).attr('y1', 0).attr('y2', 0)
        .attr('stroke', COLORS.info).attr('stroke-width', 2).attr('stroke-dasharray', '5,3');
    legend.append('text').attr('x', 125).attr('y', 4)
        .attr('font-size', '11px').attr('fill', '#ccc').text('Edad');
}

/**
 * Gráfico de barras: Distribución por institución
 */
export function renderInstitutionChart(data) {
    const container = d3.select('#chart-institutions');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos de instituciones');
        return;
    }

    // Tomar la última ventana con datos de institución
    const lastWithInst = [...data].reverse().find(d => d.institution_distribution);
    if (!lastWithInst || !lastWithInst.institution_distribution) {
        container.append('p').attr('class', 'no-data').text('Sin datos de instituciones');
        return;
    }

    const instData = Object.entries(lastWithInst.institution_distribution)
        .map(([name, count]) => ({ name, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 8);

    const margin = { top: 20, right: 30, bottom: 100, left: 70 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 300;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleBand()
        .domain(instData.map(d => d.name))
        .range([0, width])
        .padding(0.2);

    const y = d3.scaleLinear()
        .domain([0, d3.max(instData, d => d.count)])
        .nice()
        .range([height, 0]);

    const colorScale = d3.scaleOrdinal()
        .domain(instData.map(d => d.name))
        .range(d3.schemeTableau10);

    svg.selectAll('.bar')
        .data(instData)
        .join('rect')
        .attr('class', 'bar')
        .attr('x', d => x(d.name))
        .attr('width', x.bandwidth())
        .attr('fill', d => colorScale(d.name))
        .attr('y', height)
        .attr('height', 0)
        .transition()
        .duration(750)
        .delay((_, i) => i * 50)
        .attr('y', d => y(d.count))
        .attr('height', d => height - y(d.count));

    // Tooltip
    svg.selectAll('.bar')
        .on('mouseover', function (event, d) {
            d3.select(this).attr('opacity', 0.7);
            const tooltip = svg.append('g').attr('class', 'tooltip-group')
                .attr('transform', `translate(${x(d.name) + x.bandwidth() / 2}, ${y(d.count) - 10})`);
            tooltip.append('text')
                .attr('text-anchor', 'middle')
                .attr('fill', '#fff')
                .attr('font-size', '11px')
                .text(formatNumber(d.count));
        })
        .on('mouseout', function () {
            d3.select(this).attr('opacity', 1);
            svg.selectAll('.tooltip-group').remove();
        });

    const xAxis = svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x));

    xAxis.selectAll('text')
        .attr('transform', 'rotate(-45)')
        .style('text-anchor', 'end')
        .attr('font-size', '9px');

    svg.append('g')
        .call(d3.axisLeft(y).ticks(5).tickFormat(d3.format('.2s')));
}

/**
 * Gráfico de barras: Registros por ventana temporal
 */
export function renderWindowCountsChart(data) {
    const container = d3.select('#chart-window-counts');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos de ventanas');
        return;
    }

    data.sort((a, b) => (a.window_start || '').localeCompare(b.window_start || ''));

    const margin = { top: 20, right: 30, bottom: 40, left: 70 };
    const width = container.node().clientWidth - margin.left - margin.right;
    const height = 250;

    const svg = container.append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleBand()
        .domain(data.map((_, i) => i))
        .range([0, width])
        .padding(0.1);

    const y = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.total)])
        .nice()
        .range([height, 0]);

    svg.selectAll('.bar')
        .data(data)
        .join('rect')
        .attr('class', 'bar')
        .attr('x', (_, i) => x(i))
        .attr('width', x.bandwidth())
        .attr('fill', COLORS.primary)
        .attr('y', height)
        .attr('height', 0)
        .transition()
        .duration(750)
        .delay((_, i) => i * 10)
        .attr('y', d => y(d.total))
        .attr('height', d => height - y(d.total));

    svg.selectAll('.bar')
        .on('mouseover', function (event, d) {
            d3.select(this).attr('fill', COLORS.warning);
            const idx = data.indexOf(d);
            const tooltip = svg.append('g').attr('class', 'tooltip-group')
                .attr('transform', `translate(${x(idx) + x.bandwidth() / 2}, ${y(d.total) - 10})`);
            tooltip.append('text')
                .attr('text-anchor', 'middle')
                .attr('fill', '#fff')
                .attr('font-size', '11px')
                .text(`${formatNumber(d.total)} registros`);
        })
        .on('mouseout', function () {
            d3.select(this).attr('fill', COLORS.primary);
            svg.selectAll('.tooltip-group').remove();
        });

    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(x)
            .tickValues(x.domain().filter((_, i) => i % Math.max(1, Math.ceil(data.length / 10)) === 0))
            .tickFormat(i => `V${i + 1}`));

    svg.append('g')
        .call(d3.axisLeft(y).ticks(5).tickFormat(d3.format('.2s')));

    svg.append('text')
        .attr('transform', 'rotate(-90)')
        .attr('y', -50).attr('x', -height / 2)
        .attr('text-anchor', 'middle')
        .attr('font-size', '12px')
        .text('Registros');
}
