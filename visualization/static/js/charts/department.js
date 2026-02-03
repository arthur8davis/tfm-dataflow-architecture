/**
 * Gráficos de Departamento - D3.js
 */

import { COLORS } from '../modules/config.js';
import { formatNumber } from '../modules/utils.js';

export function renderDepartmentChart(data) {
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

    // Barras totales
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

export function renderDemisesDeptChart(data) {
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

    // Barras
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
