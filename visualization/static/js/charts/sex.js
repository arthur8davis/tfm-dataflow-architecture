/**
 * Gráficos de Distribución por Sexo - D3.js
 */

import { COLORS } from '../modules/config.js';
import { formatNumber } from '../modules/utils.js';

export function renderSexChart(data) {
    const container = d3.select('#chart-sex');
    container.selectAll('*').remove();

    const filteredData = (data || []).filter(d =>
        d.sexo && d.sexo !== 'Sin especificar' && d.sexo.trim() !== ''
    );

    if (!filteredData || !filteredData.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

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

export function renderDemisesSexChart(data) {
    const container = d3.select('#chart-demises-sex');
    container.selectAll('*').remove();

    const filteredData = (data || []).filter(d =>
        d.sexo && d.sexo !== 'Sin especificar' && d.sexo.trim() !== ''
    );

    if (!filteredData || !filteredData.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

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
