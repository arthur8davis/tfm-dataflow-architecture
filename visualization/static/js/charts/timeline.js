/**
 * Gráficos de Timeline - D3.js
 */

import { COLORS } from '../modules/config.js';
import { formatNumber } from '../modules/utils.js';

export function renderTimelineChart(data) {
    const container = d3.select('#chart-timeline');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

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

    const maxPositivos = d3.max(data, d => d.positivos) || 1;

    const x = d3.scaleBand()
        .domain(data.map(d => d.fecha))
        .range([0, width])
        .padding(0.1);

    const y = d3.scaleLinear()
        .domain([0, maxPositivos])
        .nice()
        .range([height, 0]);

    // Barras
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

    // Tooltip
    svg.selectAll('.bar')
        .on('mouseover', function (event, d) {
            d3.select(this).attr('fill', COLORS.warning);
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

    // Eje X
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

export function renderDemisesTimelineChart(data) {
    const container = d3.select('#chart-demises-timeline');
    container.selectAll('*').remove();

    if (!data || !data.length) {
        container.append('p').attr('class', 'no-data').text('Sin datos disponibles');
        return;
    }

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

    // Barras
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

    // Tooltip
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

    // Eje X
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
        .attr('y', -55)
        .attr('x', -height / 2)
        .attr('text-anchor', 'middle')
        .attr('font-size', '12px')
        .text('Fallecidos');
}
