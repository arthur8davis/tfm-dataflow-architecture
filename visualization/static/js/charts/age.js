/**
 * Gráfico de Grupos de Edad - D3.js
 */

import { COLORS } from '../modules/config.js';

export function renderAgeChart(data) {
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

    // Barras
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
