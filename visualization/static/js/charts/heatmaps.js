/**
 * Mapas de Calor - Leaflet
 */

import { maps } from '../modules/config.js';
import { formatNumber } from '../modules/utils.js';

function createBaseMap(containerId, mapRef) {
    const container = document.getElementById(containerId);
    container.innerHTML = '';
    container.style.height = '500px';

    const map = L.map(containerId).setView([-9.19, -75.0152], 5);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap &copy; CARTO',
        maxZoom: 19
    }).addTo(map);

    return map;
}

function createPopupContent(d, type = 'casos') {
    const labels = {
        casos: { total: 'Total', positivos: 'Positivos' },
        fallecidos: { total: 'Fallecidos' },
        hospitalizaciones: { total: 'Hospitalizaciones' }
    };

    const label = labels[type];

    return `
        <div style="text-align: center; min-width: 150px;">
            <strong>${d.departamento}</strong><br>
            ${d.provincia ? `<span style="font-size: 11px;">${d.provincia}</span><br>` : ''}
            ${d.distrito ? `<span style="font-size: 10px; color: #666;">${d.distrito}</span><br>` : ''}
            <hr style="margin: 5px 0; border-color: #ddd;">
            <b>${label.total}:</b> ${formatNumber(d.total)}<br>
            ${d.positivos !== undefined ? `<b>${label.positivos}:</b> ${formatNumber(d.positivos)}<br>` : ''}
            <small style="color: #888;">Lat: ${d.lat.toFixed(4)}, Lon: ${d.lon.toFixed(4)}</small>
        </div>
    `;
}

function filterValidData(data) {
    return data.filter(d =>
        d.lat !== null && d.lon !== null &&
        !isNaN(d.lat) && !isNaN(d.lon) &&
        d.lat >= -90 && d.lat <= 90 &&
        d.lon >= -180 && d.lon <= 180
    );
}

export function renderHeatmap(data) {
    const container = document.getElementById('chart-heatmap');

    if (!data || !data.length) {
        container.innerHTML = '<p class="no-data">Sin datos disponibles</p>';
        return;
    }

    const validData = filterValidData(data);
    console.log('[Heatmap] Datos recibidos:', data.length, 'Válidos:', validData.length);

    if (!validData.length) {
        container.innerHTML = '<p class="no-data">Sin coordenadas disponibles</p>';
        return;
    }

    if (!maps.peruMap) {
        maps.peruMap = createBaseMap('chart-heatmap', maps);
    }

    if (maps.heatLayer) {
        maps.peruMap.removeLayer(maps.heatLayer);
    }
    if (maps.markersLayer) {
        maps.peruMap.removeLayer(maps.markersLayer);
    }

    const maxTotal = d3.max(validData, d => d.total) || 1;

    const heatData = validData.map(d => {
        const intensity = Math.pow(d.total / maxTotal, 0.5);
        return [d.lat, d.lon, intensity];
    });

    maps.heatLayer = L.heatLayer(heatData, {
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
    }).addTo(maps.peruMap);

    maps.markersLayer = L.layerGroup();

    validData.forEach(d => {
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

        marker.bindPopup(createPopupContent(d, 'casos'));
        marker.bindTooltip(location, {
            permanent: false,
            direction: 'top',
            className: 'dept-tooltip'
        });

        maps.markersLayer.addLayer(marker);
    });

    maps.markersLayer.addTo(maps.peruMap);

    setTimeout(() => {
        maps.peruMap.invalidateSize();
    }, 100);
}

export function renderDemisesHeatmap(data) {
    const container = document.getElementById('chart-demises-heatmap');

    if (!container) {
        console.log('[Demises Heatmap] Contenedor no encontrado');
        return;
    }

    if (!data || !data.length) {
        container.innerHTML = '<p class="no-data">Sin datos disponibles</p>';
        return;
    }

    const validData = filterValidData(data);
    console.log('[Demises Heatmap] Datos recibidos:', data.length, 'Válidos:', validData.length);

    if (!validData.length) {
        container.innerHTML = '<p class="no-data">Sin coordenadas disponibles</p>';
        return;
    }

    if (!maps.demisesMap) {
        maps.demisesMap = createBaseMap('chart-demises-heatmap', maps);
    }

    if (maps.demisesHeatLayer) {
        maps.demisesMap.removeLayer(maps.demisesHeatLayer);
    }
    if (maps.demisesMarkersLayer) {
        maps.demisesMap.removeLayer(maps.demisesMarkersLayer);
    }

    const maxTotal = d3.max(validData, d => d.total) || 1;

    const heatData = validData.map(d => {
        const intensity = Math.pow(d.total / maxTotal, 0.5);
        return [d.lat, d.lon, intensity];
    });

    maps.demisesHeatLayer = L.heatLayer(heatData, {
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
    }).addTo(maps.demisesMap);

    maps.demisesMarkersLayer = L.layerGroup();

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

        marker.bindPopup(createPopupContent(d, 'fallecidos'));
        marker.bindTooltip(location, {
            permanent: false,
            direction: 'top',
            className: 'dept-tooltip'
        });

        maps.demisesMarkersLayer.addLayer(marker);
    });

    maps.demisesMarkersLayer.addTo(maps.demisesMap);

    setTimeout(() => {
        maps.demisesMap.invalidateSize();
    }, 100);
}

export function renderHospitalizationsHeatmap(data) {
    const container = document.getElementById('chart-hospitalizations-heatmap');

    if (!container) {
        console.log('[Hospitalizations Heatmap] Contenedor no encontrado');
        return;
    }

    if (!data || !data.length) {
        container.innerHTML = '<p class="no-data">Sin datos disponibles</p>';
        return;
    }

    const validData = filterValidData(data);
    console.log('[Hospitalizations Heatmap] Datos recibidos:', data.length, 'Válidos:', validData.length);

    if (!validData.length) {
        container.innerHTML = '<p class="no-data">Sin coordenadas disponibles</p>';
        return;
    }

    if (!maps.hospitalizationsMap) {
        maps.hospitalizationsMap = createBaseMap('chart-hospitalizations-heatmap', maps);
    }

    if (maps.hospitalizationsHeatLayer) {
        maps.hospitalizationsMap.removeLayer(maps.hospitalizationsHeatLayer);
    }
    if (maps.hospitalizationsMarkersLayer) {
        maps.hospitalizationsMap.removeLayer(maps.hospitalizationsMarkersLayer);
    }

    const maxTotal = d3.max(validData, d => d.total) || 1;

    const heatData = validData.map(d => {
        const intensity = Math.pow(d.total / maxTotal, 0.5);
        return [d.lat, d.lon, intensity];
    });

    maps.hospitalizationsHeatLayer = L.heatLayer(heatData, {
        radius: 45,
        blur: 35,
        minOpacity: 0.25,
        maxZoom: 12,
        max: 1.0,
        gradient: {
            0.0: '#1a5276',
            0.2: '#2874a6',
            0.4: '#3498db',
            0.6: '#f39c12',
            0.8: '#e67e22',
            1.0: '#d35400'
        }
    }).addTo(maps.hospitalizationsMap);

    maps.hospitalizationsMarkersLayer = L.layerGroup();

    validData.forEach(d => {
        const radius = Math.max(5, Math.min(15, Math.sqrt(d.total / maxTotal) * 15));

        const marker = L.circleMarker([d.lat, d.lon], {
            radius: radius,
            fillColor: '#e67e22',
            color: '#fff',
            weight: 1,
            opacity: 0.8,
            fillOpacity: 0.6
        });

        const location = d.distrito
            ? `${d.distrito}, ${d.provincia}`
            : (d.provincia || d.departamento);

        marker.bindPopup(createPopupContent(d, 'hospitalizaciones'));
        marker.bindTooltip(location, {
            permanent: false,
            direction: 'top',
            className: 'dept-tooltip'
        });

        maps.hospitalizationsMarkersLayer.addLayer(marker);
    });

    maps.hospitalizationsMarkersLayer.addTo(maps.hospitalizationsMap);

    setTimeout(() => {
        maps.hospitalizationsMap.invalidateSize();
    }, 100);
}
