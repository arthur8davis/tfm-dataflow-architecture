/**
 * Configuración y constantes del Dashboard COVID-19
 */

export const COLORS = {
    masculine: '#50aae5ff',
    feminine: '#d497acff',
    primary: '#4a90d9',
    positive: '#e74c3c',
    success: '#27ae60',
    warning: '#f39c12',
    info: '#3498db',
    dark: '#2c3e50'
};

// Estado global de la aplicación
export const state = {
    isConnected: false,
    alertsConfig: {},
    alertsGlobalDate: null,
    activeAlerts: [],
    selectedDepartment: 'TODOS',
    selectedSex: 'TODOS',
    // Datos cacheados
    allDepartmentsData: [],
    allDemisesDeptData: [],
    allHeatmapData: [],
    allDemisesHeatmapData: [],
    allTimelineData: [],
    allDemisesTimelineData: [],
    allAgeData: [],
    allSexData: [],
    allDemisesSexData: [],
    allHospitalizationsDeptData: [],
    allHospitalizationsHeatmapData: []
};

// Mapas Leaflet (referencias globales)
export const maps = {
    peruMap: null,
    heatLayer: null,
    markersLayer: null,
    demisesMap: null,
    demisesHeatLayer: null,
    demisesMarkersLayer: null,
    hospitalizationsMap: null,
    hospitalizationsHeatLayer: null,
    hospitalizationsMarkersLayer: null
};
