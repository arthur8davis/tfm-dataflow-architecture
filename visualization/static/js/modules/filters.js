/**
 * Sistema de Filtros del Dashboard COVID-19
 */

import { state } from './config.js';
import { formatNumber, filterValidDepts, filterValidSex } from './utils.js';

export function renderDepartmentList() {
    const container = document.getElementById('dept-list');
    if (!container || !state.allDepartmentsData.length) return;

    const validDepts = state.allDepartmentsData.filter(d =>
        d.departamento &&
        d.departamento !== 'Sin especificar' &&
        d.departamento.trim() !== ''
    );
    const total = validDepts.reduce((sum, d) => sum + d.total, 0);

    let html = `
        <div class="dept-item ${state.selectedDepartment === 'TODOS' ? 'active' : ''}"
             data-dept="TODOS" onclick="selectDepartment('TODOS')">
            <span class="dept-name">Todos los departamentos</span>
            <span class="dept-count">${formatNumber(total)}</span>
        </div>
    `;

    validDepts.forEach(dept => {
        const isActive = state.selectedDepartment === dept.departamento;
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

export function renderSexList() {
    const container = document.getElementById('sex-list');
    if (!container || !state.allSexData.length) return;

    const validSexData = state.allSexData.filter(d =>
        d.sexo &&
        d.sexo !== 'Sin especificar' &&
        d.sexo.trim() !== ''
    );

    const total = validSexData.reduce((sum, d) => sum + d.total, 0);

    let html = `
        <div class="sex-item ${state.selectedSex === 'TODOS' ? 'active' : ''}"
             data-sex="TODOS" onclick="selectSex('TODOS')">
            <span class="sex-name">Todos</span>
            <span class="sex-count">${formatNumber(total)}</span>
        </div>
    `;

    validSexData.forEach(item => {
        const isActive = state.selectedSex === item.sexo;
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

export function updateFilterIndicator() {
    const indicator = document.getElementById('filter-indicator');
    const sexLabel = document.getElementById('filter-sex-label');
    const deptName = document.getElementById('filter-dept-name');

    if (!indicator) return;

    const hasSexFilter = state.selectedSex !== 'TODOS';
    const hasDeptFilter = state.selectedDepartment !== 'TODOS';

    if (!hasSexFilter && !hasDeptFilter) {
        indicator.style.display = 'none';
    } else {
        indicator.style.display = 'inline-flex';

        if (sexLabel) {
            sexLabel.textContent = hasSexFilter ? `${state.selectedSex}` : '';
        }
        if (deptName) {
            if (hasSexFilter && hasDeptFilter) {
                deptName.textContent = ` + ${state.selectedDepartment}`;
            } else if (hasDeptFilter) {
                deptName.textContent = state.selectedDepartment;
            } else {
                deptName.textContent = '';
            }
        }
    }
}

export function filterDepartmentList(searchTerm) {
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

export function updateSummaryCardsFromServer(data) {
    const casesTotal = document.getElementById('cases-total');
    const casesPositive = document.getElementById('cases-positive');
    const demisesTotal = document.getElementById('demises-total');
    const hospitalizationsTotal = document.getElementById('hospitalizations-total');

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

export function updateSummaryCards(data) {
    const casesTotal = document.getElementById('cases-total');
    const casesPositive = document.getElementById('cases-positive');
    const demisesTotal = document.getElementById('demises-total');
    const hospitalizationsTotal = document.getElementById('hospitalizations-total');
    const lastUpdated = document.getElementById('last-updated');

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

// Funciones de filtro que requieren socket y renderizadores
export function createFilterActions(socket, renderFunctions) {
    const {
        renderDepartmentChart,
        renderDemisesDeptChart,
        renderHeatmap,
        renderDemisesHeatmap,
        renderHospitalizationsHeatmap,
        renderSexChart,
        renderDemisesSexChart,
        renderTimelineChart,
        renderDemisesTimelineChart,
        renderAgeChart
    } = renderFunctions;

    function applyLocalFilters() {
        const validDepts = filterValidDepts(state.allDepartmentsData);
        const validDemisesDepts = filterValidDepts(state.allDemisesDeptData);
        const validHospDepts = filterValidDepts(state.allHospitalizationsDeptData);
        const validHeatmap = filterValidDepts(state.allHeatmapData);
        const validDemisesHeatmap = filterValidDepts(state.allDemisesHeatmapData);
        const validHospHeatmap = filterValidDepts(state.allHospitalizationsHeatmapData);
        const validSexData = filterValidSex(state.allSexData);
        const validDemisesSexData = filterValidSex(state.allDemisesSexData);

        renderDepartmentChart(validDepts);
        renderDemisesDeptChart(validDemisesDepts);
        renderHeatmap(validHeatmap);
        renderDemisesHeatmap(validDemisesHeatmap);
        renderHospitalizationsHeatmap(validHospHeatmap);
        renderSexChart(validSexData);
        renderDemisesSexChart(validDemisesSexData);
        renderTimelineChart(state.allTimelineData);
        renderDemisesTimelineChart(state.allDemisesTimelineData);

        const totalCases = validDepts.reduce((sum, d) => sum + (d.total || 0), 0);
        const totalPositive = validDepts.reduce((sum, d) => sum + (d.positivos || 0), 0);
        const totalDemises = validDemisesDepts.reduce((sum, d) => sum + (d.total || 0), 0);
        const totalHosp = validHospDepts.reduce((sum, d) => sum + (d.total || 0), 0);

        updateSummaryCardsFromServer({
            cases_total: totalCases,
            cases_positive: totalPositive,
            demises_total: totalDemises,
            hospitalizations_total: totalHosp
        });
    }

    function applyFilters() {
        if (state.selectedDepartment !== 'TODOS' || state.selectedSex !== 'TODOS') {
            console.log(`[Filtros] Solicitando datos filtrados: dept=${state.selectedDepartment}, sexo=${state.selectedSex}`);
            socket.emit('request_filtered_data', {
                departamento: state.selectedDepartment,
                sexo: state.selectedSex
            });
        } else {
            applyLocalFilters();
            renderAgeChart(state.allAgeData);
        }
    }

    return {
        applyFilters,
        applyLocalFilters,

        selectDepartment(dept) {
            state.selectedDepartment = dept;
            document.querySelectorAll('.dept-item').forEach(item => {
                item.classList.toggle('active', item.dataset.dept === dept);
            });
            updateFilterIndicator();
            applyFilters();
        },

        selectSex(sex) {
            state.selectedSex = sex;
            document.querySelectorAll('.sex-item').forEach(item => {
                item.classList.toggle('active', item.dataset.sex === sex);
            });
            updateFilterIndicator();
            applyFilters();
        },

        clearDeptFilter() {
            this.selectDepartment('TODOS');
        },

        clearAllFilters() {
            state.selectedDepartment = 'TODOS';
            state.selectedSex = 'TODOS';

            document.querySelectorAll('.dept-item').forEach(item => {
                item.classList.toggle('active', item.dataset.dept === 'TODOS');
            });
            document.querySelectorAll('.sex-item').forEach(item => {
                item.classList.toggle('active', item.dataset.sex === 'TODOS');
            });

            const indicator = document.getElementById('filter-indicator');
            if (indicator) {
                indicator.style.display = 'none';
            }

            applyFilters();
        }
    };
}
