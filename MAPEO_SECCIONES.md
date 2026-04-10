# Mapeo Secciones 5.10 - 5.13.2 → Código Fuente

Relación entre lo descrito en el documento (doc_tfm/main.tex) y dónde se implementa en el proyecto.

---

## 5.10 Modelos analíticos y predictivos (línea 1967)

### 5.10.1 Objetivos de los modelos analíticos y predictivos (línea 1971)

| Objetivo | Estado | Implementación |
|---|---|---|
| Análisis descriptivo | IMPLEMENTADO | `src/common/transforms/compute_metrics.py` → `AggregateCasesMetrics`, `AggregateDemisesMetrics` |
| Detección temprana | IMPLEMENTADO | `src/common/transforms/anomaly_detector.py` → Z-Score, IQR, CUSUM |
| Predicción a corto plazo | IMPLEMENTADO | `src/analytics/predictive_models.py` → ARIMA, Prophet, XGBoost, RF, LR |
| Segmentación de riesgo | IMPLEMENTADO | Métricas por edad, sexo, departamento + `src/analytics/feature_engineering.py:create_age_groups()` |
| Soporte a toma de decisiones | IMPLEMENTADO | Dashboard en `visualization/` + vista `/metricas` con anomalías y predicciones |

### 5.10.2 Integración de los modelos en la arquitectura Dataflow (línea 2023)

| Modo | Estado | Implementación |
|---|---|---|
| Modo 1 - Online (DoFn en pipeline) | IMPLEMENTADO | Rama analítica post-windowing: `create_compute_metrics_branch()` + `create_analytics_branch()` en ambos pipelines |
| Modo 2 - Offline (queries MongoDB) | IMPLEMENTADO | `visualization/handlers/queries/metrics.py`, `anomalies.py`, `predictions.py` |

---

## 5.11 Análisis exploratorio en tiempo real (línea 2114)

### 5.11.1 Métricas descriptivas en tiempo real (línea 2118)

**Archivo principal:** `src/common/transforms/compute_metrics.py`

**Métricas Cases (tabla doc línea 2124):**

| Métrica | Estado | Implementación |
|---|---|---|
| Tasa de positividad | IMPLEMENTADO | `AggregateCasesMetrics.extract_output()` → `positivity_rate` |
| Media de edad | IMPLEMENTADO | `AggregateCasesMetrics.extract_output()` → `avg_age` |
| Distribución por sexo | IMPLEMENTADO | `AggregateCasesMetrics.extract_output()` → `male_ratio`, `female_ratio` |
| Concentración geográfica | IMPLEMENTADO | `AggregateCasesMetrics.extract_output()` → `top_departments` (top-5) |
| Tasa por institución | IMPLEMENTADO | `AggregateCasesMetrics.extract_output()` → `institution_distribution` |

**Métricas Demises (tabla doc línea 2142):**

| Métrica | Estado | Implementación |
|---|---|---|
| Tasa de mortalidad | PARCIAL | `AggregateDemisesMetrics.extract_output()` → `total` (falta ratio vs cases) |
| Edad media de fallecidos | IMPLEMENTADO | `AggregateDemisesMetrics.extract_output()` → `avg_age` |
| Distribución por clasificación | IMPLEMENTADO | `AggregateDemisesMetrics.extract_output()` → `classification_distribution` |
| Mortalidad por departamento | IMPLEMENTADO | `AggregateDemisesMetrics.extract_output()` → `top_departments` |

**CombineFn (código doc línea 2160):**
- Implementado en: `src/common/transforms/compute_metrics.py:AggregateCasesMetrics` y `AggregateDemisesMetrics`
- El doc muestra `AggregateWindowMetrics`, en el código se separó en dos clases para respetar campos de cada schema

**Sink de métricas:**
- `src/common/sinks/metrics_sink.py` → Escribe a `metrics_cases` y `metrics_demises` en MongoDB

**Tests de verificación:**
- `tests/test_compute_metrics.py` → 7 tests (sintéticos, datos reales CSV, integración Beam)

### 5.11.2 Detección de anomalías simples (línea 2202)

**Archivo principal:** `src/common/transforms/anomaly_detector.py`

| Método | Estado | Implementación |
|---|---|---|
| Z-Score por ventana móvil | IMPLEMENTADO | `AnomalyDetector._detect_zscore()` → z = (x - μ) / σ, umbral configurable |
| IQR (Rango Intercuartílico) | IMPLEMENTADO | `AnomalyDetector._detect_iqr()` → Q1 - 1.5*IQR, Q3 + 1.5*IQR |
| CUSUM (Suma Acumulativa) | IMPLEMENTADO | `AnomalyDetector._detect_cusum()` → S_t = max(0, S_{t-1} + (x - μ) - k) |

**Integración en pipeline:**
- `src/common/transforms/predictive_model.py:ApplyAnomalyDetection` → DoFn que usa `AnomalyDetector`
- `src/common/sinks/anomalies_sink.py` → Persiste a `anomalies_cases` y `anomalies_demises`
- Integrado en `pipelines/cases/pipeline.py` y `pipelines/demises/pipeline.py` vía `create_analytics_branch()`

**Tests:**
- `tests/test_anomaly_detector.py` → 6 tests (Z-Score spike, IQR outlier, CUSUM shift, estabilidad, severidad, None)

### 5.11.3 Soporte a la visualización y toma de decisiones (línea 2300)

| Indicador | Estado | Implementación |
|---|---|---|
| Tasa de positividad en dashboard | IMPLEMENTADO | `visualization/static/js/charts/metrics.js:renderPositivityChart()` |
| Tendencia (Rt y dirección) | IMPLEMENTADO | `src/common/transforms/predictive_model.py:ApplyPredictiveModel` → trend indicator |
| Hotspots geográficos | IMPLEMENTADO | `top_departments` en métricas + heatmap en dashboard principal |
| Alertas activas | IMPLEMENTADO | `visualization/handlers/alerts.py` + `visualization/static/js/modules/alerts.js` |
| Anomalías en dashboard | IMPLEMENTADO | `visualization/static/js/charts/analytics.js` → timeline, tabla, cards |
| Predicciones en dashboard | IMPLEMENTADO | `visualization/static/js/charts/analytics.js` → Rt chart, forecast bars |

---

## 5.12 Modelos predictivos (línea 2336)

### 5.12.1 Algoritmos utilizados (línea 2340)

**Archivo principal:** `src/analytics/predictive_models.py`

**Series de Tiempo:**

| Modelo | Estado | Implementación |
|---|---|---|
| ARIMA/SARIMA | IMPLEMENTADO | `train_arima()`, `predict_arima()` usando `statsmodels.tsa.statespace.sarimax` |
| Prophet | IMPLEMENTADO | `train_prophet()`, `predict_prophet()` usando `prophet` |
| LSTM (simplificado) | IMPLEMENTADO | `train_autoencoder()` usando `MLPRegressor` de sklearn (evita dependencia TensorFlow) |

**Clasificación:**

| Modelo | Estado | Implementación |
|---|---|---|
| XGBoost | IMPLEMENTADO | `train_xgboost()` usando `xgboost.XGBClassifier` |
| Random Forest | IMPLEMENTADO | `train_random_forest()` usando `sklearn.ensemble.RandomForestClassifier` |
| Regresión Logística | IMPLEMENTADO | `train_logistic_regression()` usando `sklearn.linear_model.LogisticRegression` |
| Evaluación | IMPLEMENTADO | `evaluate_classifier()` → accuracy, precision, recall, F1 |

**Detección de Anomalías ML:**

| Modelo | Estado | Implementación |
|---|---|---|
| Isolation Forest | IMPLEMENTADO | `train_isolation_forest()` usando `sklearn.ensemble.IsolationForest` |
| Autoencoder | IMPLEMENTADO | `train_autoencoder()` + `detect_autoencoder_anomalies()` usando `MLPRegressor` |

**Serialización:**
- `save_model()` y `load_model()` usando `joblib`

**Tests:**
- `tests/test_predictive_models.py` → 8 tests (feature engineering, clasificadores, Isolation Forest, autoencoder, serialización)

### 5.12.2 Variables de entrada (línea 2390)

**Archivo principal:** `src/analytics/feature_engineering.py`

| Variable | Estado | Implementación |
|---|---|---|
| Carga de Cases desde MongoDB | IMPLEMENTADO | `load_cases_dataframe()` |
| Carga de Demises desde MongoDB | IMPLEMENTADO | `load_demises_dataframe()` |
| Serie temporal diaria | IMPLEMENTADO | `create_daily_series()` → total, positivos por día |
| dia_semana, mes | IMPLEMENTADO | `add_derived_features()` |
| casos_7d_rolling | IMPLEMENTADO | `add_derived_features()` → rolling window 7 días |
| tasa_positividad_rolling | IMPLEMENTADO | `add_derived_features()` |
| lag_1d, lag_7d | IMPLEMENTADO | `add_derived_features()` → shift(1), shift(7) |
| grupo_edad | IMPLEMENTADO | `create_age_groups()` → 0-19, 20-39, 40-59, 60-79, 80+ |
| es_capital | IMPLEMENTADO | `add_derived_features()` → departamento == LIMA |
| Features para clasificación | IMPLEMENTADO | `prepare_classification_features()` → one-hot encoding + target |

### 5.12.3 Integración con el pipeline (línea 2444)

**Archivo principal:** `src/common/transforms/predictive_model.py`

| Componente | Estado | Implementación |
|---|---|---|
| `ApplyPredictiveModel` DoFn | IMPLEMENTADO | `ApplyPredictiveModel(beam.DoFn)` → acumula historial, genera forecast 7d y Rt |
| `ApplyAnomalyDetection` DoFn | IMPLEMENTADO | `ApplyAnomalyDetection(beam.DoFn)` → wrapper sobre `AnomalyDetector` |
| `create_analytics_branch()` | IMPLEMENTADO | Orquesta anomalías + predicciones como rama paralela |
| Colección `predictions` MongoDB | IMPLEMENTADO | `src/common/sinks/predictions_sink.py` → `predictions_cases`, `predictions_demises` |
| Colección `anomalies` MongoDB | IMPLEMENTADO | `src/common/sinks/anomalies_sink.py` → `anomalies_cases`, `anomalies_demises` |
| Integración en pipeline Cases | IMPLEMENTADO | `pipelines/cases/pipeline.py` → `create_analytics_branch()` post-métricas |
| Integración en pipeline Demises | IMPLEMENTADO | `pipelines/demises/pipeline.py` → `create_analytics_branch()` post-métricas |

---

## 5.13 Generación de escenarios futuros (línea 2516)

**Archivo principal:** `src/analytics/scenarios.py`

### 5.13.1 Simulación de tendencias (línea 2520)

| Componente | Estado | Implementación |
|---|---|---|
| Escenario Optimista (Rt < 1.0) | IMPLEMENTADO | `generate_scenarios()` → `yhat_lower` de Prophet |
| Escenario Base (Rt ≈ 1.0) | IMPLEMENTADO | `generate_scenarios()` → `yhat` de Prophet |
| Escenario Pesimista (Rt > 1.5) | IMPLEMENTADO | `generate_scenarios()` → `yhat_upper` de Prophet |
| Estimación de Rt | IMPLEMENTADO | `estimate_rt(daily_cases, serial_interval=5, window=7)` → razón de casos |
| Simulación con Prophet | IMPLEMENTADO | `generate_scenarios()` → Prophet con seasonality multiplicative |
| Proyecciones 14/30/60 días | IMPLEMENTADO | Parámetro `horizon_days` en `generate_scenarios()` |

### 5.13.2 Uso en la toma de decisiones (línea 2622)

**Función principal:** `compute_derived_indicators()`

| Indicador | Estado | Implementación |
|---|---|---|
| Pico estimado | IMPLEMENTADO | `peak_{optimista,base,pesimista}` → fecha, valor, día del pico |
| Tiempo de duplicación | IMPLEMENTADO | `doubling_time_days` → T = ln(2) / ln(1+r), basado en últimos 14 días |
| Tiempo de reducción | IMPLEMENTADO | `halving_time_days` → cuando r < 0 (casos decrecientes) |
| Demanda hospitalaria | IMPLEMENTADO | `hospital_demand_{optimista,base,pesimista}` → peak * 0.05 |
| Punto de inflexión | IMPLEMENTADO | `find_inflection_point()` → cambio de signo en segunda derivada |
| Rt actual | IMPLEMENTADO | `current_rt` vía `estimate_rt()` |
| Tendencia | IMPLEMENTADO | `trend` → declining/stable/increasing según Rt |

**Tests:**
- `tests/test_scenarios.py` → 14 tests (Rt cálculo, inflexión, pico, duplicación, hospitalaria, tendencia)

---

## Secciones de Visualización (ya implementadas)

### 5.14 Visualización interactiva y monitorización (línea 2667)

Implementado en: `visualization/app.py`

### 5.14.1 Arquitectura del sistema de visualización (línea 2671)

| Componente | Implementación |
|---|---|
| Backend Flask + Socket.IO | `visualization/app.py` |
| Frontend D3.js + Leaflet | `visualization/static/js/` |
| Conexión MongoDB | `visualization/services/database.py` |
| Polling de cambios | `visualization/app.py:refresh_loop()` |

### 5.14.2 Diseño de dashboards (línea 2739)

| Componente | Implementación |
|---|---|
| KPIs (cards resumen) | `visualization/templates/index.html` + `visualization/static/js/modules/filters.js:updateSummaryCards()` |
| Filtros interactivos | `visualization/static/js/modules/filters.js` |
| Vista principal | `http://localhost:5006/` → `visualization/templates/index.html` |
| Vista métricas + analytics | `http://localhost:5006/metricas` → `visualization/templates/metricas.html` |

### 5.14.3 Reportes y visualizaciones (línea 2783)

| Reporte | Implementación |
|---|---|
| Casos por Departamento | `visualization/handlers/queries/cases.py:get_cases_by_department()` + `charts/department.js` |
| Distribución por Sexo | `visualization/handlers/queries/cases.py:get_cases_by_sex()` + `charts/sex.js` |
| Evolución Temporal | `visualization/handlers/queries/cases.py:get_cases_by_date()` + `charts/timeline.js` |
| Casos por Edad | `visualization/handlers/queries/cases.py:get_cases_by_age()` + `charts/age.js` |
| Fallecidos por Depto | `visualization/handlers/queries/demises.py:get_demises_by_department()` + `charts/department.js` |
| Mapas de Calor | `visualization/handlers/queries/cases.py:get_heatmap_data()` + `charts/heatmaps.js` |
| Métricas por Ventana | `visualization/handlers/queries/metrics.py` + `charts/metrics.js` |
| Anomalías detectadas | `visualization/handlers/queries/anomalies.py` + `charts/analytics.js` |
| Predicciones y Rt | `visualization/handlers/queries/predictions.py` + `charts/analytics.js` |

### 5.14.4 Comunicación en tiempo real (línea 3352)

| Componente | Implementación |
|---|---|
| Flujo de actualización | `visualization/app.py:refresh_loop()` → polling cada 3s |
| Eventos WebSocket | `visualization/handlers/websocket/events.py` |
| Cliente Socket.IO (principal) | `visualization/static/js/main.js` |
| Cliente Socket.IO (métricas) | `visualization/static/js/metricas.js` |
| Eventos anomalías | `update_anomalies_cases`, `update_anomalies_summary` |
| Eventos predicciones | `update_predictions_cases`, `update_predictions_summary` |

### 5.14.5 Sistema de alertas (línea 3395)

| Componente | Implementación |
|---|---|
| Arquitectura de alertas | `visualization/handlers/alerts.py` |
| Tipos de alertas | `alerts_config` en `visualization/handlers/alerts.py` |
| Niveles de severidad | `check_threshold_alert()` → warning y critical |
| Gestión thread-safe | `alerts_lock` en `visualization/handlers/alerts.py` |
| Configuración dinámica | `update_alert_config()` + endpoint `/api/alerts/config` |
| Historial | `alerts_history` lista (max 50) |
| Visualización de alertas | `visualization/static/js/modules/alerts.js` |
| Notificaciones en tiempo real | `showAlertNotification()` + evento WebSocket `threshold_alert` |

---

## Resumen de Estado

| Sección | Descripción | Estado |
|---|---|---|
| 5.10 | Modelos analíticos y predictivos | IMPLEMENTADO |
| 5.11.1 | Métricas descriptivas en tiempo real | IMPLEMENTADO |
| 5.11.2 | Detección de anomalías simples | IMPLEMENTADO |
| 5.11.3 | Soporte a visualización | IMPLEMENTADO |
| 5.12.1 | Algoritmos predictivos | IMPLEMENTADO |
| 5.12.2 | Variables de entrada (feature engineering) | IMPLEMENTADO |
| 5.12.3 | Integración modelos en pipeline | IMPLEMENTADO |
| 5.13.1 | Simulación de tendencias | IMPLEMENTADO |
| 5.13.2 | Uso en toma de decisiones | IMPLEMENTADO |
| 5.14 | Visualización interactiva | IMPLEMENTADO |
| 5.14.1 | Arquitectura visualización | IMPLEMENTADO |
| 5.14.2 | Diseño de dashboards | IMPLEMENTADO |
| 5.14.3 | Reportes y visualizaciones | IMPLEMENTADO |
| 5.14.4 | Comunicación en tiempo real | IMPLEMENTADO |
| 5.14.5 | Sistema de alertas | IMPLEMENTADO |

---

## Dependencias ML (requirements.txt)

| Paquete | Versión | Uso |
|---|---|---|
| scikit-learn | 1.3.2 | RandomForest, LogisticRegression, IsolationForest, MLPRegressor (Autoencoder) |
| statsmodels | 0.14.1 | ARIMA/SARIMA |
| prophet | 1.1.5 | Escenarios futuros, predicciones |
| xgboost | 2.0.3 | Clasificación XGBoost |
| joblib | 1.3.2 | Serialización de modelos |
| numpy | 1.24.4 | Cálculos numéricos |
| pandas | 2.0.3 | DataFrames, feature engineering |

## Tests

| Archivo | Tests | Cobertura |
|---|---|---|
| `tests/test_compute_metrics.py` | 7 | Métricas descriptivas (5.11.1) |
| `tests/test_anomaly_detector.py` | 6 | Detección anomalías (5.11.2) |
| `tests/test_predictive_models.py` | 8 | Feature engineering + modelos ML (5.12) |
| `tests/test_scenarios.py` | 14 | Escenarios, Rt, indicadores (5.13) |
| **Total** | **35** | Secciones 5.10-5.13.2 |
