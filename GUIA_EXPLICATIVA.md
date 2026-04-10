# Guía Explicativa: Secciones 5.10 – 5.13.2

Documento de referencia para la defensa del TFM.
Estructura alineada con el documento `doc_tfm/main.tex`.
Cada sección explica qué se implementó, dónde está el código, las fórmulas utilizadas,
cómo interpretar los resultados y cómo se visualiza en el dashboard.

---

## 5.10 Modelos analíticos y predictivos

### 5.10.1 Objetivos de los modelos analíticos y predictivos

El documento define cinco objetivos para la capa analítica del sistema. A continuación se explica cada uno, su implementación y cómo se evidencia en el proyecto.

#### Objetivo 1: Análisis descriptivo

**Qué dice el documento:** Caracterizar la evolución de la pandemia mediante métricas epidemiológicas calculadas por ventana temporal.

**Dónde se implementa:** `src/common/transforms/compute_metrics.py`

**Qué hace el código:** Dentro de cada ventana de 60 segundos del pipeline Apache Beam, se calculan métricas agregadas usando el patrón `CombineFn`. Hay dos clases separadas:
- `AggregateCasesMetrics` → tasa de positividad, edad media, distribución por sexo, concentración geográfica, distribución por institución
- `AggregateDemisesMetrics` → edad media de fallecidos, distribución por clasificación, mortalidad por departamento

**Cómo explicarlo al jurado:** "Cada 60 segundos, el pipeline toma todos los registros que llegaron en esa ventana y calcula métricas descriptivas como la tasa de positividad. Esto se hace dentro del pipeline en tiempo real usando el patrón CombineFn de Apache Beam, que permite distribuir el cálculo en paralelo."

**Visualización:** Vista `/metricas` → Tarjetas de resumen, gráfico de positividad dual-axis, distribución por institución, tabla de detalle por ventana.

---

#### Objetivo 2: Detección temprana

**Qué dice el documento:** Identificar cambios abruptos en tendencias de casos y defunciones que indiquen nuevas olas o brotes.

**Dónde se implementa:**
- `src/common/transforms/anomaly_detector.py` → Tres métodos estadísticos (Z-Score, IQR, CUSUM)
- `src/common/transforms/predictive_model.py:ApplyAnomalyDetection` → DoFn que integra el detector en el pipeline

**Cómo explicarlo al jurado:** "Implementamos tres métodos complementarios de detección de anomalías que corren en tiempo real. Z-Score detecta picos aislados, IQR es robusto ante distribuciones no normales, y CUSUM detecta cambios graduales de tendencia como el inicio de una nueva ola."

**Visualización:** Vista `/metricas` → Sección "Anomalías Detectadas": cards de resumen por método/severidad, scatter plot temporal, tabla de detalle.

---

#### Objetivo 3: Predicción a corto plazo

**Qué dice el documento:** Estimar la evolución de casos y defunciones en los próximos días/semanas para planificación sanitaria.

**Dónde se implementa:**
- `src/analytics/predictive_models.py` → ARIMA/SARIMA, Prophet, XGBoost, Random Forest, Regresión Logística
- `src/common/transforms/predictive_model.py:ApplyPredictiveModel` → DoFn que genera forecast de 7 días y estima Rt

**Cómo explicarlo al jurado:** "Los modelos se entrenan offline con datos históricos de MongoDB y se integran en el pipeline como DoFn. En cada ventana, el sistema estima el Rt actual y genera una proyección de 7 días basada en la tasa de crecimiento reciente."

**Visualización:** Vista `/metricas` → Sección "Predicciones y Tendencia": cards con Rt y tendencia, gráfico de evolución de Rt, barras de forecast 7 días.

---

#### Objetivo 4: Segmentación de riesgo

**Qué dice el documento:** Identificar grupos poblacionales de alto riesgo por edad, sexo y ubicación geográfica.

**Dónde se implementa:**
- `src/analytics/feature_engineering.py:create_age_groups()` → Segmenta en 0-19, 20-39, 40-59, 60-79, 80+
- `src/common/transforms/compute_metrics.py` → Métricas por sexo (`male_ratio`, `female_ratio`), por departamento (`top_departments`), por edad (`avg_age`, `min_age`, `max_age`)

**Cómo explicarlo al jurado:** "Las métricas por ventana desglosan los casos por grupo demográfico. El feature engineering crea variables derivadas como grupo de edad que alimentan los modelos de clasificación para predecir quién tiene mayor probabilidad de resultado positivo."

**Visualización:** Dashboard principal → Gráficos de edad, sexo (donut), departamento (barras); Vista `/metricas` → distribución por sexo e institución.

---

#### Objetivo 5: Soporte a la toma de decisiones

**Qué dice el documento:** Proporcionar información accionable para autoridades sanitarias.

**Dónde se implementa:**
- `visualization/app.py` → Dashboard completo con WebSocket en tiempo real
- `visualization/handlers/alerts.py` → Sistema de alertas con umbrales configurables
- `src/analytics/scenarios.py` → Escenarios futuros con indicadores de pico, duplicación y demanda hospitalaria

**Cómo explicarlo al jurado:** "Todo el sistema converge en un dashboard interactivo que se actualiza en tiempo real. Las autoridades sanitarias pueden ver el estado actual, recibir alertas cuando se detectan anomalías, y consultar proyecciones futuras para planificar recursos."

**Cadena de valor (Figura del documento):**
```
Ingesta (Kafka + Beam) → Almacenamiento (MongoDB) → Analítica (Modelos + Métricas) → Acción (Dashboards + Alertas)
  1,269,506 registros      Time-Series + DLQ         Descriptiva + Predictiva       Decisiones + Alertas
```

---

### 5.10.2 Integración de los modelos en la arquitectura Dataflow

El documento describe dos modos complementarios de integración.

#### Modo 1: Análisis Online (dentro del pipeline)

**Qué dice el documento:** Los modelos se ejecutan como DoFn adicionales dentro del pipeline Apache Beam, procesando datos en tiempo real a medida que pasan por las ventanas temporales.

**Dónde se implementa:**
- `pipelines/cases/pipeline.py` (líneas 93-130) → Rama analítica post-windowing
- `pipelines/demises/pipeline.py` (líneas 93-130) → Igual para fallecidos

**Flujo implementado:**
```
Windowed Data ──┬── [Rama principal] → Metadata → Batch → MongoDB
                │
                └── [Rama analítica]
                     │
                     ├── ComputeMetrics (CombineFn) → MetricsSink
                     │
                     └── AnalyticsBranch
                          ├── AnomalyDetection → AnomaliesSink
                          └── PredictiveModel  → PredictionsSink
```

**Función clave:**
```python
# src/common/transforms/compute_metrics.py
create_compute_metrics_branch(windowed_data, schema_name='cases')

# src/common/transforms/predictive_model.py
create_analytics_branch(windowed_metrics, schema_name='cases')
```

**Cómo explicarlo al jurado:** "Después del windowing, el pipeline se bifurca. La rama principal continúa hacia MongoDB. La rama analítica calcula métricas descriptivas con CombineFn, luego aplica detección de anomalías y modelos predictivos. Los resultados se almacenan en colecciones separadas: `metrics_cases`, `anomalies_cases`, `predictions_cases`."

**Por qué CombineFn y no DoFn para métricas:** "CombineFn permite que Beam distribuya el cálculo. Los registros se dividen en chunks, cada chunk genera un acumulador parcial, y luego los acumuladores se combinan. Esto escala horizontalmente sin importar el volumen."

---

#### Modo 2: Análisis Offline (post-procesamiento)

**Qué dice el documento:** Se ejecutan consultas y modelos sobre datos ya almacenados en MongoDB, aprovechando las colecciones Time-Series.

**Dónde se implementa:**
- `src/analytics/feature_engineering.py:load_cases_dataframe()` → Carga datos de MongoDB a DataFrame
- `src/analytics/predictive_models.py` → Entrenamiento de modelos sobre datos históricos
- `visualization/handlers/queries/` → Consultas REST para el dashboard

**Cómo explicarlo al jurado:** "El modo offline complementa al online. Mientras el pipeline calcula métricas en tiempo real, los modelos más complejos (ARIMA, Prophet, XGBoost) se entrenan offline con datos históricos y luego se serializan con joblib para ser cargados por el pipeline."

---

## 5.11 Análisis exploratorio en tiempo real

### 5.11.1 Métricas descriptivas en tiempo real

**Qué dice el documento:** Se calculan métricas descriptivas por ventana temporal (60 segundos por defecto) sobre los datos que fluyen por el pipeline.

**Archivo principal:** `src/common/transforms/compute_metrics.py`

#### Métricas del schema Cases (Tabla 5.11.1 del documento)

| Métrica | Fórmula | Implementación | Interpretación |
|---|---|---|---|
| Tasa de positividad | `(Positivos / Total) × 100` | `positivity_rate = positives / count` | Porcentaje de pruebas positivas. OMS recomienda < 5% para considerar la epidemia bajo control. Si sube, indica transmisión comunitaria activa. |
| Media de edad | `x̄ = (Σxᵢ) / n` | `avg_age = sum(ages) / len(ages)` | Edad promedio de los casos. Si baja durante un brote, la enfermedad se está expandiendo a población más joven. |
| Distribución por sexo | `n_sexo / n_total` | `male_ratio`, `female_ratio` | Permite detectar diferencias de incidencia por género para focalizar campañas. |
| Concentración geográfica | `Top-k por conteo` | `top_departments` (top-5) | Identifica los departamentos con más casos. Ayuda a priorizar asignación de recursos. |
| Tasa por institución | `n_inst / n_total` | `institution_distribution` | Muestra qué instituciones (MINSA, EsSalud, Privado) procesan más muestras. |

#### Métricas del schema Demises (Tabla 5.11.2 del documento)

| Métrica | Fórmula | Implementación | Interpretación |
|---|---|---|---|
| Tasa de mortalidad | `(Muertes / Casos) × 100` | `total` (conteo por ventana) | Magnitud de fallecidos por ventana. |
| Edad media de fallecidos | `x̄ = (Σxᵢ) / n` | `avg_age` | Los adultos mayores tienen mayor mortalidad. Si la edad media baja, la mortalidad está afectando a grupos más jóvenes. |
| Distribución por clasificación | `n_clasif / n_total` | `classification_distribution` | Tipos de criterio de defunción (virológico, serológico, clínico, etc.). |
| Mortalidad por departamento | `Conteo por departamento` | `top_departments` (top-5) | Identifica zonas con mayor mortalidad. |

#### Patrón CombineFn (código del documento línea 2159)

```python
class AggregateCasesMetrics(beam.CombineFn):
    def create_accumulator(self):   # → Estado inicial vacío {count:0, positives:0, ages:[], ...}
    def add_input(self, acc, elem): # → Acumula cada registro individual
    def merge_accumulators(self):   # → Combina acumuladores de diferentes workers
    def extract_output(self, acc):  # → Calcula métricas finales del acumulador combinado
```

**Cómo explicarlo al jurado:**
1. `create_accumulator()` — Crea un diccionario vacío con contadores en cero
2. `add_input()` — Por cada registro que llega a la ventana, incrementa contadores (positivos, edades, sexo, departamento)
3. `merge_accumulators()` — Si Beam distribuyó la ventana en 3 workers, cada uno tiene un acumulador parcial. Esta función los combina en uno solo
4. `extract_output()` — Del acumulador final, calcula las métricas: `positivity_rate = positives/count`, `avg_age = sum(ages)/len(ages)`, etc.

**Sink:** `src/common/sinks/metrics_sink.py` → Escribe a colecciones `metrics_cases` y `metrics_demises` en MongoDB con índices en `(window_start, schema)` y `computed_at`.

**Visualización en el dashboard:**

| Gráfico | Función D3.js | Qué muestra | Cómo se lee |
|---|---|---|---|
| Tarjetas de resumen | `renderMetricsSummaryCards()` | Positividad promedio, edad media, ratio por sexo, ventanas procesadas | Valores agregados de toda la historia de métricas |
| Positividad + Edad (dual-axis) | `renderPositivityChart()` | Línea sólida = positividad %, línea punteada = edad promedio | Si positividad sube y edad baja → brote expandiéndose a población joven |
| Registros por ventana | `renderWindowCountsChart()` | Barras = registros procesados por ventana | Ventanas vacías = posible caída del productor Kafka |
| Distribución por institución | `renderInstitutionChart()` | Top 8 instituciones por volumen de muestras | Identifica capacidad del sistema de salud por institución |
| Distribución por sexo (donut) | `renderMetricsSexChart()` | Proporción M/F en última ventana | Detecta diferencias de género |
| Top departamentos (barras) | `renderMetricsDeptChart()` | Top-5 departamentos por casos | Concentración geográfica del brote |
| Tabla de detalle | `renderMetricsTable()` | Todas las métricas por ventana en formato tabular | Permite análisis granular ventana por ventana |

**Tests:** `tests/test_compute_metrics.py` → 7 tests que cubren datos sintéticos, datos reales CSV (36,600 filas cases + 27,536 filas demises), merge de acumuladores e integración Beam.

---

### 5.11.2 Detección de anomalías simples

**Qué dice el documento:** Tres métodos estadísticos de bajo costo computacional que corren en tiempo real dentro del pipeline.

**Archivo principal:** `src/common/transforms/anomaly_detector.py`

#### Método 1: Z-Score por ventana móvil

**Fórmula (documento línea 2206):**
```
z = (x - μ) / σ

Donde:
  x = conteo de casos en la ventana actual
  μ = media de las últimas N ventanas (default N=20)
  σ = desviación estándar de las últimas N ventanas
```

**Regla de decisión:**
- |z| > 2.0 → Anomalía severidad MEDIUM
- |z| > 3.0 → Anomalía severidad HIGH

**Ejemplo numérico para el jurado:**
> Las últimas 20 ventanas procesaron en promedio 100 casos con σ=15.
> La ventana actual procesa 250 casos.
> z = (250 - 100) / 15 = 10.0 → **Anomalía HIGH** (10 desviaciones estándar sobre la media)

**Ventajas:** Simple, intuitivo, ampliamente usado en epidemiología y control de calidad.
**Limitaciones:** Asume distribución normal. Sensible a outliers en el historial. Si σ=0 (valores constantes), no puede calcular.

**Requisito mínimo:** 5 ventanas históricas con variación (σ ≠ 0).

---

#### Método 2: IQR (Rango Intercuartílico)

**Fórmula (documento línea 2216):**
```
Q1 = percentil 25 del historial
Q3 = percentil 75 del historial
IQR = Q3 - Q1

Límite inferior = Q1 - 1.5 × IQR
Límite superior = Q3 + 1.5 × IQR
```

**Regla de decisión:**
- x < límite inferior O x > límite superior → Anomalía MEDIUM
- x > Q3 + 3 × IQR → Anomalía HIGH (outlier extremo)

**Ejemplo numérico para el jurado:**
> Historial de ventanas ordenado: [..., 80, 90, 100, 110, 120, ...]
> Q1 = 80, Q3 = 120 → IQR = 40
> Límite inferior = 80 - 60 = 20
> Límite superior = 120 + 60 = 180
> Ventana actual: 250 → 250 > 180 → **Anomalía**
> ¿Es extrema? 250 > Q3 + 3×IQR = 120 + 120 = 240 → Sí → **HIGH**

**Ventajas:** No asume distribución normal. Robusto ante outliers previos en el historial.
**Limitaciones:** Requiere más datos históricos (mínimo 8 ventanas). Si IQR=0, no puede calcular.

---

#### Método 3: CUSUM (Suma Acumulativa)

**Fórmula (documento línea 2228):**
```
k = 0.5 × σ          (parámetro de holgura — sensibilidad)
h = 5.0 × σ          (umbral de alarma)

S⁺ₜ = max(0, S⁺ₜ₋₁ + (xₜ - μ) - k)     ← detecta incrementos sostenidos
S⁻ₜ = max(0, S⁻ₜ₋₁ - (xₜ - μ) - k)     ← detecta decrementos sostenidos

Alarma si: S⁺ₜ > h  (incremento)  o  S⁻ₜ > h  (decremento)
```

**Ejemplo numérico para el jurado:**
> Historial: μ=100, σ=20
> k = 0.5 × 20 = 10 (holgura)
> h = 5.0 × 20 = 100 (umbral)
>
> Día 1: x=115 → S⁺ = max(0, 0 + 15 - 10) = 5
> Día 2: x=120 → S⁺ = max(0, 5 + 20 - 10) = 15
> Día 3: x=125 → S⁺ = max(0, 15 + 25 - 10) = 30
> Día 4: x=130 → S⁺ = max(0, 30 + 30 - 10) = 50
> Día 5: x=135 → S⁺ = max(0, 50 + 35 - 10) = 75
> Día 6: x=140 → S⁺ = max(0, 75 + 40 - 10) = 105 > h=100 → **ALARMA (incremento)**

**Diferencia clave con Z-Score:** Z-Score detecta el día 1 si el salto es grande. CUSUM no dispara con el día 1 (solo +5), pero acumula evidencia de que la media cambió. Tras 6 días de incremento sostenido, la suma acumulada supera el umbral.

**Ventajas:** Detecta cambios de tendencia que Z-Score ignora. Ideal para detectar el inicio de una nueva ola pandémica.
**Limitaciones:** Más lento en reaccionar a picos aislados. Siempre severidad HIGH (cambio de régimen).
**Post-alarma:** Se resetea S⁺ (o S⁻) a 0 para detectar el siguiente cambio.

---

#### Comparación de los tres métodos

| Característica | Z-Score | IQR | CUSUM |
|---|---|---|---|
| Detecta picos aislados | Sí | Sí | No |
| Detecta cambios graduales | No | No | **Sí** |
| Asume distribución normal | Sí | **No** | Sí |
| Sensible a outliers previos | Sí | **No** | Parcial |
| Velocidad de detección | Inmediata | Inmediata | Acumulativa |
| Mínimo de datos | 5 ventanas | 8 ventanas | 5 ventanas |

**Cómo explicarlo al jurado:** "Usamos los tres métodos porque son complementarios. Un brote repentino (ej: evento de superpropagación) será detectado inmediatamente por Z-Score e IQR. Un cambio gradual de tendencia (ej: inicio de una nueva ola por relajación de medidas) será detectado por CUSUM tras acumular evidencia de varios días. Al combinarlos, minimizamos los falsos negativos."

**Visualización en el dashboard:**

| Gráfico | Función D3.js | Qué muestra | Cómo se lee |
|---|---|---|---|
| Cards de anomalías | `renderAnomaliesSummaryCards()` | Conteo por método y severidad | Rojo = HIGH, Naranja = MEDIUM |
| Scatter temporal | `renderAnomaliesTimeline()` | Cada punto = una anomalía en el tiempo | Clusters de puntos = período con múltiples anomalías |
| Tabla de detalle | `renderAnomaliesTable()` | Fecha, método, severidad, valor, detalles estadísticos | Permite verificar qué método detectó cada anomalía |

**Tests:** `tests/test_anomaly_detector.py` → 6 tests: detección de spike (Z-Score), outlier (IQR), cambio de media (CUSUM), datos estables sin falsos positivos, niveles de severidad, manejo de entrada nula.

---

### 5.11.3 Soporte a la visualización y toma de decisiones

**Qué dice el documento:** Las métricas en tiempo real alimentan dashboards para tomadores de decisiones. Se definen cuatro indicadores clave.

**Indicadores implementados (documento línea 2328):**

| Indicador del documento | Implementación | Visualización |
|---|---|---|
| Tasa de positividad | `positivity_rate` en métricas por ventana | Gráfico dual-axis en `/metricas` |
| Tendencia | `trend` en predicciones (declining/stable/increasing) basado en Rt | Cards de predicciones en `/metricas` |
| Hotspots geográficos | `top_departments` en métricas + `renderHeatmap()` | Mapas de calor Leaflet en dashboard principal |
| Alertas activas | `visualization/handlers/alerts.py` → umbrales configurables | Notificaciones en tiempo real + panel de alertas |

**Flujo del documento (Figura línea 2304):**
```
Pipeline Beam                    MongoDB Time-Series
(Métricas por ventana)  ───→    + Sistema de Alertas
                                       │
                        ┌──────────────┤
                        │              │
                  Dashboard        Notificaciones
                  Interactivo
```

**Cómo explicarlo al jurado:** "Las métricas calculadas en cada ventana se almacenan en MongoDB y simultáneamente se emiten vía WebSocket al dashboard. El polling loop en `app.py` detecta cambios cada 3 segundos y actualiza todos los gráficos. Las alertas se disparan cuando los valores cruzan umbrales configurables."

---

## 5.12 Modelos predictivos

**Qué dice el documento:** Tres familias de algoritmos para estimar la evolución futura de la pandemia.

### 5.12.1 Algoritmos utilizados

**Archivo principal:** `src/analytics/predictive_models.py`

#### Familia 1: Series de Tiempo (Tabla 5.12.1 del documento)

##### ARIMA/SARIMA

**Qué es:** AutoRegressive Integrated Moving Average con componente Seasonal. Modelo clásico de series temporales.

**Parámetros implementados:** ARIMA(5,1,0) × Seasonal(1,1,0,7)
- p=5 → Usa los últimos 5 valores para autoregresión
- d=1 → Una diferenciación para hacer la serie estacionaria (eliminar tendencia)
- q=0 → Sin componente de media móvil
- s=7 → Patrón estacional semanal (los lunes se reportan más casos que los domingos)
- P=1, D=1 → Captura y elimina el patrón semanal

**Implementación:**
```python
train_arima(daily_df, order=(5,1,0), seasonal_order=(1,1,0,7))
predict_arima(model, steps=14)  # Predicción 14 días
```

**Cuándo usarlo:** Predicción de casos diarios a corto plazo (7-14 días).

**Fortaleza:** Bien establecido estadísticamente, produce métricas de bondad de ajuste (AIC).
**Limitación:** Asume relaciones lineales. No captura cambios bruscos de tendencia.

**Cómo explicarlo al jurado:** "ARIMA descompone la serie temporal en tres componentes: autoregresión (la correlación con valores pasados), diferenciación (para eliminar la tendencia y hacer la serie estable), y media móvil. El componente seasonal con período 7 captura el patrón semanal de reporte."

---

##### Prophet (Facebook/Meta)

**Qué es:** Modelo bayesiano aditivo que descompone la serie en tendencia + estacionalidad + efectos especiales.

**Fórmula:**
```
y(t) = g(t) + s(t) + h(t) + ε

g(t) = tendencia (crecimiento con changepoints automáticos)
s(t) = estacionalidad (semanal + anual, modo multiplicativo)
h(t) = efectos de días festivos (no configurados en nuestra implementación)
ε    = ruido aleatorio
```

**Parámetros implementados:**
- `changepoint_prior_scale=0.05` → Controla flexibilidad de la tendencia. Valor bajo = tendencia suave, no sobreajusta a fluctuaciones diarias
- `seasonality_mode='multiplicative'` → Los picos estacionales escalan con la tendencia (apropiado cuando durante una ola grande, la variación semanal también es más grande)

**Implementación:**
```python
train_prophet(daily_df, changepoint_prior_scale=0.05)
predict_prophet(model, periods=14)  # Con intervalos de confianza
```

**Cuándo usarlo:** Proyecciones a mediano plazo (14-60 días) donde se necesitan intervalos de confianza.

**Fortaleza:** Maneja automáticamente estacionalidad, valores faltantes y outliers. Produce bandas de incertidumbre (`yhat_lower`, `yhat_upper`).
**Limitación:** Puede ser lento con series muy largas. No modela relaciones causales.

**Cómo explicarlo al jurado:** "Prophet de Facebook descompone la serie en una tendencia general (que puede tener puntos de cambio) más una estacionalidad multiplicativa. Lo usamos especialmente para generar los tres escenarios futuros, porque sus bandas de confianza dan directamente los escenarios optimista y pesimista."

---

##### LSTM (simplificado como Autoencoder)

**Qué dice el documento:** Red neuronal recurrente para capturar dependencias temporales de largo plazo.

**Implementación real:** Se implementó como `MLPRegressor` de scikit-learn en lugar de TensorFlow/Keras para evitar una dependencia pesada e incompatibilidad con Python 3.11.

```python
train_autoencoder(X_train, encoding_dim=4, epochs=50)
```

**Cómo explicarlo al jurado:** "El documento propone LSTM, pero para mantener el sistema liviano y compatible, implementamos una red neuronal simplificada (MLPRegressor) con arquitectura de autoencoder. Funciona como detector de anomalías: aprende a reconstruir datos normales, y cuando el error de reconstrucción es alto, indica anomalía."

---

#### Familia 2: Clasificación (Tabla 5.12.2 del documento)

##### XGBoost (Extreme Gradient Boosting)

**Qué es:** Ensamble de árboles de decisión entrenados secuencialmente. Cada árbol nuevo corrige los errores del anterior (gradient boosting).

**Parámetros:**
- `n_estimators=100` → 100 árboles en el ensamble
- `max_depth=6` → Profundidad máxima por árbol (controla complejidad)
- `learning_rate=0.1` → Cuánto contribuye cada árbol nuevo
- `eval_metric='logloss'` → Función de pérdida para clasificación binaria

**Uso en el proyecto:** Predecir si un caso será positivo o negativo basándose en edad, sexo, departamento, institución, tipo de muestra.

**Cómo explicarlo al jurado:** "XGBoost construye 100 árboles de decisión secuencialmente. El primer árbol hace una predicción inicial. El segundo árbol se enfoca en los casos que el primero clasificó mal. Y así sucesivamente. Es el algoritmo que gana la mayoría de competencias de ML en datos tabulares."

---

##### Random Forest (Bosque Aleatorio)

**Qué es:** Ensamble de árboles de decisión independientes. Cada árbol usa un subconjunto aleatorio de datos y variables. La predicción final es por votación mayoritaria.

**Parámetros:** `n_estimators=100`, `max_depth=10`

**Diferencia con XGBoost:** Los árboles son independientes (paralelos, no secuenciales). Más interpretable, menos propenso a sobreajuste, pero generalmente menos preciso.

**Uso en el proyecto:** Clasificación de nivel de riesgo por zona geográfica.

---

##### Regresión Logística

**Fórmula:**
```
P(positivo) = 1 / (1 + e^(-z))
z = β₀ + β₁×edad + β₂×sexo + ... + βₙ×xₙ
```

**Parámetros:** `C=1.0` (inverso de regularización), `max_iter=1000`

**Uso en el proyecto:** Estimar la probabilidad continua (0-1) de que un caso sea positivo.

**Cómo explicarlo al jurado:** "A diferencia de los árboles que dan una clasificación binaria, la regresión logística produce una probabilidad. Podemos decir 'este caso tiene un 73% de probabilidad de ser positivo'. Además, cada coeficiente β indica cuánto influye cada variable: si β_edad es positivo, significa que a mayor edad, mayor probabilidad de positivo."

---

#### Métricas de evaluación de clasificadores

**Función:** `evaluate_classifier(model, X_test, y_test)`

| Métrica | Fórmula | Interpretación para el jurado |
|---|---|---|
| **Accuracy** | (TP+TN) / Total | "De 100 predicciones, ¿cuántas fueron correctas?" |
| **Precision** | TP / (TP+FP) | "De los que predijo positivos, ¿cuántos realmente lo son?" Evita falsos positivos. |
| **Recall** | TP / (TP+FN) | "De los positivos reales, ¿cuántos detectó?" Evita falsos negativos. |
| **F1-Score** | 2×(P×R)/(P+R) | Media armónica. Balancea precision y recall. |

**Cómo explicarlo al jurado:** "En epidemiología, el Recall es más importante que la Precision. Es preferible tener algunos falsos positivos (personas sanas a las que se les hace test extra) que falsos negativos (personas infectadas que no se detectan y siguen contagiando)."

---

#### Familia 3: Detección de Anomalías ML (Tabla 5.12.3 del documento)

##### Isolation Forest

**Concepto:** Los datos anómalos son más fáciles de aislar que los normales.

**Cómo funciona:**
1. Construye 100 árboles aleatorios particionando los datos
2. Los datos normales necesitan muchas particiones para quedar solos
3. Los datos anómalos se aíslan con pocas particiones
4. Profundidad promedio de aislamiento < esperada → anomalía

**Parámetros:** `n_estimators=100`, `contamination=0.05` (5% esperado de anomalías)

**Diferencia con Z-Score/IQR:** Trabaja en múltiples dimensiones simultáneamente. Puede detectar anomalías que no son evidentes en ninguna variable individual pero sí en la combinación (ej: muchos casos en un departamento que normalmente tiene pocos, combinado con una edad promedio inusual).

---

##### Autoencoder (MLPRegressor)

**Arquitectura:**
```
Input (N features) → Capa oculta (4 neuronas) → Output (N features)
                     [cuello de botella]
```

**Fórmula de detección:**
```
error = MSE(entrada, salida) = Σ(xᵢ - x̂ᵢ)² / N
umbral = percentil_95(todos_los_errores)
anomalía si: error > umbral
```

**Cómo explicarlo al jurado:** "El autoencoder aprende a comprimir los datos a solo 4 dimensiones y reconstruirlos. Los datos normales se reconstruyen bien (bajo error). Cuando llega un dato anómalo, el modelo no sabe reconstruirlo correctamente y el error es alto. Fijamos el umbral en el percentil 95: el 5% con mayor error se clasifica como anomalía."

**Tests:** `tests/test_predictive_models.py` → 8 tests que cubren feature engineering, evaluación de clasificadores, Isolation Forest, autoencoder y serialización de modelos.

---

### 5.12.2 Variables de entrada

**Archivo principal:** `src/analytics/feature_engineering.py`

#### Variables del dataset Cases (Tabla 5.12.4 del documento)

| Variable | Tipo | Rol | Descripción |
|---|---|---|---|
| fecha_muestra | Temporal | Feature | Fecha de la prueba diagnóstica |
| edad | Numérica | Feature | Edad del paciente |
| sexo | Categórica | Feature | Género del paciente |
| institucion | Categórica | Feature | Tipo de centro de salud (MINSA, EsSalud, etc.) |
| departamento_paciente | Categórica | Feature | Ubicación geográfica |
| tipo_muestra | Categórica | Feature | Método diagnóstico (PCR, antígeno, etc.) |
| resultado | Categórica | **Target** | POSITIVO / NEGATIVO |

#### Variables del dataset Demises (Tabla 5.12.5 del documento)

| Variable | Tipo | Rol | Descripción |
|---|---|---|---|
| fecha_fallecimiento | Temporal | Feature | Fecha de la defunción |
| edad_declarada | Numérica | Feature | Edad del fallecido |
| sexo | Categórica | Feature | Género del fallecido |
| departamento | Categórica | Feature | Ubicación geográfica |
| clasificacion_def | Categórica | **Target** | Criterio de clasificación |

#### Variables derivadas (Feature Engineering, documento línea 2432)

| Variable | Fórmula | Función | Por qué se necesita |
|---|---|---|---|
| `dia_semana` | `fecha.dayofweek` (0=Lun, 6=Dom) | `add_derived_features()` | Los lunes se reportan más casos que los domingos. El modelo debe capturar este patrón. |
| `mes` | `fecha.month` (1-12) | `add_derived_features()` | Captura estacionalidad anual (invierno vs verano en Perú). |
| `casos_7d_rolling` | `mean(total[t-6:t])` | `add_derived_features()` | Suaviza el ruido diario. La media de 7 días elimina el efecto del fin de semana. |
| `tasa_positividad_rolling` | `sum(pos[t-6:t]) / sum(total[t-6:t])` | `add_derived_features()` | Tasa de positividad suavizada, más estable que la diaria. |
| `grupo_edad` | Bins: 0-19, 20-39, 40-59, 60-79, 80+ | `create_age_groups()` | Segmenta la población por riesgo. El grupo 60-79 tiene la mayor mortalidad. |
| `es_capital` | `departamento == 'LIMA'` → 1/0 | `add_derived_features()` | Lima concentra ~40% de los casos del país. Es un predictor fuerte. |
| `lag_1d` | `total[t-1]` | `add_derived_features()` | Lo que pasó ayer predice hoy (autocorrelación temporal). |
| `lag_7d` | `total[t-7]` | `add_derived_features()` | Lo que pasó hace una semana en el mismo día de la semana. |

**Preparación para clasificación:** `prepare_classification_features()` → Aplica one-hot encoding a variables categóricas y crea la variable target binaria (POSITIVO=1, NEGATIVO=0).

**Cómo explicarlo al jurado:** "Los datos brutos no son directamente útiles para ML. El feature engineering crea variables derivadas que capturan patrones temporales (día de la semana, lags) y demográficos (grupo de edad, si es de Lima). La media móvil de 7 días es especialmente importante porque elimina el artefacto de los fines de semana y muestra la tendencia real."

---

### 5.12.3 Integración con el pipeline

**Qué dice el documento:** Los modelos predictivos se integran como DoFn que aplica un modelo pre-entrenado sobre los datos de cada ventana.

**Archivo principal:** `src/common/transforms/predictive_model.py`

#### ApplyPredictiveModel DoFn (código del documento línea 2473)

```python
class ApplyPredictiveModel(beam.DoFn):
    def __init__(self, min_history=14):
        self._history = []          # Acumula totales de ventanas

    def process(self, metrics):
        self._history.append(metrics['total'])

        if len(self._history) >= 14:  # Suficiente historial
            # Estimar Rt
            rt_values = estimate_rt(self._history)

            # Calcular tasa de crecimiento semanal
            growth_rate = (avg_recent_7d - avg_previous_7d) / avg_previous_7d

            # Forecast 7 días
            forecast[día] = avg_7d × (1 + growth_rate)^(día/7)

            yield TaggedOutput('predictions', prediction)

        yield metrics  # Siempre emitir métricas al main
```

#### ApplyAnomalyDetection DoFn

```python
class ApplyAnomalyDetection(beam.DoFn):
    def setup(self):
        self._detector = AnomalyDetector(...)  # Inicializa una vez

    def process(self, metrics):
        for anomaly in self._detector.process(metrics):
            yield TaggedOutput('anomalies', anomaly)  # Anomalías al tag
        yield metrics  # Métricas al main
```

#### Workflow de entrenamiento y despliegue (documento línea 2506)

| Paso | Descripción | Implementación |
|---|---|---|
| 1. Extracción | Obtener datos históricos de MongoDB | `load_cases_dataframe()` |
| 2. Entrenamiento | Entrenar modelo offline | `train_arima()`, `train_prophet()`, `train_xgboost()`, etc. |
| 3. Serialización | Guardar modelo entrenado | `save_model(model, 'models/arima_cases.pkl')` usando joblib |
| 4. Despliegue | DoFn carga modelo en `setup()` | `ApplyPredictiveModel` acumula historial y genera predicciones |
| 5. Almacenamiento | Predicciones en MongoDB | `PredictionsSink` → colección `predictions_cases` |

#### Colecciones MongoDB generadas

| Colección | Sink | Contenido |
|---|---|---|
| `metrics_cases` | `MetricsSink` | Métricas descriptivas por ventana |
| `metrics_demises` | `MetricsSink` | Métricas de fallecidos por ventana |
| `anomalies_cases` | `AnomaliesSink` | Anomalías detectadas (método, severidad, valor, detalles) |
| `predictions_cases` | `PredictionsSink` | Predicciones (Rt, tendencia, forecast 7d) |

**Visualización en el dashboard:**

| Gráfico | Función D3.js | Qué muestra | Cómo se lee |
|---|---|---|---|
| Cards de predicciones | `renderPredictionsSummaryCards()` | Rt actual, tendencia, promedio 7d, tasa de crecimiento | Verde=declive, Naranja=estable, Rojo=aumento |
| Evolución de Rt (line chart) | `renderPredictionsRtChart()` | Línea temporal del Rt con referencia en Rt=1.0 | Por encima de la línea punteada = epidemia crece |
| Forecast 7 días (barras) | `renderPredictionsForecast()` | Proyección de casos próximos 7 días + promedio actual | Barras crecientes = se esperan más casos |

**Cómo explicarlo al jurado:** "El pipeline se bifurca después del windowing. Las métricas pasan primero por el detector de anomalías (que emite alertas al tag 'anomalies') y luego por el modelo predictivo (que emite forecast al tag 'predictions'). Los tagged outputs de Beam permiten esta bifurcación sin duplicar datos. Cada output va a un sink diferente en MongoDB."

---

## 5.13 Generación de escenarios futuros

**Qué dice el documento:** La generación de escenarios estima la evolución de la pandemia bajo diferentes supuestos para planificación sanitaria.

**Archivo principal:** `src/analytics/scenarios.py`

### 5.13.1 Simulación de tendencias

#### Tres escenarios (Tabla 5.13.1 del documento)

| Escenario | Supuesto | Rt | Banda de Prophet | Uso |
|---|---|---|---|---|
| **Optimista** | Medidas efectivas, tendencia a la baja | Rt < 1.0 | `yhat_lower` | Mejor caso: planificación de desescalada |
| **Base** | Tendencia actual se mantiene sin cambios | Rt ≈ 1.0 | `yhat` | Caso esperado: planificación operativa |
| **Pesimista** | Relajación de medidas o nueva variante | Rt > 1.5 | `yhat_upper` | Peor caso: preparación de contingencia |

#### Estimación de Rt (documento línea 2538)

**Fórmula del documento:**
```
Rt = It / Σ(s=1..S) I(t-s) × ws
```

**Fórmula simplificada implementada (razón de casos):**
```
Rt = Σ(casos[t-7 : t]) / Σ(casos[t-SI-7 : t-SI])

Donde:
  SI = intervalo serial = 5 días
  ventana = 7 días
```

**Intervalo serial SI=5:** Tiempo promedio entre que una persona se infecta y transmite el virus. Para COVID-19, estudios epidemiológicos (Li et al., 2020) estimaron ~5 días.

**Ventana de 7 días:** Suaviza la variabilidad diaria de reporte.

**Interpretación del Rt:**

| Valor Rt | Significado | Ejemplo |
|---|---|---|
| 0.5 | Cada 2 infectados generan solo 1 caso nuevo | Epidemia en franca caída |
| 0.8 | Declive lento | Medidas funcionando |
| 1.0 | Cada infectado genera exactamente 1 caso | Equilibrio |
| 1.2 | Crecimiento lento | Inicio de posible ola |
| 2.0 | Cada infectado genera 2 casos | Crecimiento exponencial |
| 3.0 | Cada infectado genera 3 casos | Brote descontrolado |

**Implementación:**
```python
def estimate_rt(daily_cases, serial_interval=5, window=7):
    for t in range(serial_interval + window, len(cases)):
        numerator = sum(cases[t - window : t])
        denominator = sum(cases[t - SI - window : t - SI])
        if denominator > 0:
            rt = numerator / denominator
```

**Cómo explicarlo al jurado:** "El Rt se calcula como la razón entre los casos de esta semana y los de la semana anterior desplazada por el intervalo serial. Si esta semana hay más casos que la anterior, Rt > 1 y la epidemia crece. Es el mismo indicador que usaron los gobiernos del mundo para decidir cuarentenas."

---

#### Generación de escenarios con Prophet

**Implementación:**
```python
def generate_scenarios(daily_cases_df, horizon_days=30):
    model = Prophet(
        changepoint_prior_scale=0.05,
        seasonality_mode='multiplicative',
        yearly_seasonality=True,
        weekly_seasonality=True,
    )
    model.fit(df)
    forecast = model.predict(future)

    scenarios = {
        'optimista': forecast['yhat_lower'],   # Banda inferior
        'base':      forecast['yhat'],          # Predicción central
        'pesimista': forecast['yhat_upper'],    # Banda superior
    }
```

**Horizontes de proyección:**
- **14 días:** Planificación operativa (turnos de personal, insumos de laboratorio)
- **30 días:** Planificación táctica (capacidad hospitalaria, compra de equipos)
- **60 días:** Planificación estratégica (políticas públicas, restricciones)

**Figura del documento (línea 2585):**
```
Casos
  │
  │    ╱╲        ╱ · · · · Pesimista (yhat_upper)
  │   ╱  ╲      ╱ - - - - Base (yhat)
  │  ╱    ╲    ╱ · · · · · Optimista (yhat_lower)
  │ ╱      ╲  ╱
  │╱        ╲╱
  ├─────────┼──────────────────── Tiempo
  │  Datos  │    Proyección
  │históricos│   (30-60 días)
            │
          "Hoy"
```

**Cómo explicarlo al jurado:** "Prophet genera una predicción central (escenario base) con bandas de confianza. La banda inferior es nuestro escenario optimista (las cosas van mejor de lo esperado) y la superior es el pesimista (las cosas van peor). Esto permite a las autoridades prepararse para el peor caso mientras esperan el mejor."

---

### 5.13.2 Uso en la toma de decisiones

**Qué dice el documento:** Los escenarios generados apoyan decisiones en tres áreas:
1. **Planificación de capacidad hospitalaria** (camas UCI, personal médico)
2. **Asignación de recursos sanitarios** (vacunas, pruebas diagnósticas)
3. **Políticas de salud pública** (restricciones, cuarentenas)

**Función principal:** `compute_derived_indicators(daily_cases, scenarios)`

#### Indicador 1: Pico estimado (documento línea 2651)

**Fórmula:**
```
pico = max(valores_escenario)
fecha_pico = fechas[argmax(valores_escenario)]
```

**Implementación:** Se calcula para cada escenario (optimista, base, pesimista).

**Ejemplo:** Si el escenario pesimista proyecta un máximo de 10,000 casos/día el 15 de mayo:
```python
indicators['peak_pesimista'] = {
    'date': '2024-05-15',
    'value': 10000,
    'day': 23  # Día 23 de la proyección
}
```

**Cómo explicarlo al jurado:** "El pico estimado responde a la pregunta '¿cuándo será el peor momento y qué tan grave será?'. Permite a los hospitales preparar capacidad antes de que llegue."

---

#### Indicador 2: Tiempo de duplicación (documento línea 2659)

**Fórmula:**
```
r = (C_semana_actual - C_semana_anterior) / C_semana_anterior

Si r > 0 (crecimiento):
    T_duplicación = ln(2) / ln(1 + r)

Si r < 0 (declive):
    T_reducción_mitad = |ln(2) / ln(1 + r)|
```

**Ejemplo numérico para el jurado:**
> Semana anterior: 700 casos totales
> Semana actual: 1,050 casos totales
> r = (1050 - 700) / 700 = 0.5 (50% de crecimiento semanal)
> T = ln(2) / ln(1.5) = 0.693 / 0.405 = **1.71 semanas ≈ 12 días**
>
> "Los casos se duplican cada 12 días"

**Si los casos decrecen:**
> Semana anterior: 1,000 casos
> Semana actual: 700 casos
> r = (700 - 1000) / 1000 = -0.3
> T = |ln(2) / ln(0.7)| = 0.693 / 0.357 = **1.94 semanas ≈ 14 días**
>
> "Los casos se reducen a la mitad cada 14 días"

**Cómo explicarlo al jurado:** "El tiempo de duplicación traduce la tasa de crecimiento a un número intuitivo. Decir 'r=0.5' no es claro para un tomador de decisiones. Decir 'los casos se duplican cada 12 días' sí lo es. Este indicador aparece frecuentemente en los reportes de la OMS."

---

#### Indicador 3: Demanda hospitalaria (documento línea 2661)

**Fórmula:**
```
demanda_hospitalaria = pico_escenario × tasa_hospitalización
tasa_hospitalización = 0.05  (5% promedio COVID-19 Perú)
```

**Implementación:**
```python
hospitalization_rate = 0.05
for scenario_name in ['optimista', 'base', 'pesimista']:
    peak_value = indicators[f'peak_{scenario_name}']['value']
    indicators[f'hospital_demand_{scenario_name}'] = peak_value * hospitalization_rate
```

**Ejemplo numérico para el jurado:**
> Pico escenario base: 5,000 casos/día
> Demanda = 5,000 × 0.05 = **250 camas hospitalarias necesarias**
>
> Pico escenario pesimista: 12,000 casos/día
> Demanda = 12,000 × 0.05 = **600 camas hospitalarias necesarias**

**Cómo explicarlo al jurado:** "Este indicador responde directamente a '¿cuántas camas necesitamos?'. La tasa de 5% es el promedio nacional de hospitalización por COVID-19 en Perú. Con los tres escenarios, el hospital puede preparar entre 250 (optimista) y 600 (pesimista) camas."

---

#### Indicador 4: Punto de inflexión (documento línea 2663)

**Fórmula (segunda derivada numérica):**
```
d²y/dt² = y[i+1] - 2×y[i] + y[i-1]

Punto de inflexión = donde d²y/dt² cambia de signo
```

**Qué significa:** El punto de inflexión marca el momento donde la curva pasa de **acelerarse** (segunda derivada positiva → cada día hay más casos nuevos que el día anterior) a **desacelerarse** (segunda derivada negativa → cada día hay menos casos nuevos que el día anterior).

**Implementación:**
```python
def find_inflection_point(values):
    for i in range(1, len(second_deriv)):
        if second_deriv[i-1] * second_deriv[i] < 0:  # Cambio de signo
            return i
```

**Ejemplo para el jurado:**
```
Día 1: 100 casos (+20 vs día anterior)  → acelerando
Día 2: 125 casos (+25 vs día anterior)  → acelerando más
Día 3: 145 casos (+20 vs día anterior)  → ← PUNTO DE INFLEXIÓN
Día 4: 155 casos (+10 vs día anterior)  → desacelerando
Día 5: 160 casos (+5 vs día anterior)   → desacelerando más
```

**Cómo explicarlo al jurado:** "El punto de inflexión es el momento donde las medidas de contención empiezan a hacer efecto visible. Antes del punto, los casos aceleran. Después, los casos siguen subiendo pero cada vez más despacio. Es como un coche que deja de acelerar: aún avanza, pero cada vez más lento hasta que se detiene (el pico)."

---

**Tests:** `tests/test_scenarios.py` → 14 tests que cubren:
- Cálculo de Rt con datos constantes, crecientes y decrecientes
- Detección de punto de inflexión
- Estimación de pico por escenario
- Tiempo de duplicación (creciente) y reducción (decreciente)
- Demanda hospitalaria
- Clasificación de tendencia según Rt

---

## Resumen: Mapeo Sección → Código → Visualización

| Sección | Código Principal | Visualización | Tests |
|---|---|---|---|
| 5.10.1 Objetivos | Todos los módulos | Dashboard + `/metricas` | Todos |
| 5.10.2 Integración Online | `pipelines/*/pipeline.py` | N/A (infraestructura) | `test_compute_metrics.py` |
| 5.10.2 Integración Offline | `src/analytics/feature_engineering.py` | Queries REST | `test_predictive_models.py` |
| 5.11.1 Métricas descriptivas | `src/common/transforms/compute_metrics.py` | Positividad, instituciones, ventanas, sexo, deptos | `test_compute_metrics.py` (7) |
| 5.11.2 Anomalías | `src/common/transforms/anomaly_detector.py` | Cards, scatter, tabla | `test_anomaly_detector.py` (6) |
| 5.11.3 Soporte decisiones | `visualization/handlers/alerts.py` | Alertas, notificaciones | — |
| 5.12.1 Algoritmos | `src/analytics/predictive_models.py` | — (entrenamiento offline) | `test_predictive_models.py` (8) |
| 5.12.2 Variables | `src/analytics/feature_engineering.py` | — (preparación datos) | `test_predictive_models.py` |
| 5.12.3 Integración pipeline | `src/common/transforms/predictive_model.py` | Rt chart, forecast bars, cards | — |
| 5.13.1 Simulación tendencias | `src/analytics/scenarios.py` | — (ejecución offline) | `test_scenarios.py` (14) |
| 5.13.2 Indicadores decisión | `src/analytics/scenarios.py:compute_derived_indicators()` | — (indicadores calculados) | `test_scenarios.py` |
