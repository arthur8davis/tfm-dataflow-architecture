# Pendientes

## Faltantes sección 5.10 - Modelos analíticos y predictivos

### 1. Dependencias ML (requirements.txt)
- [ ] scikit-learn
- [ ] prophet
- [ ] xgboost
- [ ] statsmodels (ARIMA/SARIMA)
- [ ] tensorflow/keras (LSTM, Autoencoder)

### 2. Métricas descriptivas en tiempo real (DoFn en Beam)
- [x] Tasa de positividad por ventana temporal
- [x] Edad promedio por ventana
- [x] Distribución por sexo por ventana
- [x] Concentración geográfica por ventana
- [x] Distribución institucional por ventana
- [x] Tasa de mortalidad por ventana
- [x] Edad promedio de fallecidos por ventana
- [x] Distribución por clasificación de defunción
- [x] Defunciones por departamento por ventana

### 3. Detección de anomalías (transforms en pipeline)
- [ ] Z-Score (umbral |z| > 2)
- [ ] IQR - Rango Intercuartílico (outliers por cuartiles)
- [ ] CUSUM - Suma acumulativa (cambios de media)

### 4. Modelos predictivos
- [ ] Series temporales: ARIMA/SARIMA
- [ ] Series temporales: Prophet
- [ ] Series temporales: LSTM
- [ ] Clasificación: XGBoost
- [ ] Clasificación: Random Forest
- [ ] Clasificación: Regresión Logística
- [ ] Detección de anomalías ML: Isolation Forest
- [ ] Detección de anomalías ML: Autoencoder
- [ ] Workflow de entrenamiento offline → serialización → aplicación en pipeline

### 5. Generación de escenarios futuros
- [ ] Escenario optimista (Rt < 1.0) - Declive sostenido
- [ ] Escenario base (Rt ≈ 1.0) - Continuación de tendencia
- [ ] Escenario pesimista (Rt > 1.5) - Escenario de aumento
- [ ] Simulación con Prophet a 14, 30, 60 días
- [ ] Indicadores derivados: pico estimado, tiempo de duplicación, demanda hospitalaria, punto de inflexión

### 6. Integración online en pipeline Beam (Modo 1)
- [x] ComputeWindowMetrics como CombineFn para métricas en ventana
- [x] MetricsSink para escribir métricas a MongoDB (metrics_cases, metrics_demises)
- [x] Integración como rama paralela post-windowing en Cases y Demises
- [ ] Aplicación de modelos pre-entrenados como DoFn en ventanas temporales

### 7. Endpoints y visualizaciones en dashboard
- [ ] APIs para predicciones
- [ ] APIs para anomalías detectadas
- [ ] APIs para escenarios generados
- [ ] Gráficos de predicciones en D3.js
- [ ] Gráficos de anomalías en D3.js
- [ ] Visualización de escenarios futuros

## Mejoras Futuras
- Configurar alertas por schema
- Agregar metricas con Prometheus
- Implementar retry policies personalizadas
- Escalar con DataflowRunner en GCP
- Agregar tests unitarios por schema
- Agregar tests de integracion end-to-end
