"""
Generación de escenarios futuros.

Sección 5.13:
  5.13.1 - Simulación de tendencias (estimate_rt, generate_scenarios)
  5.13.2 - Indicadores derivados (pico, duplicación, demanda, inflexión)
"""
import math
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── 5.13.1 Simulación de tendencias ─────────────────────────────

def estimate_rt(daily_cases, serial_interval=5, window=7):
    """
    Estima Rt usando método simplificado de razón de casos.

    Rt = sum(casos[t-window:t]) / sum(casos[t-SI-window:t-SI])

    Args:
        daily_cases: Lista o array de casos diarios
        serial_interval: Intervalo serial en días (default 5)
        window: Tamaño de ventana en días (default 7)

    Returns:
        Lista de dicts con {day, rt, cases}
    """
    rt_values = []
    cases = list(daily_cases)

    for t in range(serial_interval + window, len(cases)):
        numerator = sum(cases[t - window:t])
        denominator = sum(cases[t - serial_interval - window:t - serial_interval])

        if denominator > 0:
            rt = numerator / denominator
            rt_values.append({
                'day': t,
                'rt': round(rt, 4),
                'cases': cases[t],
            })

    return rt_values


def generate_scenarios(daily_cases_df, horizon_days=30):
    """
    Genera escenarios optimista, base y pesimista usando Prophet.

    - Optimista: yhat_lower (Rt < 1.0, declive)
    - Base: yhat (Rt ≈ 1.0, tendencia actual)
    - Pesimista: yhat_upper (Rt > 1.5, aumento)

    Args:
        daily_cases_df: DataFrame con columnas 'ds' y 'total'
        horizon_days: Días a proyectar (14, 30 o 60)

    Returns:
        Dict con escenarios y metadata
    """
    from prophet import Prophet
    import logging as _log
    _log.getLogger('prophet').setLevel(_log.WARNING)
    _log.getLogger('cmdstanpy').setLevel(_log.WARNING)

    df = daily_cases_df[['ds', 'total']].rename(columns={'total': 'y'}).copy()
    df['ds'] = pd.to_datetime(df['ds'])

    model = Prophet(
        changepoint_prior_scale=0.05,
        seasonality_mode='multiplicative',
        yearly_seasonality=True,
        weekly_seasonality=True,
    )
    model.fit(df)

    future = model.make_future_dataframe(periods=horizon_days)
    forecast = model.predict(future)

    projection = forecast.tail(horizon_days).copy()

    scenarios = {
        'optimista': projection['yhat_lower'].clip(lower=0).tolist(),
        'base': projection['yhat'].clip(lower=0).tolist(),
        'pesimista': projection['yhat_upper'].clip(lower=0).tolist(),
        'dates': projection['ds'].dt.strftime('%Y-%m-%d').tolist(),
        'horizon_days': horizon_days,
    }

    # Agregar indicadores derivados
    scenarios['indicators'] = compute_derived_indicators(
        daily_cases=df['y'].tolist(),
        scenarios=scenarios,
    )

    logger.info(
        f"Escenarios generados: {horizon_days} días | "
        f"Base final: {scenarios['base'][-1]:.0f}"
    )

    return scenarios


# ── 5.13.2 Indicadores derivados ────────────────────────────────

def compute_derived_indicators(daily_cases, scenarios):
    """
    Calcula indicadores clave derivados de los escenarios.

    - Pico estimado: fecha y magnitud del máximo
    - Tiempo de duplicación: T = ln(2) / ln(1 + r)
    - Demanda hospitalaria: estimación por tasa de hospitalización
    - Punto de inflexión: cambio de tendencia ascendente a descendente
    """
    indicators = {}

    # Pico estimado por escenario
    for scenario_name in ['optimista', 'base', 'pesimista']:
        values = scenarios[scenario_name]
        dates = scenarios['dates']

        peak_idx = int(np.argmax(values))
        indicators[f'peak_{scenario_name}'] = {
            'date': dates[peak_idx],
            'value': round(values[peak_idx], 0),
            'day': peak_idx + 1,
        }

    # Tiempo de duplicación (basado en últimos 7 días históricos)
    if len(daily_cases) >= 14:
        c_t = sum(daily_cases[-7:])
        c_t_7 = sum(daily_cases[-14:-7])

        if c_t_7 > 0:
            r = (c_t - c_t_7) / c_t_7
            if r > 0:
                doubling_time = math.log(2) / math.log(1 + r)
                indicators['doubling_time_days'] = round(doubling_time, 1)
            elif r < 0:
                indicators['doubling_time_days'] = None  # Decreciendo
                indicators['halving_time_days'] = round(
                    abs(math.log(2) / math.log(1 + r)), 1
                )
            else:
                indicators['doubling_time_days'] = None  # Estable

    # Demanda hospitalaria estimada
    # Tasa de hospitalización aproximada por grupo de edad (datos COVID-19 Perú)
    hospitalization_rate = 0.05  # ~5% promedio
    for scenario_name in ['optimista', 'base', 'pesimista']:
        peak_value = indicators[f'peak_{scenario_name}']['value']
        indicators[f'hospital_demand_{scenario_name}'] = round(
            peak_value * hospitalization_rate, 0
        )

    # Punto de inflexión (donde la derivada cambia de signo en escenario base)
    base_values = scenarios['base']
    dates = scenarios['dates']
    inflection_point = find_inflection_point(base_values)
    if inflection_point is not None:
        indicators['inflection_point'] = {
            'date': dates[inflection_point],
            'day': inflection_point + 1,
            'value': round(base_values[inflection_point], 0),
        }

    # Rt actual estimado
    if len(daily_cases) >= 12:
        rt_values = estimate_rt(daily_cases)
        if rt_values:
            last_rt = rt_values[-1]['rt']
            indicators['current_rt'] = last_rt
            if last_rt < 1.0:
                indicators['trend'] = 'declining'
            elif last_rt > 1.2:
                indicators['trend'] = 'increasing'
            else:
                indicators['trend'] = 'stable'

    return indicators


def find_inflection_point(values):
    """Encuentra el punto de inflexión donde la segunda derivada cambia de signo."""
    if len(values) < 3:
        return None

    # Segunda derivada numérica
    second_deriv = []
    for i in range(1, len(values) - 1):
        d2 = values[i + 1] - 2 * values[i] + values[i - 1]
        second_deriv.append(d2)

    # Buscar cambio de signo
    for i in range(1, len(second_deriv)):
        if second_deriv[i - 1] * second_deriv[i] < 0:
            return i  # +1 por offset

    return None
