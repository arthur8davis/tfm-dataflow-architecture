"""
Modelos predictivos para datos epidemiológicos.

Sección 5.12.1 - Tres familias de algoritmos:
  1. Series de tiempo: ARIMA/SARIMA, Prophet, LSTM (simplificado)
  2. Clasificación: XGBoost, Random Forest, Regresión Logística
  3. Detección de anomalías ML: Isolation Forest, Autoencoder (simplificado)
"""
import joblib
import logging
import numpy as np
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


# ── 1. Modelos de Series de Tiempo ──────────────────────────────

def train_arima(daily_df, order=(5, 1, 0), seasonal_order=(1, 1, 0, 7)):
    """Entrena modelo ARIMA/SARIMA para predicción de casos diarios."""
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    y = daily_df['total'].values

    model = SARIMAX(y, order=order, seasonal_order=seasonal_order,
                    enforce_stationarity=False, enforce_invertibility=False)
    result = model.fit(disp=False, maxiter=200)

    logger.info(f"ARIMA entrenado - AIC: {result.aic:.2f}")
    return result


def predict_arima(model, steps=14):
    """Genera predicciones con modelo ARIMA."""
    forecast = model.forecast(steps=steps)
    return np.maximum(forecast, 0).tolist()


def train_prophet(daily_df, changepoint_prior_scale=0.05):
    """Entrena modelo Prophet para predicción con intervalos de confianza."""
    from prophet import Prophet
    import logging as _log
    _log.getLogger('prophet').setLevel(_log.WARNING)
    _log.getLogger('cmdstanpy').setLevel(_log.WARNING)

    df = daily_df[['ds', 'total']].rename(columns={'total': 'y'}).copy()
    df['ds'] = pd.to_datetime(df['ds'])

    model = Prophet(
        changepoint_prior_scale=changepoint_prior_scale,
        seasonality_mode='multiplicative',
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
    )
    model.fit(df)

    logger.info("Prophet entrenado")
    return model


def predict_prophet(model, periods=14):
    """Genera predicciones con Prophet incluyendo intervalos de confianza."""
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    prediction = forecast.tail(periods)[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    prediction['yhat'] = prediction['yhat'].clip(lower=0)
    prediction['yhat_lower'] = prediction['yhat_lower'].clip(lower=0)
    prediction['yhat_upper'] = prediction['yhat_upper'].clip(lower=0)

    return prediction.to_dict('records')


# ── 2. Modelos de Clasificación ─────────────────────────────────

def train_xgboost(X_train, y_train):
    """Entrena XGBoost para predicción de resultado (positivo/negativo)."""
    from xgboost import XGBClassifier

    model = XGBClassifier(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        use_label_encoder=False,
        eval_metric='logloss',
        random_state=42,
    )
    model.fit(X_train, y_train)

    logger.info(f"XGBoost entrenado - {model.n_estimators} estimators")
    return model


def train_random_forest(X_train, y_train):
    """Entrena Random Forest para clasificación de riesgo."""
    from sklearn.ensemble import RandomForestClassifier

    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    logger.info(f"Random Forest entrenado - {model.n_estimators} estimators")
    return model


def train_logistic_regression(X_train, y_train):
    """Entrena Regresión Logística para estimación de probabilidad de positividad."""
    from sklearn.linear_model import LogisticRegression

    model = LogisticRegression(
        max_iter=1000,
        random_state=42,
        C=1.0,
    )
    model.fit(X_train, y_train)

    logger.info("Regresión Logística entrenada")
    return model


def evaluate_classifier(model, X_test, y_test):
    """Evalúa un clasificador y retorna métricas."""
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

    y_pred = model.predict(X_test)

    return {
        'accuracy': round(accuracy_score(y_test, y_pred), 4),
        'precision': round(precision_score(y_test, y_pred, zero_division=0), 4),
        'recall': round(recall_score(y_test, y_pred, zero_division=0), 4),
        'f1': round(f1_score(y_test, y_pred, zero_division=0), 4),
    }


# ── 3. Detección de Anomalías ML ────────────────────────────────

def train_isolation_forest(X_train, contamination=0.05):
    """Entrena Isolation Forest para detección de picos inusuales."""
    from sklearn.ensemble import IsolationForest

    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        random_state=42,
    )
    model.fit(X_train)

    logger.info(f"Isolation Forest entrenado - contamination={contamination}")
    return model


def train_autoencoder(X_train, encoding_dim=4, epochs=50):
    """Entrena Autoencoder simplificado con scikit-learn (MLPRegressor)
    para detección de anomalías en patrones multivariantes."""
    from sklearn.neural_network import MLPRegressor

    # Autoencoder como MLP que reconstruye su input
    model = MLPRegressor(
        hidden_layer_sizes=(encoding_dim,),
        activation='relu',
        max_iter=epochs,
        random_state=42,
    )
    model.fit(X_train, X_train)

    logger.info(f"Autoencoder entrenado - encoding_dim={encoding_dim}")
    return model


def detect_autoencoder_anomalies(model, X, threshold_percentile=95):
    """Detecta anomalías basándose en error de reconstrucción."""
    reconstructed = model.predict(X)
    mse = np.mean((X - reconstructed) ** 2, axis=1)
    threshold = np.percentile(mse, threshold_percentile)

    return {
        'anomaly_indices': np.where(mse > threshold)[0].tolist(),
        'reconstruction_errors': mse.tolist(),
        'threshold': float(threshold),
    }


# ── Serialización ────────────────────────────────────────────────

def save_model(model, path):
    """Serializa modelo con joblib."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
    logger.info(f"Modelo guardado: {path}")


def load_model(path):
    """Carga modelo serializado."""
    model = joblib.load(path)
    logger.info(f"Modelo cargado: {path}")
    return model
