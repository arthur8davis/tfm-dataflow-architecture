"""
Feature engineering para modelos predictivos.

Sección 5.12.2 - Variables derivadas:
  - dia_semana, mes
  - casos_7d_rolling, tasa_positividad_rolling
  - grupo_edad, es_capital
  - lag_1d, lag_7d
"""
import pandas as pd
import numpy as np
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)


def load_cases_dataframe(connection_string, database, collection='cases'):
    """Carga datos de cases desde MongoDB a un DataFrame."""
    client = MongoClient(connection_string)
    db = client[database]

    cursor = db[collection].find(
        {},
        {'fecha_muestra': 1, 'edad': 1, 'sexo': 1, 'resultado': 1,
         'departamento_paciente': 1, 'institucion': 1, 'tipo_muestra': 1,
         '_id': 0}
    )
    df = pd.DataFrame(list(cursor))
    client.close()

    if df.empty:
        return df

    # Parsear fecha
    df['fecha'] = pd.to_numeric(df['fecha_muestra'], errors='coerce')
    df['fecha'] = pd.to_datetime(df['fecha'].astype(str), format='%Y%m%d', errors='coerce')
    df = df.dropna(subset=['fecha'])

    return df


def load_demises_dataframe(connection_string, database, collection='demises'):
    """Carga datos de demises desde MongoDB a un DataFrame."""
    client = MongoClient(connection_string)
    db = client[database]

    cursor = db[collection].find(
        {},
        {'fecha_fallecimiento': 1, 'edad_declarada': 1, 'sexo': 1,
         'clasificacion_def': 1, 'departamento': 1, '_id': 0}
    )
    df = pd.DataFrame(list(cursor))
    client.close()

    if df.empty:
        return df

    df['fecha'] = pd.to_numeric(df['fecha_fallecimiento'], errors='coerce')
    df['fecha'] = pd.to_datetime(df['fecha'].astype(str), format='%Y%m%d', errors='coerce')
    df = df.dropna(subset=['fecha'])

    return df


def create_daily_series(df, date_col='fecha', result_col='resultado'):
    """Crea serie temporal diaria de casos totales y positivos."""
    daily = df.groupby(date_col).agg(
        total=pd.NamedAgg(column=date_col, aggfunc='count'),
        positivos=pd.NamedAgg(
            column=result_col,
            aggfunc=lambda x: (x == 'POSITIVO').sum()
        ) if result_col in df.columns else pd.NamedAgg(column=date_col, aggfunc='count')
    ).reset_index()
    daily = daily.rename(columns={date_col: 'ds'})
    daily = daily.sort_values('ds').reset_index(drop=True)
    return daily


def add_derived_features(daily_df):
    """Agrega variables derivadas al DataFrame diario (sección 5.12.2)."""
    df = daily_df.copy()

    # Temporales
    df['dia_semana'] = df['ds'].dt.dayofweek
    df['mes'] = df['ds'].dt.month

    # Rolling
    df['casos_7d_rolling'] = df['total'].rolling(window=7, min_periods=1).mean()

    if 'positivos' in df.columns:
        df['tasa_positividad_rolling'] = (
            df['positivos'].rolling(window=7, min_periods=1).sum() /
            df['total'].rolling(window=7, min_periods=1).sum()
        ).fillna(0)

    # Lags
    df['lag_1d'] = df['total'].shift(1).fillna(0)
    df['lag_7d'] = df['total'].shift(7).fillna(0)

    # Capital
    if 'departamento' in df.columns:
        df['es_capital'] = (df['departamento'] == 'LIMA').astype(int)

    return df


def create_age_groups(edad_series):
    """Categoriza edad en rangos: 0-19, 20-39, 40-59, 60-79, 80+."""
    bins = [0, 20, 40, 60, 80, 200]
    labels = ['0-19', '20-39', '40-59', '60-79', '80+']
    return pd.cut(edad_series, bins=bins, labels=labels, right=False)


def prepare_classification_features(df):
    """Prepara features para modelos de clasificación (resultado POSITIVO/NEGATIVO)."""
    features_df = df.copy()

    # Edad numérica
    features_df['edad'] = pd.to_numeric(features_df.get('edad', 0), errors='coerce').fillna(0)

    # Grupo de edad
    features_df['grupo_edad'] = create_age_groups(features_df['edad'])

    # Encoding categóricas
    cat_cols = ['sexo', 'institucion', 'departamento_paciente', 'tipo_muestra', 'grupo_edad']
    for col in cat_cols:
        if col in features_df.columns:
            features_df[col] = features_df[col].astype(str)
            dummies = pd.get_dummies(features_df[col], prefix=col, drop_first=True)
            features_df = pd.concat([features_df, dummies], axis=1)

    # Target
    if 'resultado' in features_df.columns:
        features_df['target'] = (features_df['resultado'] == 'POSITIVO').astype(int)

    return features_df
