import polars as pl

from pathlib import Path

script_dir = Path(__file__).parent

filename = script_dir / 'dataset-casos.csv'

df = pl.read_csv(filename, has_header=True, separator=';', null_values=['NULL', 'null', 'NA', ''])

df = df.rename({
    'FECHA_CORTE': 'fecha_corte',
    'UUID': 'uuid',
    'FECHA_MUESTRA': 'fecha_muestra',
    'EDAD': 'edad',
    'SEXO': 'sexo',
    'INSTITIUTCION': 'institucion',
    'UBIGEO_PACIENTE': 'ubigeo_paciente',
    'DEPARTAMENTO_PACIENTE': 'departamento_paciente',
    'PROVINCIA_PACIENTE': 'provincia_paciente',
    'DISTRITO_PACIENTE': 'distrito_paciente',
    'DEPARTAMENTO_MUESTRA': 'departamento_muestra',
    'PROVINCIA_MUESTRA': 'provincia_muestra',
    'DISTRITO_MUESTRA': 'distrito_muestra',
    'TIPO_MUESTRA': 'tipo_muestra',
    'RESULTADO': 'resultado'
})

df_filter = df.select(pl.all().exclude(['fecha_corte']))
df_filter.write_parquet(script_dir / 'covid_cases.parquet')

print(f'columns: {df.columns}')

print(f'df_shape: {df_filter.shape}') # (1048575, 15)
print(f'df_head: {df_filter.head(5)}')
# print(f'df_len: {len(df)}')
