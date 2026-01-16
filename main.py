import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Importar configuración y builder del pipeline
from config import dev, prod
from dataflow_model.pipeline import build_pipeline_cases, build_pipeline_cases_native

def run(env='dev', strategy='manual'):
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f"Iniciando pipeline en entorno: {env} | Estrategia: {strategy}")

    # Selección de configuración
    config = prod if env == 'prod' else dev

    pipeline_args = [
        "--runner=DirectRunner",
        "--streaming"
    ]
    
    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        if strategy == 'native':
            build_pipeline_cases_native(p, config)
        else:
            build_pipeline_cases(p, config)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Entorno de ejecución')
    parser.add_argument('--strategy', choices=['manual', 'native'], default='manual', help='Estrategia de batching')
    args = parser.parse_args()
    
    run(args.env, args.strategy)
