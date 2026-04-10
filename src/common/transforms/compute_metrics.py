"""
Cálculo de métricas descriptivas por ventana temporal.

Implementa ComputeWindowMetrics como CombineFn de Apache Beam,
calculando métricas epidemiológicas por ventana de 60 segundos.

Sección 5.10 - Modelos analíticos y predictivos
"""
import apache_beam as beam
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class AggregateCasesMetrics(beam.CombineFn):
    """Combina métricas del schema Cases dentro de una ventana temporal.

    Métricas calculadas:
    - Tasa de positividad (positivos / total)
    - Media de edad
    - Distribución por sexo (ratio masculino/femenino)
    - Concentración geográfica (top-k departamentos)
    - Tasa por institución
    """

    def create_accumulator(self):
        return {
            'count': 0,
            'positives': 0,
            'ages': [],
            'male': 0,
            'female': 0,
            'departments': {},
            'institutions': {},
        }

    def add_input(self, acc, element):
        acc['count'] += 1
        data = element.get('data', {})

        # Tasa de positividad
        if data.get('resultado') == 'POSITIVO':
            acc['positives'] += 1

        # Edad
        edad = data.get('edad')
        if edad is not None:
            try:
                acc['ages'].append(int(edad))
            except (ValueError, TypeError):
                pass

        # Distribución por sexo
        sexo = str(data.get('sexo', '')).upper()
        if sexo == 'MASCULINO':
            acc['male'] += 1
        elif sexo == 'FEMENINO':
            acc['female'] += 1

        # Concentración geográfica
        dept = data.get('departamento_paciente') or data.get('departamento_muestra') or 'DESCONOCIDO'
        acc['departments'][dept] = acc['departments'].get(dept, 0) + 1

        # Distribución por institución
        inst = data.get('institucion') or 'DESCONOCIDO'
        acc['institutions'][inst] = acc['institutions'].get(inst, 0) + 1

        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged['count'] += acc['count']
            merged['positives'] += acc['positives']
            merged['ages'].extend(acc['ages'])
            merged['male'] += acc['male']
            merged['female'] += acc['female']
            for dept, count in acc['departments'].items():
                merged['departments'][dept] = merged['departments'].get(dept, 0) + count
            for inst, count in acc['institutions'].items():
                merged['institutions'][inst] = merged['institutions'].get(inst, 0) + count
        return merged

    def extract_output(self, acc):
        count = acc['count']
        if count == 0:
            return None

        ages = acc['ages']
        return {
            'schema': 'cases',
            'total': count,
            'positivity_rate': round(acc['positives'] / count, 4) if count > 0 else 0,
            'positive_count': acc['positives'],
            'avg_age': round(sum(ages) / len(ages), 2) if ages else None,
            'min_age': min(ages) if ages else None,
            'max_age': max(ages) if ages else None,
            'male_count': acc['male'],
            'female_count': acc['female'],
            'male_ratio': round(acc['male'] / count, 4) if count > 0 else 0,
            'female_ratio': round(acc['female'] / count, 4) if count > 0 else 0,
            'top_departments': sorted(
                acc['departments'].items(),
                key=lambda x: x[1], reverse=True
            )[:5],
            'department_count': len(acc['departments']),
            'institution_distribution': dict(acc['institutions']),
        }


class AggregateDemisesMetrics(beam.CombineFn):
    """Combina métricas del schema Demises dentro de una ventana temporal.

    Métricas calculadas:
    - Total de fallecimientos en la ventana
    - Edad media de fallecidos
    - Distribución por clasificación de defunción
    - Mortalidad por departamento
    """

    def create_accumulator(self):
        return {
            'count': 0,
            'ages': [],
            'male': 0,
            'female': 0,
            'classifications': {},
            'departments': {},
        }

    def add_input(self, acc, element):
        acc['count'] += 1
        data = element.get('data', {})

        # Edad de fallecidos
        edad = data.get('edad_declarada')
        if edad is not None:
            try:
                acc['ages'].append(int(edad))
            except (ValueError, TypeError):
                pass

        # Distribución por sexo
        sexo = str(data.get('sexo', '')).upper()
        if sexo == 'MASCULINO':
            acc['male'] += 1
        elif sexo == 'FEMENINO':
            acc['female'] += 1

        # Clasificación de defunción
        clasif = data.get('clasificacion_def') or 'DESCONOCIDO'
        acc['classifications'][clasif] = acc['classifications'].get(clasif, 0) + 1

        # Mortalidad por departamento
        dept = data.get('departamento') or 'DESCONOCIDO'
        acc['departments'][dept] = acc['departments'].get(dept, 0) + 1

        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged['count'] += acc['count']
            merged['ages'].extend(acc['ages'])
            merged['male'] += acc['male']
            merged['female'] += acc['female']
            for clasif, count in acc['classifications'].items():
                merged['classifications'][clasif] = merged['classifications'].get(clasif, 0) + count
            for dept, count in acc['departments'].items():
                merged['departments'][dept] = merged['departments'].get(dept, 0) + count
        return merged

    def extract_output(self, acc):
        count = acc['count']
        if count == 0:
            return None

        ages = acc['ages']
        return {
            'schema': 'demises',
            'total': count,
            'avg_age': round(sum(ages) / len(ages), 2) if ages else None,
            'min_age': min(ages) if ages else None,
            'max_age': max(ages) if ages else None,
            'male_count': acc['male'],
            'female_count': acc['female'],
            'male_ratio': round(acc['male'] / count, 4) if count > 0 else 0,
            'classification_distribution': dict(acc['classifications']),
            'top_departments': sorted(
                acc['departments'].items(),
                key=lambda x: x[1], reverse=True
            )[:5],
            'department_count': len(acc['departments']),
        }


class AddWindowInfo(beam.DoFn):
    """Agrega información de la ventana temporal a las métricas calculadas."""

    def process(self, metrics, window=beam.DoFn.WindowParam):
        if metrics is None:
            return

        metrics['window_start'] = window.start.to_utc_datetime().isoformat()
        metrics['window_end'] = window.end.to_utc_datetime().isoformat()
        metrics['computed_at'] = datetime.now(tz=timezone.utc).isoformat()

        logger.info(
            f"Window [{metrics['window_start']}, {metrics['window_end']}] "
            f"schema={metrics.get('schema')} total={metrics.get('total')}"
        )
        yield metrics


def create_compute_metrics_branch(windowed_data, schema_name, label_prefix=""):
    """Crea una rama paralela que calcula métricas descriptivas por ventana.

    Args:
        windowed_data: PCollection con datos ya windowed
        schema_name: 'cases' o 'demises'
        label_prefix: Prefijo para labels de Beam (evitar duplicados)

    Returns:
        PCollection con métricas por ventana
    """
    if schema_name == 'cases':
        combine_fn = AggregateCasesMetrics()
    elif schema_name == 'demises':
        combine_fn = AggregateDemisesMetrics()
    else:
        raise ValueError(f"Schema no soportado para métricas: {schema_name}")

    prefix = f"{label_prefix}" if label_prefix else ""

    metrics = (
        windowed_data
        | f"{prefix}Compute Window Metrics" >> beam.CombineGlobally(combine_fn).without_defaults()
        | f"{prefix}Add Window Info" >> beam.ParDo(AddWindowInfo())
    )

    return metrics
