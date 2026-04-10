"""
Test de verificación de métricas descriptivas por ventana.

Ejecuta el CombineFn de métricas con datos sintéticos y reales
para comprobar que las métricas se calculan correctamente.

Uso:
    python tests/test_compute_metrics.py
"""
import sys
import csv
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_not_empty

from src.common.transforms.compute_metrics import (
    AggregateCasesMetrics,
    AggregateDemisesMetrics,
    AddWindowInfo,
    create_compute_metrics_branch,
)


# ── Datos sintéticos ──────────────────────────────────────────────
SAMPLE_CASES = [
    {'schema': 'cases', 'data': {'resultado': 'POSITIVO', 'edad': 30, 'sexo': 'MASCULINO',
     'departamento_paciente': 'LIMA', 'institucion': 'MINSA'}},
    {'schema': 'cases', 'data': {'resultado': 'POSITIVO', 'edad': 45, 'sexo': 'FEMENINO',
     'departamento_paciente': 'LIMA', 'institucion': 'ESSALUD'}},
    {'schema': 'cases', 'data': {'resultado': 'NEGATIVO', 'edad': 60, 'sexo': 'MASCULINO',
     'departamento_paciente': 'AREQUIPA', 'institucion': 'PRIVADO'}},
    {'schema': 'cases', 'data': {'resultado': 'POSITIVO', 'edad': 25, 'sexo': 'FEMENINO',
     'departamento_paciente': 'CUSCO', 'institucion': 'MINSA'}},
    {'schema': 'cases', 'data': {'resultado': 'NEGATIVO', 'edad': 50, 'sexo': 'MASCULINO',
     'departamento_paciente': 'LIMA', 'institucion': 'PRIVADO'}},
]

SAMPLE_DEMISES = [
    {'schema': 'demises', 'data': {'edad_declarada': 70, 'sexo': 'MASCULINO',
     'clasificacion_def': 'Criterio virologico', 'departamento': 'LIMA'}},
    {'schema': 'demises', 'data': {'edad_declarada': 65, 'sexo': 'FEMENINO',
     'clasificacion_def': 'Criterio SINADEF', 'departamento': 'AREQUIPA'}},
    {'schema': 'demises', 'data': {'edad_declarada': 80, 'sexo': 'MASCULINO',
     'clasificacion_def': 'Criterio virologico', 'departamento': 'LIMA'}},
]


def test_cases_metrics_synthetic():
    """Verifica métricas de Cases con datos sintéticos."""
    print("\n" + "="*60)
    print("TEST 1: Métricas Cases - Datos sintéticos")
    print("="*60)

    combine_fn = AggregateCasesMetrics()
    acc = combine_fn.create_accumulator()

    for element in SAMPLE_CASES:
        acc = combine_fn.add_input(acc, element)

    result = combine_fn.extract_output(acc)

    print(f"\n  Total registros:     {result['total']}")
    print(f"  Positivos:           {result['positive_count']}")
    print(f"  Tasa positividad:    {result['positivity_rate']*100:.1f}%")
    print(f"  Edad promedio:       {result['avg_age']}")
    print(f"  Edad min/max:        {result['min_age']} / {result['max_age']}")
    print(f"  Masculino:           {result['male_count']} ({result['male_ratio']*100:.1f}%)")
    print(f"  Femenino:            {result['female_count']} ({result['female_ratio']*100:.1f}%)")
    print(f"  Top departamentos:   {result['top_departments']}")
    print(f"  Instituciones:       {result['institution_distribution']}")

    # Validaciones
    assert result['total'] == 5, f"Expected 5, got {result['total']}"
    assert result['positive_count'] == 3, f"Expected 3 positives, got {result['positive_count']}"
    assert result['positivity_rate'] == 0.6, f"Expected 0.6, got {result['positivity_rate']}"
    assert result['avg_age'] == 42.0, f"Expected 42.0, got {result['avg_age']}"
    assert result['male_count'] == 3
    assert result['female_count'] == 2
    assert result['top_departments'][0] == ('LIMA', 3)

    print("\n  ✓ Todas las validaciones pasaron")


def test_demises_metrics_synthetic():
    """Verifica métricas de Demises con datos sintéticos."""
    print("\n" + "="*60)
    print("TEST 2: Métricas Demises - Datos sintéticos")
    print("="*60)

    combine_fn = AggregateDemisesMetrics()
    acc = combine_fn.create_accumulator()

    for element in SAMPLE_DEMISES:
        acc = combine_fn.add_input(acc, element)

    result = combine_fn.extract_output(acc)

    print(f"\n  Total fallecidos:    {result['total']}")
    print(f"  Edad promedio:       {result['avg_age']}")
    print(f"  Edad min/max:        {result['min_age']} / {result['max_age']}")
    print(f"  Masculino:           {result['male_count']} ({result['male_ratio']*100:.1f}%)")
    print(f"  Clasificaciones:     {result['classification_distribution']}")
    print(f"  Top departamentos:   {result['top_departments']}")

    assert result['total'] == 3
    assert result['avg_age'] == 71.67
    assert result['male_count'] == 2
    assert result['female_count'] == 1
    assert result['classification_distribution']['Criterio virologico'] == 2

    print("\n  ✓ Todas las validaciones pasaron")


def test_merge_accumulators():
    """Verifica que merge_accumulators funciona correctamente (simula paralelismo)."""
    print("\n" + "="*60)
    print("TEST 3: Merge de acumuladores (paralelismo)")
    print("="*60)

    combine_fn = AggregateCasesMetrics()

    acc1 = combine_fn.create_accumulator()
    for el in SAMPLE_CASES[:3]:
        acc1 = combine_fn.add_input(acc1, el)

    acc2 = combine_fn.create_accumulator()
    for el in SAMPLE_CASES[3:]:
        acc2 = combine_fn.add_input(acc2, el)

    merged = combine_fn.merge_accumulators([acc1, acc2])
    result = combine_fn.extract_output(merged)

    print(f"\n  Acc1 count: {acc1['count']}, Acc2 count: {acc2['count']}")
    print(f"  Merged total: {result['total']}")
    print(f"  Merged positivity: {result['positivity_rate']*100:.1f}%")

    assert result['total'] == 5
    assert result['positive_count'] == 3

    print("\n  ✓ Merge funciona correctamente")


def test_cases_metrics_with_real_csv():
    """Verifica métricas con datos reales del CSV de cases."""
    print("\n" + "="*60)
    print("TEST 4: Métricas Cases - Datos reales (CSV)")
    print("="*60)

    csv_path = ROOT_DIR / "datasets" / "cases" / "file_0_cases.csv"
    if not csv_path.exists():
        print(f"  SKIP: {csv_path} no existe")
        return

    combine_fn = AggregateCasesMetrics()
    acc = combine_fn.create_accumulator()
    row_count = 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            element = {'schema': 'cases', 'data': row}
            acc = combine_fn.add_input(acc, element)
            row_count += 1

    result = combine_fn.extract_output(acc)

    print(f"\n  Filas leídas:        {row_count}")
    print(f"  Total registros:     {result['total']}")
    print(f"  Positivos:           {result['positive_count']}")
    print(f"  Tasa positividad:    {result['positivity_rate']*100:.1f}%")
    print(f"  Edad promedio:       {result['avg_age']}")
    print(f"  Edad min/max:        {result['min_age']} / {result['max_age']}")
    print(f"  Masculino ratio:     {result['male_ratio']*100:.1f}%")
    print(f"  Femenino ratio:      {result['female_ratio']*100:.1f}%")
    print(f"  Departamentos únicos:{result['department_count']}")
    print(f"  Top 5 departamentos: {result['top_departments']}")
    print(f"  Instituciones:       {result['institution_distribution']}")

    assert result['total'] == row_count
    assert 0 <= result['positivity_rate'] <= 1

    print("\n  ✓ Métricas reales calculadas correctamente")


def test_demises_metrics_with_real_csv():
    """Verifica métricas con datos reales del CSV de demises."""
    print("\n" + "="*60)
    print("TEST 5: Métricas Demises - Datos reales (CSV)")
    print("="*60)

    csv_path = ROOT_DIR / "datasets" / "demises" / "file_0_demises.csv"
    if not csv_path.exists():
        print(f"  SKIP: {csv_path} no existe")
        return

    combine_fn = AggregateDemisesMetrics()
    acc = combine_fn.create_accumulator()
    row_count = 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            element = {'schema': 'demises', 'data': row}
            acc = combine_fn.add_input(acc, element)
            row_count += 1

    result = combine_fn.extract_output(acc)

    print(f"\n  Filas leídas:        {row_count}")
    print(f"  Total fallecidos:    {result['total']}")
    print(f"  Edad promedio:       {result['avg_age']}")
    print(f"  Edad min/max:        {result['min_age']} / {result['max_age']}")
    print(f"  Masculino ratio:     {result['male_ratio']*100:.1f}%")
    print(f"  Departamentos únicos:{result['department_count']}")
    print(f"  Top 5 departamentos: {result['top_departments']}")
    print(f"  Clasificaciones:     {result['classification_distribution']}")

    assert result['total'] == row_count

    print("\n  ✓ Métricas reales calculadas correctamente")


def test_beam_pipeline_integration():
    """Verifica que el CombineFn funciona dentro de un pipeline Beam real."""
    print("\n" + "="*60)
    print("TEST 6: Integración con Apache Beam Pipeline")
    print("="*60)

    def check_metrics(elements):
        assert len(elements) == 1, f"Expected 1 metric output, got {len(elements)}"
        result = elements[0]
        assert result['total'] == 5, f"Expected total=5, got {result['total']}"
        assert result['positivity_rate'] == 0.6, f"Expected 0.6, got {result['positivity_rate']}"
        print(f"\n  Pipeline output: total={result['total']}, positivity={result['positivity_rate']*100:.1f}%")

    with TestPipeline() as p:
        metrics = (
            p
            | "Create" >> beam.Create(SAMPLE_CASES)
            | "Combine" >> beam.CombineGlobally(AggregateCasesMetrics()).without_defaults()
        )
        assert_that(metrics, is_not_empty())

    print("\n  ✓ Integración Beam funciona correctamente")


def test_empty_accumulator():
    """Verifica que un acumulador vacío retorna None."""
    print("\n" + "="*60)
    print("TEST 7: Acumulador vacío")
    print("="*60)

    combine_fn = AggregateCasesMetrics()
    acc = combine_fn.create_accumulator()
    result = combine_fn.extract_output(acc)

    assert result is None, f"Expected None, got {result}"
    print("\n  ✓ Acumulador vacío retorna None correctamente")


if __name__ == '__main__':
    print("\n" + "#"*60)
    print("  VERIFICACIÓN DE MÉTRICAS DESCRIPTIVAS")
    print("  Sección 5.10 - Modelos analíticos y predictivos")
    print("#"*60)

    test_cases_metrics_synthetic()
    test_demises_metrics_synthetic()
    test_merge_accumulators()
    test_cases_metrics_with_real_csv()
    test_demises_metrics_with_real_csv()
    test_beam_pipeline_integration()
    test_empty_accumulator()

    print("\n" + "="*60)
    print("  ✓ TODOS LOS TESTS PASARON")
    print("="*60 + "\n")
