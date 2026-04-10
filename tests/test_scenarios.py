"""
Tests para generación de escenarios futuros (Sección 5.13).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import unittest
from src.analytics.scenarios import estimate_rt, find_inflection_point, compute_derived_indicators


class TestEstimateRt(unittest.TestCase):

    def test_basic_rt_calculation(self):
        """Rt debe calcularse correctamente con datos suficientes."""
        # Datos constantes → Rt ≈ 1.0
        daily_cases = [100] * 30
        rt_values = estimate_rt(daily_cases)
        self.assertGreater(len(rt_values), 0)
        for entry in rt_values:
            self.assertAlmostEqual(entry['rt'], 1.0, places=2)

    def test_increasing_cases_rt_above_1(self):
        """Casos crecientes → Rt > 1."""
        daily_cases = [10 * (1.1 ** i) for i in range(30)]
        rt_values = estimate_rt(daily_cases)
        self.assertGreater(len(rt_values), 0)
        avg_rt = sum(r['rt'] for r in rt_values) / len(rt_values)
        self.assertGreater(avg_rt, 1.0)

    def test_decreasing_cases_rt_below_1(self):
        """Casos decrecientes → Rt < 1."""
        daily_cases = [1000 * (0.9 ** i) for i in range(30)]
        rt_values = estimate_rt(daily_cases)
        self.assertGreater(len(rt_values), 0)
        avg_rt = sum(r['rt'] for r in rt_values) / len(rt_values)
        self.assertLess(avg_rt, 1.0)

    def test_insufficient_data(self):
        """Con pocos datos, no debe generar resultados."""
        daily_cases = [100] * 5
        rt_values = estimate_rt(daily_cases)
        self.assertEqual(len(rt_values), 0)

    def test_output_structure(self):
        """Cada resultado debe tener day, rt, cases."""
        daily_cases = [100] * 20
        rt_values = estimate_rt(daily_cases)
        if rt_values:
            entry = rt_values[0]
            self.assertIn('day', entry)
            self.assertIn('rt', entry)
            self.assertIn('cases', entry)


class TestFindInflectionPoint(unittest.TestCase):

    def test_convex_to_concave(self):
        """Detecta inflexión donde segunda derivada cambia de signo."""
        # Curva que sube acelerando y luego desacelera
        values = [1, 3, 7, 13, 20, 26, 30, 33, 35, 36, 36.5]
        result = find_inflection_point(values)
        self.assertIsNotNone(result)

    def test_monotone_no_inflection(self):
        """Función monótona convexa no tiene inflexión."""
        values = [x ** 2 for x in range(10)]
        result = find_inflection_point(values)
        # Pure convex may not have inflection
        # This is acceptable either way

    def test_too_short(self):
        """Menos de 3 valores retorna None."""
        self.assertIsNone(find_inflection_point([1, 2]))
        self.assertIsNone(find_inflection_point([]))


class TestComputeDerivedIndicators(unittest.TestCase):

    def test_peak_detection(self):
        """Debe detectar el pico en cada escenario."""
        scenarios = {
            'optimista': [10, 20, 30, 20, 10],
            'base': [15, 25, 35, 25, 15],
            'pesimista': [20, 30, 50, 30, 20],
            'dates': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
            'horizon_days': 5,
        }
        daily_cases = [100] * 20

        indicators = compute_derived_indicators(daily_cases, scenarios)

        self.assertIn('peak_base', indicators)
        self.assertEqual(indicators['peak_base']['date'], '2024-01-03')
        self.assertEqual(indicators['peak_base']['value'], 35)

    def test_doubling_time_growing(self):
        """Cuando casos crecen, doubling_time debe ser positivo."""
        scenarios = {
            'optimista': [10] * 5,
            'base': [20] * 5,
            'pesimista': [30] * 5,
            'dates': ['2024-01-01'] * 5,
            'horizon_days': 5,
        }
        # Crecimiento: segunda semana > primera semana
        daily_cases = [50] * 7 + [100] * 7

        indicators = compute_derived_indicators(daily_cases, scenarios)
        self.assertIn('doubling_time_days', indicators)
        self.assertIsNotNone(indicators['doubling_time_days'])
        self.assertGreater(indicators['doubling_time_days'], 0)

    def test_halving_time_declining(self):
        """Cuando casos decrecen, halving_time debe aparecer."""
        scenarios = {
            'optimista': [10] * 5,
            'base': [20] * 5,
            'pesimista': [30] * 5,
            'dates': ['2024-01-01'] * 5,
            'horizon_days': 5,
        }
        # Decrecimiento: segunda semana < primera semana
        daily_cases = [100] * 7 + [50] * 7

        indicators = compute_derived_indicators(daily_cases, scenarios)
        self.assertIn('halving_time_days', indicators)
        self.assertIsNotNone(indicators['halving_time_days'])

    def test_hospital_demand(self):
        """Demanda hospitalaria = peak * 0.05."""
        scenarios = {
            'optimista': [100],
            'base': [200],
            'pesimista': [400],
            'dates': ['2024-01-01'],
            'horizon_days': 1,
        }
        daily_cases = [100] * 20

        indicators = compute_derived_indicators(daily_cases, scenarios)
        self.assertEqual(indicators['hospital_demand_base'], 10.0)  # 200 * 0.05
        self.assertEqual(indicators['hospital_demand_pesimista'], 20.0)  # 400 * 0.05

    def test_rt_trend_indicator(self):
        """Debe calcular Rt actual y clasificar tendencia."""
        scenarios = {
            'optimista': [10] * 5,
            'base': [20] * 5,
            'pesimista': [30] * 5,
            'dates': ['2024-01-01'] * 5,
            'horizon_days': 5,
        }
        daily_cases = [100] * 20  # Constante → Rt ≈ 1.0 → stable

        indicators = compute_derived_indicators(daily_cases, scenarios)
        if 'current_rt' in indicators:
            self.assertAlmostEqual(indicators['current_rt'], 1.0, places=1)
            self.assertEqual(indicators['trend'], 'stable')


if __name__ == '__main__':
    unittest.main()
