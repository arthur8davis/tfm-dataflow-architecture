"""
Enriquecimiento geográfico - Agrega latitud y longitud basado en UBIGEO
"""
import apache_beam as beam
import logging
import sys
from pathlib import Path

# Agregar path para imports
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.common.data.ubigeo_coords import get_coords_from_ubigeo

logger = logging.getLogger(__name__)


class EnrichGeoFromUbigeo(beam.DoFn):
    """Enriquece registros con latitud y longitud basado en ubicación geográfica"""

    def __init__(self,
                 ubigeo_field: str = "ubigeo_paciente",
                 dept_field: str = "departamento_paciente",
                 prov_field: str = "provincia_paciente",
                 dist_field: str = "distrito_paciente"):
        """
        Args:
            ubigeo_field: Campo con UBIGEO del paciente (domicilio)
            dept_field: Campo con el departamento
            prov_field: Campo con la provincia
            dist_field: Campo con el distrito (más preciso)
        """
        self.ubigeo_field = ubigeo_field
        self.dept_field = dept_field
        self.prov_field = prov_field
        self.dist_field = dist_field

    def process(self, element):
        """
        Agrega latitud y longitud al registro basado en ubicación geográfica

        Args:
            element: Registro con estructura {schema, data}

        Yields:
            dict: Registro enriquecido con lat/lon
        """
        try:
            if not isinstance(element, dict) or 'data' not in element:
                yield element
                return

            data = element['data']

            # Coordenadas basadas en departamento/provincia/distrito de muestra
            departamento = data.get(self.dept_field)
            provincia = data.get(self.prov_field)
            distrito = data.get(self.dist_field)
            coords = get_coords_from_ubigeo(departamento, provincia, distrito)

            if coords:
                data['latitud'] = coords['lat']
                data['longitud'] = coords['lon']
            else:
                data['latitud'] = None
                data['longitud'] = None
                logger.debug(f"No se encontraron coordenadas para dept={departamento}, prov={provincia}, dist={distrito}")

            element['data'] = data
            yield element

        except Exception as e:
            logger.error(f"Error enriching geo data: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'record': str(element),
                'error_type': 'geo_enrichment_error'
            })
