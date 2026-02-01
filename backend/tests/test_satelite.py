"""
Testes para o serviço de imagens de satélite.

Uso:
    pytest backend/tests/test_satelite.py -v
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from backend.src.services.inpe_satellite_service import (
    INPESatelliteService,
    BoundingBox,
    SatelliteMetadata
)


class TestBoundingBox:
    """Testes para a classe BoundingBox."""
    
    def test_calculo_center(self):
        """Testa cálculo do centro da bbox."""
        bbox = BoundingBox(
            min_lat=-20.0,
            max_lat=-19.0,
            min_lon=-44.0,
            max_lon=-43.0
        )
        
        assert bbox.center_lat == pytest.approx(-19.5)
        assert bbox.center_lon == pytest.approx(-43.5)
    
    def test_calculo_dimensoes(self):
        """Testa cálculo de dimensões."""
        bbox = BoundingBox(
            min_lat=-20.0,
            max_lat=-19.0,  # 1 grau = ~110 km
            min_lon=-44.0,
            max_lon=-43.0   # 1 grau = ~111 km
        )
        
        assert bbox.height_km == pytest.approx(110.567, rel=0.01)
        assert bbox.width_km == pytest.approx(111.0, rel=0.01)
    
    def test_to_wgs84_string(self):
        """Testa conversão para string WGS84."""
        bbox = BoundingBox(
            min_lat=-20.0,
            max_lat=-19.0,
            min_lon=-44.0,
            max_lon=-43.0
        )
        
        wgs84_str = bbox.to_wgs84_string()
        assert wgs84_str == "-44.0,-20.0,-43.0,-19.0"
    
    def test_to_geojson(self):
        """Testa conversão para GeoJSON."""
        bbox = BoundingBox(
            min_lat=-20.0,
            max_lat=-19.0,
            min_lon=-44.0,
            max_lon=-43.0
        )
        
        geojson = bbox.to_geojson()
        assert geojson["type"] == "Polygon"
        assert len(geojson["coordinates"][0]) == 5  # 4 pontos + fechamento
    
    def test_bbox_invalida_lat(self):
        """Testa bbox com latitude inválida."""
        # Não deve lançar erro na criação, validação é responsabilidade do usuário
        bbox = BoundingBox(
            min_lat=-91.0,  # Inválido
            max_lat=91.0,   # Inválido
            min_lon=-44.0,
            max_lon=-43.0
        )
        # Testes de validação poderiam ser adicionados


class TestINPESatelliteService:
    """Testes para INPESatelliteService."""
    
    @pytest.fixture
    def service(self):
        """Fixture para o serviço."""
        return INPESatelliteService()
    
    def test_calcular_bbox_subestacao(self, service):
        """Testa cálculo de bbox para subestação."""
        bbox = service.calcular_bbox_subestacao(
            latitude=-19.925,
            longitude=-43.938,
            raio_km=5.0
        )
        
        assert isinstance(bbox, BoundingBox)
        assert bbox.center_lat == pytest.approx(-19.925)
        assert bbox.center_lon == pytest.approx(-43.938)
        # Raio de 5 km deve resultar em bbox de aproximadamente 10x10 km
        assert 9.0 <= bbox.width_km <= 11.0
        assert 9.0 <= bbox.height_km <= 11.0
    
    def test_calcular_bbox_diferentes_raios(self, service):
        """Testa bbox com diferentes raios."""
        bbox_3km = service.calcular_bbox_subestacao(
            latitude=-19.925,
            longitude=-43.938,
            raio_km=3.0
        )
        
        bbox_10km = service.calcular_bbox_subestacao(
            latitude=-19.925,
            longitude=-43.938,
            raio_km=10.0
        )
        
        # Raio maior deve gerar bbox maior
        assert bbox_10km.width_km > bbox_3km.width_km
        assert bbox_10km.height_km > bbox_3km.height_km
    
    def test_construir_url_wms_terrabrasilis(self, service):
        """Testa construção de URL WMS."""
        bbox = BoundingBox(
            min_lat=-20.0,
            max_lat=-19.0,
            min_lon=-44.0,
            max_lon=-43.0
        )
        
        url = service.construir_url_wms_terrabrasilis(
            bbox,
            camada="prodes",
            largura_px=512,
            altura_px=512
        )
        
        assert "WMS" in url
        assert "prodes" in url
        assert "EPSG:4326" in url
        assert "512" in url
    
    def test_gerar_url_sentinel2_stac(self, service):
        """Testa geração de URL STAC Sentinel-2."""
        bbox = BoundingBox(
            min_lat=-20.0,
            max_lat=-19.0,
            min_lon=-44.0,
            max_lon=-43.0
        )
        
        data_inicio = datetime(2026, 1, 1)
        data_fim = datetime(2026, 1, 31)
        
        resultado = service.gerar_url_sentinel2_stac(
            bbox,
            data_inicio,
            data_fim,
            cobertura_nuvem_max_pct=50.0
        )
        
        assert resultado["method"] == "POST"
        assert "planetarycomputer" in resultado["url"]
        assert resultado["payload"]["collections"] == ["sentinel-2-l2a"]
        assert resultado["payload"]["query"]["eo:cloud_cover"]["lte"] == 50.0
    
    def test_gerar_url_landsat_stac(self, service):
        """Testa geração de URL STAC Landsat."""
        bbox = BoundingBox(
            min_lat=-20.0,
            max_lat=-19.0,
            min_lon=-44.0,
            max_lon=-43.0
        )
        
        data_inicio = datetime(2026, 1, 1)
        data_fim = datetime(2026, 1, 31)
        
        resultado = service.gerar_url_landsat_stac(
            bbox,
            data_inicio,
            data_fim
        )
        
        assert resultado["method"] == "POST"
        assert "usgs" in resultado["url"]
        assert resultado["payload"]["query"]["landsat:cloud_cover"]["lte"] == 50.0


class TestSatelliteMetadata:
    """Testes para SatelliteMetadata."""
    
    def test_criar_metadata(self):
        """Testa criação de metadados."""
        bbox = BoundingBox(-20.0, -19.0, -44.0, -43.0)
        metadata = SatelliteMetadata(
            id="S2_20260101",
            data_aquisicao=datetime(2026, 1, 1),
            sensor="Sentinel-2",
            resolucao_m=10,
            cobertura_nuvem_pct=12.5,
            url="https://example.com/image.zip",
            bounding_box=bbox,
            propriedades={"tile": "23KPA"}
        )
        
        assert metadata.id == "S2_20260101"
        assert metadata.sensor == "Sentinel-2"
        assert metadata.resolucao_m == 10
        assert metadata.cobertura_nuvem_pct == 12.5


class TestIntegracaoAPI:
    """Testes de integração com API (se usar mock)."""
    
    @pytest.fixture
    def client(self):
        """Fixture para cliente HTTP."""
        from fastapi.testclient import TestClient
        from backend.src.main import app
        return TestClient(app)
    
    def test_endpoint_coordenadas(self, client):
        """Testa endpoint de coordenadas."""
        # Este teste só funcionará se houver dados no banco de teste
        # Pode ser pulado ou usar mock
        pass
    
    def test_endpoint_bbox(self, client):
        """Testa endpoint de bbox."""
        pass


# Testes parametrizados
class TestCalculosBoundingBox:
    """Testes parametrizados para cálculos de bbox."""
    
    @pytest.mark.parametrize("raio_km,expected_min,expected_max", [
        (1.0, 0.8, 1.2),
        (5.0, 4.5, 5.5),
        (10.0, 9.0, 11.0),
        (20.0, 18.0, 22.0),
    ])
    def test_bbox_dimensoes_para_diferentes_raios(
        self,
        raio_km,
        expected_min,
        expected_max
    ):
        """Testa que dimensões são próximas ao raio * 2."""
        service = INPESatelliteService()
        bbox = service.calcular_bbox_subestacao(
            latitude=-19.925,
            longitude=-43.938,
            raio_km=raio_km
        )
        
        esperado = raio_km * 2
        assert expected_min <= bbox.width_km <= expected_max
        assert expected_min <= bbox.height_km <= expected_max


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
