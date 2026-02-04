"""
Serviço de Transformadores
Lógica de negócio para operações com transformadores
"""

import json
import logging
from typing import Dict, List, Optional

import pandas as pd

from ..repositories.transformador_repository import TransformadorRepository


class TransformadorService:
    """
    Serviço de transformadores.
    Responsável por: Lógica de negócio, validações, formatações, orquestração.
    """

    def __init__(self, engine):
        """
        Inicializa o serviço com engine SQLAlchemy.
        
        Args:
            engine: SQLAlchemy Engine para banco de dados
        """
        self.engine = engine
        self.repository = TransformadorRepository(engine)
        self.logger = logging.getLogger(__name__)

    # ========================================================================
    # DETALHES E CONSULTAS UNITÁRIAS
    # ========================================================================

    def obter_detalhes(self, transformador_id: int) -> Optional[Dict]:
        """
        Obtém detalhes completos de um transformador.
        
        Args:
            transformador_id: ID do transformador
            
        Returns:
            Dict com dados do transformador ou None se não encontrado
        """
        transformador = self.repository.obter_por_id(transformador_id)
        
        if not transformador:
            self.logger.warning(f"Transformador {transformador_id} não encontrado")
            return None
        
        # Buscar área de cobertura se existir
        area = self.repository.obter_area_cobertura(transformador_id)
        
        return {
            **transformador,
            'area_cobertura': area
        }

    def obter_area_cobertura_geojson(self, transformador_id: int, formato: str = "geojson") -> Optional[Dict]:
        """
        Retorna área de cobertura em diferentes formatos.
        
        Args:
            transformador_id: ID do transformador
            formato: "geojson", "wkt" ou "json"
            
        Returns:
            Dict formatado ou None
        """
        transformador = self.repository.obter_por_id(transformador_id)
        if not transformador:
            return None
        
        area = self.repository.obter_area_cobertura(transformador_id)
        
        # Se não houver área calculada, retornar área padrão
        if not area:
            area = {
                'id': None,
                'transformador_codigo': transformador.get('codigo'),
                'tipo_tensao': transformador.get('tipo_tensao', 'DESCONHECIDO'),
                'metodo_calculo': 'buffer_2km_padrao',
                'area_km2': 12.566,  # π * 2²
                'area_m2': 12566370,
                'num_consumidores': 0,
                'num_vertices': 0,
                'geojson_area': None,
                'wkt_area': None
            }
        
        if formato == "geojson":
            return {
                "type": "Feature",
                "geometry": json.loads(area['geojson_area']) if area['geojson_area'] else None,
                "properties": {
                    "id": area['id'],
                    "transformador_codigo": area['transformador_codigo'],
                    "tipo_tensao": area['tipo_tensao'],
                    "metodo_calculo": area['metodo_calculo'],
                    "area_km2": area['area_km2'],
                    "area_m2": area['area_m2'],
                    "num_consumidores": area['num_consumidores'],
                    "num_vertices": area['num_vertices']
                }
            }
        
        elif formato == "wkt":
            return {
                "wkt": area['wkt_area'],
                "area_km2": area['area_km2'],
                "num_consumidores": area['num_consumidores']
            }
        
        else:  # json
            return {
                "id": area['id'],
                "transformador_codigo": area['transformador_codigo'],
                "tipo_tensao": area['tipo_tensao'],
                "metodo_calculo": area['metodo_calculo'],
                "area_km2": area['area_km2'],
                "num_consumidores": area['num_consumidores']
            }

    def obter_bbox_para_satelite(self, transformador_id: int, margem_km: float = 2.0) -> Optional[Dict]:
        """
        Calcula bounding box para download de imagens de satélite.
        
        Args:
            transformador_id: ID do transformador
            margem_km: Margem em km ao redor do transformador
            
        Returns:
            Dict com bbox ou None
        """
        transformador = self.repository.obter_por_id(transformador_id)
        if not transformador or not transformador['latitude'] or not transformador['longitude']:
            return None
        
        # Converter km para graus (aproximado: 1 grau ≈ 111 km)
        delta = margem_km / 111.0
        
        return {
            "transformador_id": transformador_id,
            "transformador_codigo": transformador['codigo'],
            "latitude": transformador['latitude'],
            "longitude": transformador['longitude'],
            "bbox": {
                "min_lat": transformador['latitude'] - delta,
                "min_lon": transformador['longitude'] - delta,
                "max_lat": transformador['latitude'] + delta,
                "max_lon": transformador['longitude'] + delta
            },
            "margem_km": margem_km
        }

    # ========================================================================
    # LISTAGEM COM PAGINAÇÃO
    # ========================================================================

    def listar_todos(self, skip: int = 0, limit: int = 100) -> Dict:
        """
        Lista todos os transformadores com paginação.
        
        Args:
            skip: Offset para paginação
            limit: Limite de resultados
            
        Returns:
            Dict com dados e metadados
        """
        df = self.repository.listar_todos(skip=skip, limit=limit)
        total = self.repository.contar_total()
        
        return {
            "data": df.to_dict('records') if not df.empty else [],
            "total": total,
            "skip": skip,
            "limit": limit,
            "tem_proxima": (skip + limit) < total
        }

    def listar_por_subestacao(self, subestacao_codigo: str, skip: int = 0, limit: int = 100) -> Dict:
        """
        Lista transformadores de uma subestação.
        
        Args:
            subestacao_codigo: Código da subestação
            skip: Offset para paginação
            limit: Limite de resultados
            
        Returns:
            Dict com dados e metadados
        """
        df = self.repository.listar_por_subestacao(subestacao_codigo, skip=skip, limit=limit)
        total = self.repository.contar_por_subestacao(subestacao_codigo)
        
        return {
            "subestacao_codigo": subestacao_codigo,
            "data": df.to_dict('records') if not df.empty else [],
            "total": total,
            "skip": skip,
            "limit": limit,
            "tem_proxima": (skip + limit) < total
        }

    def listar_por_distribuidora(self, distribuidora: str, skip: int = 0, limit: int = 100) -> Dict:
        """
        Lista transformadores de uma distribuidora.
        
        Args:
            distribuidora: Nome da distribuidora
            skip: Offset para paginação
            limit: Limite de resultados
            
        Returns:
            Dict com dados e metadados
        """
        df = self.repository.listar_por_distribuidora(distribuidora, skip=skip, limit=limit)
        total = self.repository.contar_por_distribuidora(distribuidora)
        
        return {
            "distribuidora": distribuidora,
            "data": df.to_dict('records') if not df.empty else [],
            "total": total,
            "skip": skip,
            "limit": limit,
            "tem_proxima": (skip + limit) < total
        }

    def listar_por_tipo_tensao(self, tipo_tensao: str, skip: int = 0, limit: int = 100) -> Dict:
        """
        Lista transformadores por tipo de tensão (BT, MT, AT).
        
        Args:
            tipo_tensao: "BT", "MT" ou "AT"
            skip: Offset para paginação
            limit: Limite de resultados
            
        Returns:
            Dict com dados e metadados
        """
        if tipo_tensao not in ["BT", "MT", "AT"]:
            raise ValueError(f"Tipo de tensão inválido: {tipo_tensao}. Use BT, MT ou AT")
        
        df = self.repository.buscar_por_tipo_tensao(tipo_tensao, skip=skip, limit=limit)
        
        return {
            "tipo_tensao": tipo_tensao,
            "data": df.to_dict('records') if not df.empty else [],
            "total": len(df),
            "skip": skip,
            "limit": limit
        }

    # ========================================================================
    # BUSCA ESPACIAL
    # ========================================================================

    def buscar_por_regiao(self, min_lat: float, min_lon: float, max_lat: float, max_lon: float, 
                          skip: int = 0, limit: int = 100) -> Dict:
        """
        Busca transformadores dentro de um bounding box (região geográfica).
        
        Args:
            min_lat, min_lon, max_lat, max_lon: Coordenadas do retângulo
            skip: Offset para paginação
            limit: Limite de resultados
            
        Returns:
            Dict com dados encontrados
        """
        # Validar coordenadas
        if not (-90 <= min_lat <= max_lat <= 90):
            raise ValueError("Latitude inválida")
        if not (-180 <= min_lon <= max_lon <= 180):
            raise ValueError("Longitude inválida")
        
        df = self.repository.buscar_por_regiao(min_lat, min_lon, max_lat, max_lon)
        
        # Aplicar paginação em memory
        total = len(df)
        df_paginado = df.iloc[skip:skip + limit]
        
        return {
            "bbox": {
                "min_lat": min_lat,
                "min_lon": min_lon,
                "max_lat": max_lat,
                "max_lon": max_lon
            },
            "data": df_paginado.to_dict('records') if not df_paginado.empty else [],
            "total": total,
            "skip": skip,
            "limit": limit,
            "tem_proxima": (skip + limit) < total
        }

    # ========================================================================
    # ESTATÍSTICAS
    # ========================================================================

    def obter_estatisticas_gerais(self) -> Dict:
        """
        Retorna estatísticas gerais de transformadores.
        
        Returns:
            Dict com estatísticas
        """
        return self.repository.obter_estadisticas_gerais()

    def obter_estatisticas_areas(self) -> Dict:
        """
        Retorna estatísticas de áreas de cobertura.
        
        Returns:
            Dict com estatísticas de áreas
        """
        return self.repository.obter_estatisticas_areas()

    # ========================================================================
    # EXPORT
    # ========================================================================

    def exportar_csv(self) -> str:
        """
        Exporta transformadores para CSV.
        
        Returns:
            String CSV
        """
        df = self.repository.exportar_como_dataframe()
        return df.to_csv(index=False)

    def exportar_geojson(self) -> Dict:
        """
        Exporta transformadores para GeoJSON.
        
        Returns:
            Dict GeoJSON FeatureCollection
        """
        df = self.repository.exportar_como_dataframe()
        
        features = []
        for _, row in df.iterrows():
            if row['latitude'] and row['longitude']:
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(row['longitude']), float(row['latitude'])]
                    },
                    "properties": {
                        "id": int(row['id']),
                        "codigo": str(row['codigo']),
                        "nome": str(row['nome']) if row['nome'] else None,
                        "potencia_kva": float(row['potencia_kva']) if row['potencia_kva'] else None,
                        "tipo_tensao": str(row['tipo_tensao']) if row['tipo_tensao'] else None,
                        "distribuidora": str(row['distribuidora']) if row['distribuidora'] else None,
                        "area_km2": float(row['area_km2']) if row['area_km2'] else None
                    }
                })
        
        return {
            "type": "FeatureCollection",
            "features": features,
            "total_features": len(features)
        }

    def exportar_json(self) -> Dict:
        """
        Exporta transformadores para JSON simples.
        
        Returns:
            Dict com lista de transformadores
        """
        df = self.repository.exportar_como_dataframe()
        return {
            "transformadores": df.to_dict('records'),
            "total": len(df)
        }

    # ========================================================================
    # CONSUMIDORES ASSOCIADOS
    # ========================================================================

    def obter_consumidores_associados(self, transformador_codigo: str) -> Dict:
        """
        Retorna contagem de consumidores BT/MT/AT associados a um transformador.
        
        Args:
            transformador_codigo: Código do transformador
            
        Returns:
            Dict com contagem por tipo de tensão
        """
        return self.repository.contar_consumidores_por_transformador(transformador_codigo)

    def listar_consumidores_bt_do_transformador(self, transformador_codigo: str, limit: int = 100) -> Dict:
        """
        Lista consumidores de Baixa Tensão (BT) associados a um transformador.
        
        Args:
            transformador_codigo: Código do transformador
            limit: Limite de resultados
            
        Returns:
            Dict com lista de consumidores BT
        """
        df = self.repository.obter_consumidores_bt_por_transformador(transformador_codigo, limit=limit)
        
        return {
            "transformador_codigo": transformador_codigo,
            "tipo_consumidor": "BT (Baixa Tensão)",
            "data": df.to_dict('records') if not df.empty else [],
            "total": len(df)
        }

    def listar_consumidores_mt_do_transformador(self, transformador_codigo: str, limit: int = 100) -> Dict:
        """
        Lista consumidores de Média Tensão (MT) associados a um transformador.
        
        Args:
            transformador_codigo: Código do transformador
            limit: Limite de resultados
            
        Returns:
            Dict com lista de consumidores MT
        """
        df = self.repository.obter_consumidores_mt_por_transformador(transformador_codigo, limit=limit)
        
        return {
            "transformador_codigo": transformador_codigo,
            "tipo_consumidor": "MT (Média Tensão)",
            "data": df.to_dict('records') if not df.empty else [],
            "total": len(df)
        }

    def exportar(self, formato: str = "json") -> str | Dict:
        """
        Exporta transformadores em diferentes formatos.
        
        Args:
            formato: "csv", "json" ou "geojson"
            
        Returns:
            String (CSV) ou Dict (JSON/GeoJSON)
        """
        if formato == "csv":
            return self.exportar_csv()
        elif formato == "geojson":
            return self.exportar_geojson()
        elif formato == "json":
            return self.exportar_json()
        else:
            raise ValueError(f"Formato inválido: {formato}. Use csv, json ou geojson")

