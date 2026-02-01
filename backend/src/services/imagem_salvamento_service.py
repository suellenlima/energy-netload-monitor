#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Serviço para salvar imagens no banco de dados
- Google Maps (RGB, zoom 19, área poligonal)
- CBERS-4A (multibanda, 5 bandas espectrais)
"""

import logging
import io
import json
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
from sqlalchemy import text, Engine
import requests
from PIL import Image
import numpy as np

logger = logging.getLogger(__name__)

class ImagemSalvamentoService:
    """Salva imagens de múltiplas fontes na tabela satelite_imagens"""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def salvar_imagem_google_maps(self,
                                  subestacao_id: int,
                                  transformador_id: int,
                                  url: str,
                                  latitude: float,
                                  longitude: float,
                                  zoom: int = 19,
                                  largura: int = 640,
                                  altura: int = 640,
                                  vertices_poligono: Optional[list] = None,
                                  resolucao_m: float = 1.0) -> Dict[str, Any]:
        """
        Salva imagem do Google Maps na tabela satelite_imagens
        
        Args:
            subestacao_id: ID da subestação
            transformador_id: ID do transformador
            url: URL da imagem Google Maps
            latitude: Latitude do centro
            longitude: Longitude do centro
            zoom: Nível de zoom (máximo=19)
            largura: Largura em pixels
            altura: Altura em pixels
            vertices_poligono: Vértices da área poligonal (se houver)
            resolucao_m: Resolução em metros/pixel
            
        Returns:
            Dicionário com resultado do salvamento
        """
        
        try:
            logger.info(f"[GOOGLE MAPS] Salvando imagem para subestação {subestacao_id}")
            
            # Baixar imagem para cálculos
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                raise Exception(f"Erro ao baixar imagem: HTTP {response.status_code}")
            
            img = Image.open(io.BytesIO(response.content))
            
            # Converter para RGB e obter dimensões
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            img_array = np.array(img)
            
            # Calcular bbox (bounding box)
            bbox = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [longitude - 0.01, latitude - 0.01],
                        [longitude + 0.01, latitude - 0.01],
                        [longitude + 0.01, latitude + 0.01],
                        [longitude - 0.01, latitude + 0.01],
                        [longitude - 0.01, latitude - 0.01]
                    ]]
                }
            }
            
            # Metadados
            propriedades = {
                'transformador_id': transformador_id,
                'zoom': zoom,
                'largura_pixels': largura,
                'altura_pixels': altura,
                'bandas': 3,  # RGB
                'nome_bandas': ['Red', 'Green', 'Blue'],
                'resolucao_m_pixel': resolucao_m,
                'area_poligonal': 'sim' if vertices_poligono else 'nao',
                'num_vertices_poligono': len(vertices_poligono) if vertices_poligono else 0,
                'rgb_medio': {
                    'r': float(np.mean(img_array[:, :, 0])),
                    'g': float(np.mean(img_array[:, :, 1])),
                    'b': float(np.mean(img_array[:, :, 2]))
                },
                'tamanho_arquivo_bytes': len(response.content)
            }
            
            # Salvar na tabela
            with self.engine.connect() as conn:
                query = text("""
                    INSERT INTO satelite_imagens (
                        subestacao_id,
                        sensor,
                        data_aquisicao,
                        resolucao_m,
                        url,
                        bbox_json,
                        propriedades_json
                    ) VALUES (
                        :sub_id,
                        :sensor,
                        :data_aquisicao,
                        :resolucao_m,
                        :url,
                        :bbox,
                        :propriedades
                    )
                    RETURNING id;
                """)
                
                result = conn.execute(query, {
                    'sub_id': subestacao_id,
                    'sensor': 'Google_Maps_Satellite',
                    'data_aquisicao': datetime.now(),
                    'resolucao_m': int(resolucao_m),
                    'url': url,
                    'bbox': json.dumps(bbox),  # JSONB
                    'propriedades': json.dumps(propriedades)  # JSONB
                })
                
                imagem_id = result.fetchone()[0]
                conn.commit()
                
                logger.info(f"✓ Google Maps salva com ID {imagem_id}")
                
                return {
                    'sucesso': True,
                    'imagem_id': imagem_id,
                    'sensor': 'Google_Maps_Satellite',
                    'transformador_id': transformador_id,
                    'resolucao_m': int(resolucao_m),
                    'propriedades': propriedades
                }
        
        except Exception as e:
            logger.error(f"✗ Erro ao salvar Google Maps: {e}")
            return {
                'sucesso': False,
                'erro': str(e)
            }
    
    def salvar_imagem_cbers4a(self,
                              subestacao_id: int,
                              transformador_id: int,
                              imagem_id_cbers: int,
                              urls_bandas: Dict[str, str],
                              latitude: float,
                              longitude: float,
                              vertices_poligono: Optional[list] = None,
                              resolucao_m: float = 2.0) -> Dict[str, Any]:
        """
        Salva referência de imagem CBERS-4A com bandas espectrais
        
        Args:
            subestacao_id: ID da subestação
            transformador_id: ID do transformador
            imagem_id_cbers: ID da imagem CBERS-4A
            urls_bandas: Dicionário com URLs das 5 bandas
            latitude: Latitude
            longitude: Longitude
            vertices_poligono: Vértices da área poligonal
            resolucao_m: Resolução (2m para CBERS-4A)
            
        Returns:
            Dicionário com resultado
        """
        
        try:
            logger.info(f"[CBERS-4A] Registrando referência para subestação {subestacao_id}")
            
            # Metadados das 5 bandas
            propriedades = {
                'transformador_id': transformador_id,
                'imagem_id_cbers': imagem_id_cbers,
                'resolucao_m_pixel': resolucao_m,
                'bandas': 5,
                'nome_bandas': ['Blue (B0)', 'Green (B1)', 'Red (B2)', 'NIR (B3)', 'SWIR (B4)'],
                'comprimentos_onda_nm': {
                    'B0': '0.45-0.52 (Azul)',
                    'B1': '0.52-0.59 (Verde)',
                    'B2': '0.63-0.69 (Vermelho)',
                    'B3': '0.77-0.89 (Infravermelho próximo)',
                    'B4': '1.55-1.75 (Infravermelho de onda curta)'
                },
                'urls_bandas': urls_bandas,
                'area_poligonal': 'sim' if vertices_poligono else 'nao',
                'num_vertices_poligono': len(vertices_poligono) if vertices_poligono else 0,
                'tipo_composicao': 'RGB_3bandas (B2-B1-B0)',
                'fonte_dados': 'INPE - Instituto Nacional de Pesquisas Espaciais'
            }
            
            # Bbox aproximado
            bbox = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [longitude - 0.05, latitude - 0.05],
                        [longitude + 0.05, latitude - 0.05],
                        [longitude + 0.05, latitude + 0.05],
                        [longitude - 0.05, latitude + 0.05],
                        [longitude - 0.05, latitude - 0.05]
                    ]]
                }
            }
            
            # Salvar na tabela
            with self.engine.connect() as conn:
                query = text("""
                    INSERT INTO satelite_imagens (
                        subestacao_id,
                        sensor,
                        data_aquisicao,
                        resolucao_m,
                        url,
                        bbox_json,
                        propriedades_json
                    ) VALUES (
                        :sub_id,
                        :sensor,
                        :data_aquisicao,
                        :resolucao_m,
                        :url,
                        :bbox,
                        :propriedades
                    )
                    RETURNING id;
                """)
                
                # URL principal (banda Red)
                url_principal = urls_bandas.get('red', '')
                
                result = conn.execute(query, {
                    'sub_id': subestacao_id,
                    'sensor': 'CBERS-4A_Multibanda',
                    'data_aquisicao': datetime.now(),
                    'resolucao_m': int(resolucao_m),
                    'url': url_principal,
                    'bbox': json.dumps(bbox),
                    'propriedades': json.dumps(propriedades)
                })
                
                imagem_id = result.fetchone()[0]
                conn.commit()
                
                logger.info(f"✓ CBERS-4A salva com ID {imagem_id} (5 bandas espectrais)")
                
                return {
                    'sucesso': True,
                    'imagem_id': imagem_id,
                    'sensor': 'CBERS-4A_Multibanda',
                    'transformador_id': transformador_id,
                    'resolucao_m': int(resolucao_m),
                    'bandas': 5,
                    'propriedades': propriedades
                }
        
        except Exception as e:
            logger.error(f"✗ Erro ao salvar CBERS-4A: {e}")
            return {
                'sucesso': False,
                'erro': str(e)
            }


    def salvar_imagens_grid_google_maps(self,
                                        subestacao_id: int,
                                        transformador_id: int,
                                        resultado_grid: Dict[str, Any]) -> Dict[str, Any]:
        """
        Salva todas as imagens de um grid do Google Maps no banco
        
        Args:
            subestacao_id: ID da subestação
            transformador_id: ID do transformador
            resultado_grid: Resultado do método buscar_imagens_grid_transformador
            
        Returns:
            Dicionário com resultado do salvamento
        """
        
        if not resultado_grid.get('sucesso'):
            return {
                'sucesso': False,
                'erro': resultado_grid.get('motivo', 'Grid não foi gerado com sucesso')
            }
        
        try:
            imagens = resultado_grid.get('imagens', [])
            imagens_salvas = []
            total_imagens = len(imagens)
            
            logger.info(f"[GRID] Salvando {total_imagens} imagens do grid para transformador {transformador_id}")
            
            for idx, imagem in enumerate(imagens):
                linha = imagem.get('linha', 0)
                coluna = imagem.get('coluna', 0)
                url = imagem.get('url')
                latitude = imagem.get('latitude')
                longitude = imagem.get('longitude')
                zoom = imagem.get('zoom', 20)
                tamanho = imagem.get('tamanho_pixels', '640x640')
                
                # Extrair largura e altura
                largura, altura = map(int, tamanho.split('x'))
                
                # Calcular resolução aproximada (metros por pixel)
                # Zoom 20 ≈ 1.19 m/pixel, zoom 19 ≈ 2.39 m/pixel
                resolucao_m = 156543.03392 * np.cos(np.radians(latitude)) / (2 ** zoom)
                
                try:
                    # Baixar imagem para cálculos
                    response = requests.get(url, timeout=15)
                    if response.status_code != 200:
                        logger.warning(f"   ⚠ Erro ao baixar imagem [{linha},{coluna}]: HTTP {response.status_code}")
                        continue
                    
                    img = Image.open(io.BytesIO(response.content))
                    
                    # Converter para RGB
                    if img.mode != 'RGB':
                        img = img.convert('RGB')
                    
                    img_array = np.array(img)
                    
                    # Calcular bbox
                    # Offset aproximado baseado no zoom e tamanho da imagem
                    lat_offset = (altura / 2) * resolucao_m / 111000  # 1 grau lat ≈ 111km
                    lon_offset = (largura / 2) * resolucao_m / (111000 * np.cos(np.radians(latitude)))
                    
                    bbox = {
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Polygon',
                            'coordinates': [[
                                [longitude - lon_offset, latitude - lat_offset],
                                [longitude + lon_offset, latitude - lat_offset],
                                [longitude + lon_offset, latitude + lat_offset],
                                [longitude - lon_offset, latitude + lat_offset],
                                [longitude - lon_offset, latitude - lat_offset]
                            ]]
                        }
                    }
                    
                    # Metadados específicos do grid
                    propriedades = {
                        'transformador_id': transformador_id,
                        'tipo_imagem': 'grid_cell',
                        'grid_posicao': {
                            'linha': linha,
                            'coluna': coluna
                        },
                        'grid_dimensoes': resultado_grid.get('dimensoes_grid'),
                        'zoom': zoom,
                        'largura_pixels': largura,
                        'altura_pixels': altura,
                        'bandas': 3,
                        'nome_bandas': ['Red', 'Green', 'Blue'],
                        'resolucao_m_pixel': round(resolucao_m, 4),
                        'offset_centro': {
                            'lat_km': imagem.get('offset_lat_km', 0),
                            'lon_km': imagem.get('offset_lon_km', 0)
                        },
                        'rgb_medio': {
                            'r': float(np.mean(img_array[:, :, 0])),
                            'g': float(np.mean(img_array[:, :, 1])),
                            'b': float(np.mean(img_array[:, :, 2]))
                        },
                        'tamanho_arquivo_bytes': len(response.content),
                        'indice_grid': idx,
                        'total_imagens_grid': total_imagens
                    }
                    
                    # Salvar na tabela
                    with self.engine.connect() as conn:
                        query = text("""
                            INSERT INTO satelite_imagens (
                                subestacao_id,
                                sensor,
                                data_aquisicao,
                                resolucao_m,
                                url,
                                bbox_json,
                                propriedades_json
                            ) VALUES (
                                :sub_id,
                                :sensor,
                                :data_aquisicao,
                                :resolucao_m,
                                :url,
                                :bbox,
                                :propriedades
                            )
                            RETURNING id;
                        """)
                        
                        result = conn.execute(query, {
                            'sub_id': subestacao_id,
                            'sensor': f'Google_Maps_Grid_Z{zoom}',
                            'data_aquisicao': datetime.now(),
                            'resolucao_m': int(resolucao_m),
                            'url': url,
                            'bbox': json.dumps(bbox),
                            'propriedades': json.dumps(propriedades)
                        })
                        
                        imagem_id = result.fetchone()[0]
                        conn.commit()
                        
                        imagens_salvas.append({
                            'imagem_id': imagem_id,
                            'linha': linha,
                            'coluna': coluna,
                            'url': url
                        })
                        
                        logger.info(f"   ✓ Grid [{linha},{coluna}] salva com ID {imagem_id}")
                
                except Exception as img_error:
                    logger.error(f"   ✗ Erro ao salvar imagem [{linha},{coluna}]: {img_error}")
                    continue
            
            logger.info(f"✅ Grid completo: {len(imagens_salvas)}/{total_imagens} imagens salvas no banco")
            
            return {
                'sucesso': True,
                'total_solicitadas': total_imagens,
                'total_salvas': len(imagens_salvas),
                'transformador_id': transformador_id,
                'subestacao_id': subestacao_id,
                'imagens': imagens_salvas,
                'sensor': f'Google_Maps_Grid_Z{resultado_grid.get("zoom_grid", 20)}',
                'dimensoes_grid': resultado_grid.get('dimensoes_grid')
            }
        
        except Exception as e:
            logger.error(f"✗ Erro ao salvar grid: {e}", exc_info=True)
            return {
                'sucesso': False,
                'erro': str(e)
            }


def criar_servico_salvamento(engine: Engine) -> ImagemSalvamentoService:
    """Factory para criar serviço"""
    return ImagemSalvamentoService(engine)
