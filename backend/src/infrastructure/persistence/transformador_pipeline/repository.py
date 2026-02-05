"""Repository para pipeline de transformadores - Infrastructure Layer."""

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ..shared import BaseRepository


class TransformadorPipelineRepository(BaseRepository):
    """Repository para acesso aos dados do pipeline (telhados e painéis)."""

    def obter_transformador(self, transformador_id: int) -> Optional[Dict]:
        """Busca transformador por ID."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT id, codigo, nome, latitude, longitude, distribuidora, subestacao_codigo
                    FROM transformadores_aneel
                    WHERE id = :trans_id AND ativo = TRUE
                """), {"trans_id": transformador_id})

                row = result.fetchone()
                if not row:
                    self.logger.warning(f"Transformador {transformador_id} não encontrado ou inativo")
                    return None

                return {
                    "id": int(row[0]),
                    "codigo": row[1],
                    "nome": row[2],
                    "latitude": float(row[3]) if row[3] else None,
                    "longitude": float(row[4]) if row[4] else None,
                    "distribuidora": row[5],
                    "subestacao_codigo": row[6]
                }
        except Exception as e:
            self.logger.error(f"Erro ao obter transformador {transformador_id}: {e}")
            return None

    def limpar_paineis_do_transformador(self, transformador_id: int) -> bool:
        """Limpa todos os painéis de um transformador."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    DELETE FROM paineis_solares_detectados 
                    WHERE transformador_id = :trans_id
                """), {"trans_id": transformador_id})
            return True
        except Exception as e:
            self.logger.error(f"Erro ao limpar painéis: {e}")
            return False

    def limpar_potencia_do_transformador(self, transformador_id: int) -> bool:
        """Limpa resumo de potência de um transformador."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    DELETE FROM potencia_telhados 
                    WHERE transformador_id = :trans_id
                """), {"trans_id": transformador_id})
            return True
        except Exception as e:
            self.logger.error(f"Erro ao limpar potência: {e}")
            return False

    def salvar_referencia_imagem(self,
                                  transformador_id: int,
                                  indice: int,
                                  url: str,
                                  caminho_disco: str) -> bool:
        """Salva referência da imagem no banco."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO satelite_requisicoes_google_maps 
                    (transformador_id, tipo_requisicao, url_satellite, zoom, tamanho_pixels, status, data_requisicao)
                    VALUES (:trans_id, :tipo, :url, 18, '640x640', 'processada', NOW())
                    ON CONFLICT DO NOTHING
                """), {
                    "trans_id": transformador_id,
                    "tipo": f"grid_painel_{indice}",
                    "url": url
                })
            self.logger.debug(f"Referência de imagem salva para transformador {transformador_id}, imagem {indice}")
            return True
        except Exception as e:
            self.logger.warning(f"Erro ao salvar ref de imagem: {e}")
            return False

    def obter_telhado_por_id(self, telhado_id: int) -> Optional[Dict]:
        """Busca dados do telhado."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT transformador_id, id as subestacao_id
                    FROM telhados_detectados_transformador
                    WHERE id = :telhado_id
                """), {"telhado_id": telhado_id})

                row = result.fetchone()
                if not row:
                    return None

                return {
                    "transformador_id": int(row[0]) if row[0] else None,
                    "subestacao_id": int(row[1]) if row[1] else None
                }
        except Exception as e:
            self.logger.error(f"Erro ao obter telhado: {e}")
            return None

    def limpar_paineis_do_telhado(self, telhado_id: int) -> bool:
        """Limpa painéis anteriores de um telhado."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    DELETE FROM paineis_solares_detectados 
                    WHERE telhado_id = :telhado_id
                """), {"telhado_id": telhado_id})
            return True
        except Exception as e:
            self.logger.error(f"Erro ao limpar painéis do telhado: {e}")
            return False

    def salvar_painel(self, painel_data: Dict) -> bool:
        """Salva um painel detectado no banco."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO paineis_solares_detectados
                    (telhado_id, transformador_id, subestacao_id, 
                     bbox_json, centroide_json, area_pixeis, area_m2,
                     confianca, tipo_painel, potencia_w, timestamp_deteccao)
                    VALUES (:telhado_id, :trans_id, :sub_id,
                            :bbox, :centroide, :area_px, :area_m2,
                            :conf, :tipo, :potencia, :timestamp)
                """), painel_data)
            return True
        except Exception as e:
            self.logger.error(f"Erro ao salvar painel: {e}")
            return False

    def salvar_paineis_lote(self, paineis: List[Dict]) -> int:
        """Salva múltiplos painéis em uma única transação."""
        count = 0
        try:
            with self.engine.begin() as conn:
                for painel_data in paineis:
                    try:
                        conn.execute(text("""
                            INSERT INTO paineis_solares_detectados
                            (telhado_id, transformador_id, subestacao_id, 
                             bbox_json, centroide_json, area_pixeis, area_m2,
                             confianca, tipo_painel, potencia_w, timestamp_deteccao)
                            VALUES (:telhado_id, :trans_id, :sub_id,
                                    :bbox, :centroide, :area_px, :area_m2,
                                    :conf, :tipo, :potencia, :timestamp)
                        """), painel_data)
                        count += 1
                    except Exception as e:
                        self.logger.warning(f"Erro ao salvar painel individual: {e}")
                        continue
            return count
        except Exception as e:
            self.logger.error(f"Erro em salvar_paineis_lote: {e}")
            return count

    def limpar_potencia_do_telhado(self, telhado_id: int) -> bool:
        """Limpa resumo de potência de um telhado."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    DELETE FROM potencia_telhados 
                    WHERE telhado_id = :telhado_id
                """), {"telhado_id": telhado_id})
            return True
        except Exception as e:
            self.logger.error(f"Erro ao limpar potência do telhado: {e}")
            return False

    def salvar_potencia_telhado(self, potencia_data: Dict) -> bool:
        """Salva resumo de potência para um telhado."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO potencia_telhados
                    (telhado_id, transformador_id, num_paineis, area_total_m2,
                     potencia_instalada_kw, producao_diaria_kwh, producao_anual_kwh,
                     economia_anual_brl, potencia_por_m2)
                    VALUES (:telhado_id, :trans_id, :num, :area,
                            :pot_kw, :prod_dia, :prod_ano, :economia, :pot_m2)
                """), potencia_data)
            return True
        except Exception as e:
            self.logger.error(f"Erro ao salvar potência do telhado: {e}")
            return False

    def contar_paineis_do_transformador(self, transformador_id: int) -> int:
        """Conta total de painéis de um transformador."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM paineis_solares_detectados 
                    WHERE transformador_id = :trans_id
                """), {"trans_id": transformador_id})

                row = result.fetchone()
                return int(row[0]) if row else 0
        except Exception as e:
            self.logger.error(f"Erro ao contar painéis: {e}")
            return 0

    def contar_telhados_do_transformador(self, transformador_id: int) -> int:
        """Conta total de telhados de um transformador."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM telhados_detectados_transformador 
                    WHERE transformador_id = :trans_id
                """), {"trans_id": transformador_id})

                row = result.fetchone()
                return int(row[0]) if row else 0
        except Exception as e:
            self.logger.error(f"Erro ao contar telhados: {e}")
            return 0
