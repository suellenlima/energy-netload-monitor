"""Infrastructure Layer - RealTimeEstimation SQLAlchemy Repository"""

from typing import List, Optional
from datetime import datetime, timedelta
from sqlalchemy import text

from ....domain.realtime_estimation import (
    RealTimeEstimationRepository,
    Irradiancia,
    CargaONS,
    GeracaoMMGD,
    EstadoSistemaReal,
    Previsao,
    EstadoNaoDisponibleError,
    DadosIrradianciaInvalidosError,
)
from ....core.database import get_db_connection
from .mapper import RealTimeEstimationMapper


class SQLAlchemyRealTimeEstimationRepository(RealTimeEstimationRepository):
    """SQLAlchemy implementation of RealTimeEstimation repository using raw SQL"""

    def obter_carga_ons(self, subsistema: str) -> Optional[CargaONS]:
        """Obtém a carga atual do ONS para um subsistema"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    carga_mw,
                    hora_medicao,
                    subsistema,
                    precisao
                FROM cargas_ons
                WHERE subsistema = :subsistema
                ORDER BY hora_medicao DESC
                LIMIT 1
            """), {"subsistema": subsistema})

            row = result.fetchone()
            if not row:
                raise EstadoNaoDisponibleError(f"Carga ONS não disponível para {subsistema}")

            row_dict = dict(row._mapping)
            return RealTimeEstimationMapper.carga_ons_to_domain(row_dict)
        except EstadoNaoDisponibleError:
            raise
        except Exception as e:
            raise EstadoNaoDisponibleError(f"Erro ao buscar carga ONS: {str(e)}")

    def obter_irradiancia_atual(self, latitude: float, longitude: float) -> Irradiancia:
        """Obtém a irradiância solar atual para coordenadas"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    wm2,
                    nuvem_percentual,
                    confiabilidade
                FROM irradiancia_solar
                WHERE latitude = :latitude 
                    AND longitude = :longitude
                ORDER BY timestamp DESC
                LIMIT 1
            """), {"latitude": latitude, "longitude": longitude})

            row = result.fetchone()
            if not row:
                raise DadosIrradianciaInvalidosError(
                    f"Irradiância não disponível para ({latitude}, {longitude})"
                )

            row_dict = dict(row._mapping)
            return RealTimeEstimationMapper.irradiancia_to_domain(row_dict)
        except DadosIrradianciaInvalidosError:
            raise
        except Exception as e:
            raise DadosIrradianciaInvalidosError(f"Erro ao buscar irradiância: {str(e)}")

    def obter_geracao_mmgd_estimada(self, subsistema: str) -> Optional[GeracaoMMGD]:
        """Obtém a geração estimada de MMGD para um subsistema"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    geracao_estimada_mw,
                    confiabilidade_estimativa,
                    hora_calculo,
                    fatores_usados
                FROM geracao_mmgd
                WHERE subsistema = :subsistema
                ORDER BY hora_calculo DESC
                LIMIT 1
            """), {"subsistema": subsistema})

            row = result.fetchone()
            if not row:
                return None

            row_dict = dict(row._mapping)
            return RealTimeEstimationMapper.geracao_mmgd_to_domain(row_dict)
        except Exception as e:
            raise EstadoNaoDisponibleError(f"Erro ao buscar geração MMGD: {str(e)}")

    def obter_estado_atual(self, subsistema: str) -> EstadoSistemaReal:
        """Obtém o estado atual completo do sistema"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    timestamp,
                    hora_atual,
                    carga_ons_mw,
                    geracao_mmgd_mw,
                    consumo_estimado_mw,
                    irradiancia_wm2,
                    subsistema,
                    confiabilidade_geral
                FROM estado_sistema_realtime
                WHERE subsistema = :subsistema
                ORDER BY timestamp DESC
                LIMIT 1
            """), {"subsistema": subsistema})

            row = result.fetchone()
            if not row:
                raise EstadoNaoDisponibleError(f"Estado não disponível para {subsistema}")

            row_dict = dict(row._mapping)
            return RealTimeEstimationMapper.estado_sistema_real_to_domain(row_dict)
        except EstadoNaoDisponibleError:
            raise
        except Exception as e:
            raise EstadoNaoDisponibleError(f"Erro ao buscar estado: {str(e)}")

    def obter_previsoes(self, subsistema: str, horas: int = 24) -> List[Previsao]:
        """Obtém previsões de carga para as próximas horas"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    proxima_hora_mw,
                    proximas_3horas_mw,
                    proximas_24horas_mw,
                    confiabilidade,
                    data_geracao
                FROM previsoes_carga
                WHERE subsistema = :subsistema
                    AND data_geracao >= NOW() - INTERVAL '24 hours'
                ORDER BY data_geracao DESC
                LIMIT :horas
            """), {"subsistema": subsistema, "horas": horas})

            rows = result.fetchall()
            previsoes = []
            for row in rows:
                row_dict = dict(row._mapping)
                previsoes.append(RealTimeEstimationMapper.previsao_to_domain(row_dict))

            return previsoes
        except Exception as e:
            raise EstadoNaoDisponibleError(f"Erro ao buscar previsões: {str(e)}")

    def salvar_estado(self, estado: EstadoSistemaReal) -> EstadoSistemaReal:
        """Salva o estado do sistema no banco para histórico"""
        try:
            conn = get_db_connection()
            conn.execute(text("""
                INSERT INTO estado_sistema_realtime (
                    timestamp,
                    hora_atual,
                    carga_ons_mw,
                    geracao_mmgd_mw,
                    consumo_estimado_mw,
                    irradiancia_wm2,
                    subsistema,
                    confiabilidade_geral
                ) VALUES (
                    :timestamp,
                    :hora_atual,
                    :carga_ons_mw,
                    :geracao_mmgd_mw,
                    :consumo_estimado_mw,
                    :irradiancia_wm2,
                    :subsistema,
                    :confiabilidade_geral
                )
            """), {
                "timestamp": estado.timestamp,
                "hora_atual": estado.hora_atual,
                "carga_ons_mw": estado.carga_ons_mw,
                "geracao_mmgd_mw": estado.geracao_mmgd_mw,
                "consumo_estimado_mw": estado.consumo_estimado_mw,
                "irradiancia_wm2": estado.irradiancia_wm2,
                "subsistema": estado.subsistema,
                "confiabilidade_geral": estado.confiabilidade_geral,
            })
            conn.commit()
            return estado
        except Exception as e:
            raise EstadoNaoDisponibleError(f"Erro ao salvar estado: {str(e)}")
