"""Infrastructure Layer - LoadCalculation SQLAlchemy Repository"""

from typing import List, Optional
from sqlalchemy import text

from ....domain.load_calculation import (
    LoadCalculationRepository,
    PerfilCargaHorario,
    ConsumoGranular,
    MMGD,
    CargaCalculada,
    CalibraçaoParametros,
    PerfilNaoEncontradoError,
    DadosConsumoInvalidosError,
    CalibracaoNaoDisponibleError,
    LoadCalculationError,
)
from ....core.database import get_db_connection
from .mapper import LoadCalculationMapper


class SQLAlchemyLoadCalculationRepository(LoadCalculationRepository):
    """SQLAlchemy implementation of LoadCalculation repository using raw SQL"""

    def obter_perfil_classe(self, classe: str) -> Optional[PerfilCargaHorario]:
        """Obtém o perfil de carga para uma classe"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    classe,
                    fatores_24h,
                    pico_hora,
                    minima_hora,
                    fator_pico
                FROM perfis_carga_classe
                WHERE UPPER(classe) = UPPER(:classe)
                LIMIT 1
            """), {"classe": classe})

            row = result.fetchone()
            if not row:
                raise PerfilNaoEncontradoError(f"Perfil não encontrado para classe: {classe}")

            row_dict = dict(row._mapping)
            return LoadCalculationMapper.perfil_carga_horario_to_domain(row_dict)
        except PerfilNaoEncontradoError:
            raise
        except Exception as e:
            raise PerfilNaoEncontradoError(f"Erro ao buscar perfil: {str(e)}")

    def obter_consumo_granular(self, classe: str) -> Optional[ConsumoGranular]:
        """Obtém dados granulares de consumo para uma classe"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    classe,
                    consumo_mwh,
                    quantidade_ucs,
                    consumo_medio_por_uc_kwh
                FROM consumo_granular_classe
                WHERE UPPER(classe) = UPPER(:classe)
                ORDER BY data_consumo DESC
                LIMIT 1
            """), {"classe": classe})

            row = result.fetchone()
            if not row:
                raise DadosConsumoInvalidosError(f"Consumo não encontrado para classe: {classe}")

            row_dict = dict(row._mapping)
            return LoadCalculationMapper.consumo_granular_to_domain(row_dict)
        except DadosConsumoInvalidosError:
            raise
        except Exception as e:
            raise DadosConsumoInvalidosError(f"Erro ao buscar consumo granular: {str(e)}")

    def obter_mmgd_subsistema(self, subsistema: str) -> Optional[MMGD]:
        """Obtém dados de MMGD para um subsistema"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    quantidade_instalacoes,
                    potencia_instalada_mw,
                    geracao_estimada_mw,
                    tipo_tecnologia
                FROM mmgd_subsistema
                WHERE UPPER(subsistema) = UPPER(:subsistema)
                ORDER BY data_atualizacao DESC
                LIMIT 1
            """), {"subsistema": subsistema})

            row = result.fetchone()
            if not row:
                return None

            row_dict = dict(row._mapping)
            return LoadCalculationMapper.mmgd_to_domain(row_dict)
        except Exception as e:
            raise LoadCalculationError(f"Erro ao buscar MMGD: {str(e)}")

    def obter_calibracao(self, classe: str) -> Optional[CalibraçaoParametros]:
        """Obtém parâmetros de calibração para uma classe"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    fator_sazonalidade,
                    fator_dia_semana,
                    fator_feriado,
                    ajuste_temperatura,
                    data_calibracao
                FROM calibracao_parametros
                WHERE UPPER(classe) = UPPER(:classe)
                ORDER BY data_calibracao DESC
                LIMIT 1
            """), {"classe": classe})

            row = result.fetchone()
            if not row:
                return None

            row_dict = dict(row._mapping)
            return LoadCalculationMapper.calibracao_parametros_to_domain(row_dict)
        except Exception as e:
            raise CalibracaoNaoDisponibleError(f"Erro ao buscar calibração: {str(e)}")

    def salvar_carga_calculada(self, carga: CargaCalculada) -> CargaCalculada:
        """Salva uma carga calculada no banco para histórico"""
        try:
            conn = get_db_connection()
            conn.execute(text("""
                INSERT INTO cargas_calculadas (
                    classe,
                    hora,
                    carga_base_mw,
                    carga_com_sazonalidade_mw,
                    carga_estimada_final_mw,
                    confiabilidade,
                    timestamp_criacao
                ) VALUES (
                    :classe,
                    :hora,
                    :carga_base_mw,
                    :carga_com_sazonalidade_mw,
                    :carga_estimada_final_mw,
                    :confiabilidade,
                    NOW()
                )
            """), {
                "classe": carga.classe,
                "hora": carga.hora,
                "carga_base_mw": carga.carga_base_mw,
                "carga_com_sazonalidade_mw": carga.carga_com_sazonalidade_mw,
                "carga_estimada_final_mw": carga.carga_estimada_final_mw,
                "confiabilidade": carga.confiabilidade,
            })
            conn.commit()
            return carga
        except Exception as e:
            raise LoadCalculationError(f"Erro ao salvar carga: {str(e)}")

    def obter_historico_cargas(self, classe: str, dias: int = 30) -> List[CargaCalculada]:
        """Obtém histórico de cargas calculadas para uma classe"""
        try:
            conn = get_db_connection()
            result = conn.execute(text("""
                SELECT 
                    classe,
                    hora,
                    carga_base_mw,
                    carga_com_sazonalidade_mw,
                    carga_estimada_final_mw,
                    confiabilidade
                FROM cargas_calculadas
                WHERE UPPER(classe) = UPPER(:classe)
                    AND timestamp_criacao >= NOW() - INTERVAL :dias || ' days'
                ORDER BY timestamp_criacao DESC
                LIMIT 1000
            """), {"classe": classe, "dias": dias})

            rows = result.fetchall()
            cargas = []
            for row in rows:
                row_dict = dict(row._mapping)
                cargas.append(LoadCalculationMapper.carga_calculada_to_domain(row_dict))

            return cargas
        except Exception as e:
            raise LoadCalculationError(f"Erro ao buscar histórico: {str(e)}")
