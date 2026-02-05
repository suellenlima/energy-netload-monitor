"""Infrastructure Layer - LoadCalculation Mapper (DTO Converter)"""

from typing import Dict, Any
from datetime import datetime

from ....domain.load_calculation import (
    PerfilCargaHorario,
    ConsumoGranular,
    MMGD,
    CargaCalculada,
    CalibraçaoParametros,
)


class LoadCalculationMapper:
    """Converte entre modelo ORM, domínio e DTOs"""

    @staticmethod
    def perfil_carga_horario_to_domain(row: Dict[str, Any]) -> PerfilCargaHorario:
        """Converte row do banco para PerfilCargaHorario"""
        fatores_24h = row.get("fatores_24h") or [1.0] * 24
        return PerfilCargaHorario(
            classe=row.get("classe", "RESIDENCIAL"),
            fatores_24h=fatores_24h,
            pico_hora=int(row.get("pico_hora", 19)),
            minima_hora=int(row.get("minima_hora", 3)),
            fator_pico=float(row.get("fator_pico", 1.5)),
        )

    @staticmethod
    def consumo_granular_to_domain(row: Dict[str, Any]) -> ConsumoGranular:
        """Converte row do banco para ConsumoGranular"""
        return ConsumoGranular(
            classe=row.get("classe", "RESIDENCIAL"),
            consumo_mwh=float(row.get("consumo_mwh", 0)),
            quantidade_ucs=int(row.get("quantidade_ucs", 0)),
            consumo_medio_por_uc_kwh=float(row.get("consumo_medio_por_uc_kwh", 0)),
        )

    @staticmethod
    def mmgd_to_domain(row: Dict[str, Any]) -> MMGD:
        """Converte row do banco para MMGD"""
        return MMGD(
            quantidade_instalacoes=int(row.get("quantidade_instalacoes", 0)),
            potencia_instalada_mw=float(row.get("potencia_instalada_mw", 0)),
            geracao_estimada_mw=float(row.get("geracao_estimada_mw", 0)),
            tipo_tecnologia=row.get("tipo_tecnologia", "SOLAR_FV"),
        )

    @staticmethod
    def carga_calculada_to_domain(row: Dict[str, Any]) -> CargaCalculada:
        """Converte row do banco para CargaCalculada"""
        return CargaCalculada(
            classe=row.get("classe", "RESIDENCIAL"),
            hora=int(row.get("hora", 0)),
            carga_base_mw=float(row.get("carga_base_mw", 0)),
            carga_com_sazonalidade_mw=float(row.get("carga_com_sazonalidade_mw", 0)),
            carga_estimada_final_mw=float(row.get("carga_estimada_final_mw", 0)),
            confiabilidade=float(row.get("confiabilidade", 0.85)),
        )

    @staticmethod
    def calibracao_parametros_to_domain(row: Dict[str, Any]) -> CalibraçaoParametros:
        """Converte row do banco para CalibraçaoParametros"""
        return CalibraçaoParametros(
            fator_sazonalidade=float(row.get("fator_sazonalidade", 1.0)),
            fator_dia_semana=float(row.get("fator_dia_semana", 1.0)),
            fator_feriado=float(row.get("fator_feriado", 0.8)),
            ajuste_temperatura=float(row.get("ajuste_temperatura", 0.0)),
            data_calibracao=row.get("data_calibracao") or datetime.now(),
        )

    @staticmethod
    def carga_to_response(carga: CargaCalculada) -> Dict[str, Any]:
        """Converte CargaCalculada para resposta JSON"""
        return {
            "classe": carga.classe,
            "hora": carga.hora,
            "carga_base_mw": carga.carga_base_mw,
            "carga_com_sazonalidade_mw": carga.carga_com_sazonalidade_mw,
            "carga_estimada_final_mw": carga.carga_estimada_final_mw,
            "confiabilidade": carga.confiabilidade,
        }

    @staticmethod
    def perfil_to_response(perfil: PerfilCargaHorario) -> Dict[str, Any]:
        """Converte PerfilCargaHorario para resposta JSON"""
        return {
            "classe": perfil.classe,
            "fatores_24h": perfil.fatores_24h,
            "pico_hora": perfil.pico_hora,
            "minima_hora": perfil.minima_hora,
            "fator_pico": perfil.fator_pico,
        }

    @staticmethod
    def consumo_to_response(consumo: ConsumoGranular) -> Dict[str, Any]:
        """Converte ConsumoGranular para resposta JSON"""
        return {
            "classe": consumo.classe,
            "consumo_mwh": consumo.consumo_mwh,
            "quantidade_ucs": consumo.quantidade_ucs,
            "consumo_medio_por_uc_kwh": consumo.consumo_medio_por_uc_kwh,
        }

    @staticmethod
    def mmgd_to_response(mmgd: MMGD) -> Dict[str, Any]:
        """Converte MMGD para resposta JSON"""
        return {
            "quantidade_instalacoes": mmgd.quantidade_instalacoes,
            "potencia_instalada_mw": mmgd.potencia_instalada_mw,
            "geracao_estimada_mw": mmgd.geracao_estimada_mw,
            "tipo_tecnologia": mmgd.tipo_tecnologia,
        }
