"""Infrastructure Layer - RealTimeEstimation Mapper (DTO Converter)"""

from typing import Dict, Any
from datetime import datetime

from ....domain.realtime_estimation import (
    Irradiancia,
    CargaONS,
    GeracaoMMGD,
    EstadoSistemaReal,
    Previsao,
)


class RealTimeEstimationMapper:
    """Converte entre modelo ORM, domínio e DTOs"""

    @staticmethod
    def irradiancia_to_domain(row: Dict[str, Any]) -> Irradiancia:
        """Converte row do banco para Irradiancia"""
        return Irradiancia(
            wm2=float(row.get("wm2", 0)),
            nuvem_percentual=float(row.get("nuvem_percentual", 0)),
            confiabilidade=float(row.get("confiabilidade", 0.7)),
        )

    @staticmethod
    def carga_ons_to_domain(row: Dict[str, Any]) -> CargaONS:
        """Converte row do banco para CargaONS"""
        return CargaONS(
            carga_mw=float(row.get("carga_mw", 0)),
            hora_medicao=row.get("hora_medicao") or datetime.now(),
            subsistema=row.get("subsistema", "SE"),
            precisao=float(row.get("precisao", 0.95)),
        )

    @staticmethod
    def geracao_mmgd_to_domain(row: Dict[str, Any]) -> GeracaoMMGD:
        """Converte row do banco para GeracaoMMGD"""
        return GeracaoMMGD(
            geracao_estimada_mw=float(row.get("geracao_estimada_mw", 0)),
            confiabilidade_estimativa=float(row.get("confiabilidade_estimativa", 0.8)),
            hora_calculo=row.get("hora_calculo") or datetime.now(),
            fatores_usados=row.get("fatores_usados") or {},
        )

    @staticmethod
    def estado_sistema_real_to_domain(row: Dict[str, Any]) -> EstadoSistemaReal:
        """Converte row do banco para EstadoSistemaReal"""
        return EstadoSistemaReal(
            timestamp=row.get("timestamp") or datetime.now(),
            hora_atual=row.get("hora_atual") or datetime.now(),
            carga_ons_mw=float(row.get("carga_ons_mw", 0)),
            geracao_mmgd_mw=float(row.get("geracao_mmgd_mw", 0)),
            consumo_estimado_mw=float(row.get("consumo_estimado_mw", 0)),
            irradiancia_wm2=float(row.get("irradiancia_wm2", 0)),
            subsistema=row.get("subsistema", "SE"),
            confiabilidade_geral=float(row.get("confiabilidade_geral", 0.8)),
        )

    @staticmethod
    def previsao_to_domain(row: Dict[str, Any]) -> Previsao:
        """Converte row do banco para Previsao"""
        return Previsao(
            proximaHora_mw=float(row.get("proxima_hora_mw", 0)),
            proximas3horas_mw=float(row.get("proximas_3horas_mw", 0)),
            proximias24horas_mw=float(row.get("proximas_24horas_mw", 0)),
            confiabilidade=float(row.get("confiabilidade", 0.75)),
            data_geracao=row.get("data_geracao") or datetime.now(),
        )

    @staticmethod
    def estado_to_response(estado: EstadoSistemaReal) -> Dict[str, Any]:
        """Converte EstadoSistemaReal para resposta JSON"""
        return {
            "timestamp": estado.timestamp.isoformat(),
            "hora_atual": estado.hora_atual.isoformat(),
            "carga_ons_mw": estado.carga_ons_mw,
            "geracao_mmgd_mw": estado.geracao_mmgd_mw,
            "consumo_estimado_mw": estado.consumo_estimado_mw,
            "irradiancia_wm2": estado.irradiancia_wm2,
            "subsistema": estado.subsistema,
            "confiabilidade_geral": estado.confiabilidade_geral,
            "carga_liquida_mw": max(0, estado.carga_ons_mw - estado.geracao_mmgd_mw),
        }

    @staticmethod
    def previsao_to_response(previsao: Previsao) -> Dict[str, Any]:
        """Converte Previsao para resposta JSON"""
        return {
            "proxima_hora_mw": previsao.proximaHora_mw,
            "proximas_3horas_mw": previsao.proximas3horas_mw,
            "proximas_24horas_mw": previsao.proximias24horas_mw,
            "confiabilidade": previsao.confiabilidade,
            "data_geracao": previsao.data_geracao.isoformat(),
        }
