from typing import Any, Dict, Optional, Tuple

import streamlit as st

from services.api_client import ApiClient
from utils.errors import show_error


def fetch_alerta(client: ApiClient, distribuidora: str) -> Optional[Dict[str, Any]]:
    params = {}
    if distribuidora:
        params["distribuidora"] = distribuidora

    result = client.get("/analise/alertas-fraude", params=params)
    if result.error:
        show_error(result.error)
        return None

    if isinstance(result.data, dict) and result.data:
        return result.data
    return None


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def render_alerta(dados_ia: Optional[Dict[str, Any]], multiplicador: int) -> Tuple[float, float]:
    fraude_mw = 0.0
    impacto_projecao_mw = 0.0

    if dados_ia and dados_ia.get("status") == "ALERTA":
        fraude_kw = _to_float(dados_ia.get("fraude_kw"))
        oficial_kw = _to_float(dados_ia.get("oficial_kw"))
        fraude_mw = fraude_kw / 1000
        impacto_projecao_mw = fraude_mw * multiplicador

        with st.container():
            distribuidora = dados_ia.get("distribuidora", "DESCONHECIDA")
            st.error(f"ALERTA: FRAUDE DETECTADA NA CONCESSÃO {distribuidora}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Detecção Unitária (IA)", f"{fraude_kw:.2f} kW")
            c2.metric(
                f"Projeção ({multiplicador}x)",
                f"{impacto_projecao_mw:.2f} MW",
                delta="Carga Fantasma",
            )
            c3.metric("Potência Oficial", f"{oficial_kw:.2f} kW")
            c4.metric("Ação", "Despacho Térmico", delta="Urgente", delta_color="inverse")
            st.divider()

    return fraude_mw, impacto_projecao_mw