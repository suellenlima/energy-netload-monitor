from typing import Any, Dict, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def render_auditoria(dados_ia: Optional[Dict[str, Any]], impacto_projecao_mw: float, multiplicador: int) -> None:
    st.markdown("---")
    st.header("Relatório de Auditoria de Integridade")

    if not dados_ia or dados_ia.get("status") != "ALERTA":
        st.info("Selecione uma Distribuidora na barra lateral para iniciar a auditoria.")
        return

    with st.container():
        c1, c2 = st.columns([1.5, 1])

        with c1:
            st.subheader("Evidência Detectada (Satélite)")
            kpi1, kpi2 = st.columns(2)
            kpi3, kpi4 = st.columns(2)

            fraude_kw = _to_float(dados_ia.get("fraude_kw"))
            data_inspecao = dados_ia.get("data")
            area_m2 = fraude_kw / 0.2 if fraude_kw else 0
            classe_ia = str(dados_ia.get("classe_ia", "N/A"))
            classe_icone = "IND" if "Industrial" in classe_ia or "Comercial" in classe_ia else "RES"

            kpi1.metric("Classificação (IA)", f"{classe_icone} {classe_ia}")
            if data_inspecao:
                kpi2.metric("Data da Varredura", pd.to_datetime(data_inspecao).strftime("%d/%m/%Y"))
            else:
                kpi2.metric("Data da Varredura", "N/A")
            kpi3.metric("Área Estimada", f"{area_m2:.0f} m2")
            kpi4.metric("Potência Oculta", f"{fraude_kw:.2f} kW", delta="Desvio", delta_color="inverse")

            st.info("O algoritmo identificou padrões visuais incompatíveis na coordenada analisada.")

        with c2:
            st.subheader("Nível de Risco Projetado")
            max_risco_mw = 500
            fig_gauge = go.Figure(
                go.Indicator(
                    mode="gauge+number+delta",
                    value=impacto_projecao_mw,
                    title={"text": f"Carga Fantasma ({multiplicador}x)"},
                    delta={"reference": 0, "increasing": {"color": "red"}},
                    gauge={
                        "axis": {"range": [None, max_risco_mw], "tickwidth": 1},
                        "bar": {"color": "#ff2b2b"},
                        "bgcolor": "rgba(0,0,0,0)",
                        "steps": [
                            {"range": [0, max_risco_mw * 0.3], "color": "rgba(0, 255, 0, 0.3)"},
                            {"range": [max_risco_mw * 0.3, max_risco_mw * 0.7], "color": "rgba(255, 165, 0, 0.3)"},
                            {"range": [max_risco_mw * 0.7, max_risco_mw], "color": "rgba(255, 0, 0, 0.3)"},
                        ],
                        "threshold": {
                            "line": {"color": "red", "width": 4},
                            "thickness": 0.75,
                            "value": impacto_projecao_mw,
                        },
                    },
                )
            )
            fig_gauge.update_layout(height=300, margin=dict(t=50, b=20, l=20, r=20), template="plotly_dark")
            st.plotly_chart(fig_gauge, use_container_width=True)