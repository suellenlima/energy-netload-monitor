from typing import Any, Dict, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from services.api_client import ApiClient
from utils.errors import show_error


def load_carga_data(client: ApiClient, subsistema: str, distribuidora: str) -> pd.DataFrame:
    params = {"subsistema": subsistema}
    if distribuidora:
        params["distribuidora"] = distribuidora

    result = client.get("/analise/carga-oculta", params=params)
    if result.error:
        show_error(result.error)
        return pd.DataFrame()

    if not result.data:
        return pd.DataFrame()

    df = pd.DataFrame(result.data)
    if not df.empty and "hora" in df.columns:
        df["hora"] = pd.to_datetime(df["hora"])
    return df


def render_carga_section(
    df_carga: pd.DataFrame,
    impacto_projecao_mw: float,
    multiplicador: int,
    subsistema: str,
) -> None:
    st.header(f"Curva de Carga Líquida ({subsistema})")

    if df_carga.empty:
        st.info("Sem dados de carga para o período selecionado.")
        return

    if impacto_projecao_mw > 0:
        df_carga["carga_auditada"] = df_carga["carga_real_estimada"] + impacto_projecao_mw

    col1, col2, col3, col4 = st.columns(4)

    carga_atual = df_carga.iloc[-1]["carga_ons"]
    oculta_oficial = df_carga.iloc[-1]["estimativa_solar_mw"]
    pico_solar_dia = df_carga["estimativa_solar_mw"].max()
    hora_pico = df_carga.loc[df_carga["estimativa_solar_mw"].idxmax(), "hora"].strftime("%Hh")

    col1.metric("Carga Rede (ONS)", f"{carga_atual:,.0f} MW")
    col2.metric("GD Distribuída (Agora)", f"{oculta_oficial:,.0f} MW", delta="Oficial")

    if impacto_projecao_mw > 0:
        col3.metric(
            "Carga Auditada (IA)",
            f"{oculta_oficial + impacto_projecao_mw:,.0f} MW",
            delta="RISCO",
            delta_color="inverse",
        )
    else:
        col3.metric("Carga Auditada", "Sem desvios")

    col4.metric(f"Pico Solar (às {hora_pico})", f"{pico_solar_dia:,.0f} MW", delta="Atividade Solar")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_carga["hora"],
            y=df_carga["carga_ons"],
            mode="lines",
            name="Carga Rede (ONS)",
            line=dict(color="#00BFFF", width=3),
            fill="tozeroy",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_carga["hora"],
            y=df_carga["carga_real_estimada"],
            mode="lines",
            name="Consumo Real (ANEEL)",
            line=dict(color="#FFA500", width=2, dash="dot"),
        )
    )

    if impacto_projecao_mw > 0 and "carga_auditada" in df_carga.columns:
        fig.add_trace(
            go.Scatter(
                x=df_carga["hora"],
                y=df_carga["carga_auditada"],
                mode="lines",
                name=f"Cenário Projetado ({multiplicador}x)",
                line=dict(color="#FF0000", width=2, dash="dashdot"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_carga["hora"],
                y=df_carga["carga_auditada"],
                fill="tonexty",
                fillcolor="rgba(255, 0, 0, 0.3)",
                name="Carga Fantasma",
                mode="none",
            )
        )

    fig.update_layout(height=450, template="plotly_dark", title="Impacto das Fraudes na Curva de Carga")
    st.plotly_chart(fig, use_container_width=True)


def render_classes_consumo(client: ApiClient, distribuidora: str) -> None:
    result = client.get("/analise/classes-consumo", params={"distribuidora": distribuidora})
    if result.error:
        show_error(result.error)
        return

    if not result.data:
        return

    df_classes = pd.DataFrame(result.data)
    if df_classes.empty:
        return

    st.markdown("---")
    st.header("Detalhamento da Concessão")
    c1, c2 = st.columns([1, 2])
    with c1:
        st.dataframe(df_classes, use_container_width=True, hide_index=True)
    with c2:
        fig_pie = px.pie(df_classes, values="mw", names="classe", hole=0.4, title="Perfil de Consumo")
        fig_pie.update_layout(template="plotly_dark")
        st.plotly_chart(fig_pie, use_container_width=True)