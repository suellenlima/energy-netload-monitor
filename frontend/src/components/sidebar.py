from dataclasses import dataclass
from typing import List

import streamlit as st

from services.api_client import ApiClient
from utils.errors import show_error


@dataclass
class SidebarState:
    subsistema: str
    distribuidora: str
    multiplicador: int
    refresh: bool


def _load_distribuidoras(client: ApiClient) -> List[str]:
    result = client.get("/auxiliar/distribuidoras")
    if result.error:
        show_error(result.error, location="sidebar")
        return [""]
    if isinstance(result.data, list):
        return result.data
    return [""]


def render_sidebar(client: ApiClient) -> SidebarState:
    st.sidebar.header("Configura??es")

    subsistema = st.sidebar.selectbox(
        "Subsistema (ONS)",
        ["SUDESTE", "SUL", "NORDESTE", "NORTE"],
    )

    st.sidebar.subheader("An?lise por Distribuidora")
    opcoes_distribuidoras = _load_distribuidoras(client)
    distribuidora = st.sidebar.selectbox("Concess?o (GD):", opcoes_distribuidoras)

    st.sidebar.markdown("---")
    st.sidebar.subheader("Cruzamento com IA")
    multiplicador = st.sidebar.slider("Proje??o de Fraudes (Quantidade de casos)", 1, 5000, 1)
    st.sidebar.info("Arraste para simular o impacto de m?ltiplas fraudes na rede.")

    refresh = st.sidebar.button("Atualizar Dashboard", type="primary")

    return SidebarState(
        subsistema=subsistema,
        distribuidora=distribuidora,
        multiplicador=multiplicador,
        refresh=refresh,
    )