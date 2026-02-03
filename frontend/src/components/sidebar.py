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


def _load_distribuidoras(client: ApiClient, subsistema: str | None = None) -> List[str]:
    result = client.get("/auxiliar/distribuidoras", params={"subsistema": subsistema})
    if result.error:
        show_error(result.error, location="sidebar")
        return [""]
    if isinstance(result.data, list):
        return result.data
    return [""]


def render_sidebar(client: ApiClient) -> SidebarState:
    """
    Renderiza sidebar com controles de filtros.

    Usa session_state para manter estado persistente e evitar
    recarregamentos desnecessários do dashboard.
    """
    st.sidebar.header("Configurações")

    # Inicializar session_state se necessário
    if "dashboard_loaded" not in st.session_state:
        st.session_state.dashboard_loaded = False

    if "subsistema" not in st.session_state:
        st.session_state.subsistema = "SUDESTE"

    if "distribuidora" not in st.session_state:
        st.session_state.distribuidora = ""

    if "multiplicador" not in st.session_state:
        st.session_state.multiplicador = 1

    # Controles de filtros (não causam recarregamento automático)
    subsistema = st.sidebar.selectbox(
        "Subsistema (ONS)",
        ["SUDESTE", "SUL", "NORDESTE", "NORTE"],
        index=["SUDESTE", "SUL", "NORDESTE", "NORTE"].index(st.session_state.subsistema),
        key="subsistema_select"
    )

    st.sidebar.subheader("Análise por Distribuidora")
    opcoes_distribuidoras = _load_distribuidoras(client, subsistema)

    # Encontrar índice da distribuidora atual
    dist_index = 0
    if st.session_state.distribuidora in opcoes_distribuidoras:
        dist_index = opcoes_distribuidoras.index(st.session_state.distribuidora)

    distribuidora = st.sidebar.selectbox(
        "Distribuidora:",
        opcoes_distribuidoras,
        index=dist_index,
        key="distribuidora_select"
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Cruzamento com IA")
    multiplicador = st.sidebar.slider(
        "Projeção de Fraudes (Quantidade de casos)",
        1, 5000,
        st.session_state.multiplicador,
        key="multiplicador_slider"
    )
    st.sidebar.info("Arraste para simular o impacto de múltiplas fraudes na rede.")

    # Botão de atualização - quando clicado, marca o dashboard como carregado
    if st.sidebar.button("Atualizar Dashboard", type="primary", key="btn_update_dashboard"):
        st.session_state.dashboard_loaded = True
        st.session_state.subsistema = subsistema
        st.session_state.distribuidora = distribuidora
        st.session_state.multiplicador = multiplicador

    # Retornar estado baseado em session_state (persistente)
    return SidebarState(
        subsistema=st.session_state.subsistema,
        distribuidora=st.session_state.distribuidora,
        multiplicador=st.session_state.multiplicador,
        refresh=st.session_state.dashboard_loaded,
    )