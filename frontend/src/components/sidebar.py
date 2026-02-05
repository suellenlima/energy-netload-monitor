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


def _load_subsistemas(client: ApiClient) -> List[str]:
    """Carrega lista de subsistemas da API."""
    result = client.get("/auxiliar/subsistemas")
    if result.error:
        show_error(result.error, location="sidebar")
        return ["SUDESTE", "SUL", "NORDESTE", "NORTE"]  # fallback
    if isinstance(result.data, list):
        return result.data
    return ["SUDESTE", "SUL", "NORDESTE", "NORTE"]  # fallback


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
        st.session_state.subsistema = "Sudeste/Centro-Oeste"

    if "distribuidora" not in st.session_state:
        st.session_state.distribuidora = ""

    if "multiplicador" not in st.session_state:
        st.session_state.multiplicador = 1

    # Controles de filtros (não causam recarregamento automático)
    opcoes_subsistemas = _load_subsistemas(client)
    
    # Garantir que "Sudeste/Centro-Oeste" seja o padrão
    subsistema_padrao = "Sudeste/Centro-Oeste"
    if subsistema_padrao not in opcoes_subsistemas:
        opcoes_subsistemas.insert(0, subsistema_padrao)
    
    # Encontrar índice do subsistema atual
    subsistema_index = 0
    if st.session_state.subsistema in opcoes_subsistemas:
        subsistema_index = opcoes_subsistemas.index(st.session_state.subsistema)
    else:
        subsistema_index = opcoes_subsistemas.index(subsistema_padrao) if subsistema_padrao in opcoes_subsistemas else 0
    
    subsistema = st.sidebar.selectbox(
        "Subsistema (ONS)",
        opcoes_subsistemas,
        index=subsistema_index,
        key="subsistema_select"
    )

    st.sidebar.subheader("Análise por Distribuidora")
    opcoes_distribuidoras = _load_distribuidoras(client, subsistema)

    # Definir distribuidora padrão baseada no subsistema
    distribuidora_padrao = ""
    if subsistema == "Sudeste/Centro-Oeste":
        distribuidora_padrao = "LIGHT"
    
    # Encontrar índice da distribuidora atual
    dist_index = 0
    if st.session_state.distribuidora and st.session_state.distribuidora in opcoes_distribuidoras:
        dist_index = opcoes_distribuidoras.index(st.session_state.distribuidora)
    elif distribuidora_padrao and distribuidora_padrao in opcoes_distribuidoras:
        dist_index = opcoes_distribuidoras.index(distribuidora_padrao)
        st.session_state.distribuidora = distribuidora_padrao

    distribuidora = st.sidebar.selectbox(
        "Distribuidora:",
        opcoes_distribuidoras,
        index=dist_index,
        key="distribuidora_select"
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Cenário de MMGD Detectada")
    multiplicador = st.sidebar.slider(
        "Projeção de MMGDs detectadas (Multiplicador de casos)",
        1, 5000,
        st.session_state.multiplicador,
        key="multiplicador_slider"
    )
    st.sidebar.info("Arraste para simular o impacto de múltiplas placas solares não mapeadas na rede.")

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