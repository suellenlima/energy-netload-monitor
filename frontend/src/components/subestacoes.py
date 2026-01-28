"""
Componente para exibir e gerenciar subestações no Streamlit.
"""

import streamlit as st
import pandas as pd
from services.api_client import ApiClient


def render_subestacoes_section(client: ApiClient, distribuidora: str | None = None):
    """
    Renderiza seção de subestações com abas para ONS e detectadas.
    """
    st.subheader("⚡ Análise de Subestações")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("🔄 Atualizar Detecção", use_container_width=True):
            atualizar_subestacoes_detectadas(client, distribuidora)
    
    with col2:
        eps_km = st.number_input("Raio de detecção (km)", min_value=1.0, max_value=20.0, value=5.0, step=0.5)
    
    with col3:
        limite = st.number_input("Limite de registros", min_value=10, max_value=500, value=100, step=10)
    
    # Abas para diferentes visualizações
    tab_ons, tab_detectadas, tab_geo = st.tabs(["🏢 ONS (Oficial)", "🔍 Detectadas (Clustering)", "🗺️ Mapa"])
    
    # Aba ONS
    with tab_ons:
        render_tab_subestacoes_ons(client, distribuidora, limite)
    
    # Aba Detectadas
    with tab_detectadas:
        render_tab_subestacoes_detectadas(client, distribuidora, limite)
    
    # Aba Mapa
    with tab_geo:
        render_tab_mapa_subestacoes(client)
    
    # Resumo
    render_resumo_subestacoes(client)


def render_tab_subestacoes_ons(client: ApiClient, distribuidora: str | None, limite: int):
    """
    Exibe tabela de subestações oficiais do ONS.
    """
    result = client.get("/subestacoes/ons", params={"distribuidora": distribuidora, "limite": limite})
    
    if result.error:
        st.error(f"Erro ao buscar subestações ONS: {result.error}")
        return
    
    if not result.data:
        st.info("Nenhuma subestação ONS encontrada.")
        return
    
    df = pd.DataFrame(result.data)
    
    # Reformatar colunas
    df_display = df[["nome", "sigla_se", "tensao_kv", "subsistema", "distribuidora"]].copy()
    df_display.columns = ["Nome", "Sigla", "Tensão (kV)", "Subsistema", "Distribuidora"]
    
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    
    # Estatísticas
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total de SEs", len(df))
    
    with col2:
        tensoes_unicas = df["tensao_kv"].nunique()
        st.metric("Níveis de Tensão", tensoes_unicas)
    
    with col3:
        distribuidoras = df["distribuidora"].nunique()
        st.metric("Distribuidoras", distribuidoras)


def render_tab_subestacoes_detectadas(client: ApiClient, distribuidora: str | None, limite: int):
    """
    Exibe subestações detectadas automaticamente.
    """
    result = client.get("/subestacoes/detectadas", params={"distribuidora": distribuidora, "limite": limite})
    
    if result.error:
        st.error(f"Erro ao buscar subestações detectadas: {result.error}")
        return
    
    if not result.data:
        st.info("Nenhuma subestação detectada. Clique em 'Atualizar Detecção' acima.")
        return
    
    df = pd.DataFrame(result.data)
    
    # Reformatar
    df_display = df[[
        "nome", "cluster_id", "quantidade_gd", "potencia_total_mw", 
        "raio_deteccao_km", "subsistema", "distribuidora"
    ]].copy()
    
    df_display.columns = [
        "Nome", "Cluster", "MMGD Count", "Potência (MW)", 
        "Raio (km)", "Subsistema", "Distribuidora"
    ]
    
    df_display["Potência (MW)"] = df_display["Potência (MW)"].apply(lambda x: f"{x:.1f}")
    
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        # Potência por subsistema
        df_subsistema = df.groupby("subsistema")["potencia_total_mw"].sum().sort_values(ascending=False)
        st.bar_chart(df_subsistema, use_container_width=True)
        st.caption("Potência por Subsistema")
    
    with col2:
        # Distribuição por subsistema
        df_count = df.groupby("subsistema").size().sort_values(ascending=False)
        st.bar_chart(df_count, use_container_width=True)
        st.caption("Quantidade de SEs Detectadas")


def render_tab_mapa_subestacoes(client: ApiClient):
    """
    Exibe mapa com subestações (ONS e detectadas).
    """
    result = client.get("/subestacoes/geo", params={"origem": "ambas"})
    
    if result.error:
        st.error(f"Erro ao buscar dados de mapa: {result.error}")
        return
    
    if not result.data or not result.data.get("features"):
        st.info("Nenhuma subestação para exibir no mapa.")
        return
    
    # Preparar dados para mapa
    features = result.data.get("features", [])
    
    if not features:
        st.info("Nenhuma subestação para exibir.")
        return
    
    # Separar ONS e detectadas
    ons_points = []
    detectadas_points = []
    
    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [0, 0])
        
        point = {
            "lat": coords[1],
            "lon": coords[0],
            "nome": props.get("nome", "Unknown"),
            "tipo": props.get("tipo", "unknown")
        }
        
        if props.get("tipo") == "subestacao_ons":
            ons_points.append(point)
        else:
            detectadas_points.append(point)
    
    # Usar st.map (simplificado) ou st_folium se disponível
    if ons_points or detectadas_points:
        # Combinar dados
        all_points = []
        for p in ons_points:
            all_points.append({"latitude": p["lat"], "longitude": p["lon"], "name": f"ONS: {p['nome']}"})
        for p in detectadas_points:
            all_points.append({"latitude": p["lat"], "longitude": p["lon"], "name": f"Detectada: {p['nome']}"})
        
        if all_points:
            map_df = pd.DataFrame(all_points)
            st.map(map_df, zoom=3)


def render_resumo_subestacoes(client: ApiClient):
    """
    Exibe resumo geral de subestações.
    """
    with st.expander("📊 Resumo por Distribuidora"):
        result = client.get("/subestacoes/resumo")
        
        if result.error:
            st.error(f"Erro ao buscar resumo: {result.error}")
            return
        
        if not result.data:
            st.info("Nenhum dado disponível.")
            return
        
        df_resumo = pd.DataFrame(result.data)
        df_resumo = df_resumo.sort_values("total", ascending=False)
        
        st.dataframe(df_resumo, use_container_width=True, hide_index=True)


def atualizar_subestacoes_detectadas(client: ApiClient, distribuidora: str | None):
    """
    Executa atualização de subestações detectadas via clustering.
    """
    with st.spinner("🔄 Processando clustering geoespacial..."):
        eps_km = st.session_state.get("eps_km", 5.0)
        
        result = client.post(
            "/subestacoes/detectadas/atualizar",
            params={"distribuidora": distribuidora, "eps_km": eps_km}
        )
        
        if result.error:
            st.error(f"Erro: {result.error}")
        else:
            data = result.data or {}
            quantidade = data.get("quantidade", 0)
            st.success(f"✅ {data.get('mensagem', 'Atualizado com sucesso')} ({quantidade} registros)")
