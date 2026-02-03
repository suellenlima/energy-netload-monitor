"""
Componente para exibir e gerenciar subestações no Streamlit.
"""

import streamlit as st
import pandas as pd
from services.api_client import ApiClient
from utils.formatters import (
    format_mw,
    format_kw,
    format_voltage,
    format_distance,
    format_integer,
    format_percentage,
    format_factor,
    apply_plotly_locale
)


def render_subestacoes_section(client: ApiClient, distribuidora: str | None = None):
    """
    Renderiza seção de subestações com abas para ONS e detectadas.
    """
    st.subheader("⚡ Análise de Subestações")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("🔄 Atualizar Detecção", use_container_width=True, key="btn_atualizar_deteccao"):
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
        st.metric("Total de SEs", format_integer(len(df)))

    with col2:
        tensoes_unicas = df["tensao_kv"].nunique()
        st.metric("Níveis de Tensão", format_integer(tensoes_unicas))

    with col3:
        distribuidoras = df["distribuidora"].nunique()
        st.metric("Distribuidoras", format_integer(distribuidoras))


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

    df_display["Potência (MW)"] = df_display["Potência (MW)"].apply(lambda x: format_mw(x, decimals=1).replace(" MW", ""))
    df_display["Raio (km)"] = df_display["Raio (km)"].apply(lambda x: format_distance(x).replace(" km", ""))
    
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
            st.success(f"✅ {data.get('mensagem', 'Atualizado com sucesso')} ({format_integer(quantidade)} registros)")


def render_analise_local_subestacao(client: ApiClient, distribuidora: str | None = None):
    """
    Renderiza análise local por subestação (FASE 2).

    Permite selecionar uma subestação e ver:
    - Mix de consumidores
    - Curva de carga sintética
    - Comparativos
    """
    import plotly.graph_objects as go

    st.header("🎯 Análise Local por Subestação")

    st.info("""
    **Análise Centrada na Subestação** (FASE 2)

    Abandonamos visões agregadas por distribuidora/região para focar em análises
    locais defensáveis e compatíveis com o escopo do MVP.
    """)

    # Buscar lista de subestações
    params = {}
    if distribuidora:
        params["distribuidora"] = distribuidora

    result = client.get("/subestacoes/detectadas", params={**params, "limite": 500})

    if result.error or not result.data:
        st.warning("Nenhuma subestação disponível. Execute a detecção primeiro.")
        return

    subestacoes = result.data
    df_subs = pd.DataFrame(subestacoes)

    # Seletor de subestação
    col1, col2 = st.columns([2, 1])

    with col1:
        if not df_subs.empty:
            # Criar display com nome + ID
            df_subs["display"] = df_subs.apply(
                lambda row: f"{row.get('nome', 'SE ' + str(row['id']))} (ID: {row['id']})",
                axis=1
            )
            subestacao_selecionada = st.selectbox(
                "Selecione a subestação:",
                options=df_subs["display"].tolist(),
                index=0
            )

            # Extrair ID
            subestacao_id = int(subestacao_selecionada.split("ID: ")[1].split(")")[0])
        else:
            st.warning("Nenhuma subestação encontrada")
            return

    with col2:
        if st.button("🔗 Associar UCs", use_container_width=True, key="btn_associar_ucs"):
            with st.spinner("Associando UCs à subestação..."):
                assoc_result = client.post("/subestacoes/associar-ucs", params={"raio_km": 10.0})
                if assoc_result.error:
                    st.error(f"Erro: {assoc_result.error}")
                else:
                    stats = assoc_result.data
                    st.success(
                        f"✅ {format_integer(stats.get('ucs_associadas', 0))}/{format_integer(stats.get('total_ucs', 0))} UCs "
                        f"({format_percentage(stats.get('percentual_associado', 0)).replace('%', '')}%)"
                    )

    st.divider()

    # Tab de análises
    tab_mix, tab_carga, tab_comparativo = st.tabs([
        "📊 Mix de Consumidores",
        "📈 Carga Sintética",
        "⚖️ Comparativos"
    ])

    # Tab: Mix de Consumidores
    with tab_mix:
        st.subheader("Mix de Unidades Consumidoras")

        mix_result = client.get(f"/subestacoes/{subestacao_id}/mix-consumidores")

        if mix_result.error:
            st.error(f"Erro: {mix_result.error}")
        elif not mix_result.data or not mix_result.data.get("mix"):
            st.warning("Nenhuma UC associada a esta subestação. Execute 'Associar UCs' primeiro.")
        else:
            mix_data = mix_result.data
            mix = mix_data.get("mix", {})
            totais = mix_data.get("totais", {})

            # Métricas totais
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric("Total de Instalações", format_integer(totais.get('qtd_instalacoes', 0)))
            with col_b:
                st.metric("Total de UCs", format_integer(totais.get('qtd_unidades_consumidoras', 0)))
            with col_c:
                st.metric("Potência Total", format_mw(totais.get('potencia_total_mw', 0), decimals=2))

            # Gráfico de pizza
            if mix:
                labels = list(mix.keys())
                values = [mix[classe]["potencia_total_mw"] for classe in labels]

                fig = go.Figure(data=[go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.4,
                    textinfo='label+percent'
                )])

                fig.update_layout(
                    title="Distribuição de Potência por Classe",
                    template="plotly_dark",
                    separators=",."
                )

                apply_plotly_locale(fig)
                st.plotly_chart(fig, use_container_width=True)

                # Tabela detalhada
                st.subheader("Detalhamento por Classe")
                for classe, dados in mix.items():
                    with st.expander(f"📌 {classe}"):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Instalações", format_integer(dados['qtd_instalacoes']))
                        with col2:
                            st.metric("UCs", format_integer(dados['qtd_unidades_consumidoras']))
                        with col3:
                            st.metric("Potência", format_mw(dados['potencia_total_mw'], decimals=2))

                        # Por tipo
                        if "por_tipo" in dados:
                            st.write("**Por tipo de estabelecimento:**")
                            df_tipo = pd.DataFrame(dados["por_tipo"]).T
                            st.dataframe(df_tipo, use_container_width=True)

    # Tab: Carga Sintética
    with tab_carga:
        st.subheader("Curva de Carga Sintética (24h)")

        carga_result = client.get(f"/subestacoes/{subestacao_id}/carga-sintetica")

        if carga_result.error:
            st.error(f"Erro: {carga_result.error}")
        elif "erro" in carga_result.data:
            st.warning(carga_result.data.get("erro", "Erro desconhecido"))
        else:
            carga_data = carga_result.data
            curva_kw = carga_data.get("curva_horaria_kw", [])
            stats = carga_data.get("estatisticas", {})

            # Métricas
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Pico", format_kw(stats.get('pico_kw', 0), decimals=1),
                         f"Hora: {stats.get('hora_pico', 0)}h")
            with col2:
                st.metric("Média", format_kw(stats.get('media_kw', 0), decimals=1))
            with col3:
                st.metric("Vale", format_kw(stats.get('vale_kw', 0), decimals=1),
                         f"Hora: {stats.get('hora_vale', 0)}h")
            with col4:
                st.metric("Fator de Carga", format_factor(stats.get('fator_carga', 0), decimals=2))

            # Gráfico da curva
            if curva_kw:
                horas = list(range(24))

                fig = go.Figure()

                fig.add_trace(go.Scatter(
                    x=horas,
                    y=curva_kw,
                    mode='lines+markers',
                    name='Carga Sintética',
                    line=dict(color='#1f77b4', width=3),
                    fill='tozeroy'
                ))

                # Marcar pico e vale
                fig.add_trace(go.Scatter(
                    x=[stats.get('hora_pico', 0)],
                    y=[stats.get('pico_kw', 0)],
                    mode='markers',
                    name='Pico',
                    marker=dict(size=15, color='red', symbol='star')
                ))

                fig.add_trace(go.Scatter(
                    x=[stats.get('hora_vale', 0)],
                    y=[stats.get('vale_kw', 0)],
                    mode='markers',
                    name='Vale',
                    marker=dict(size=15, color='green', symbol='diamond')
                ))

                fig.update_layout(
                    title=f"Curva de Carga Sintética - Subestação {subestacao_id}",
                    xaxis_title="Hora do Dia",
                    yaxis_title="Potência (kW)",
                    template="plotly_dark",
                    hovermode="x unified",
                    height=450,
                    separators=",."
                )

                fig.update_xaxes(
                    tickmode='linear',
                    tick0=0,
                    dtick=2,
                    ticksuffix="h"
                )

                fig.update_yaxes(
                    ticksuffix=" kW"
                )

                apply_plotly_locale(fig)
                st.plotly_chart(fig, use_container_width=True)

            # Contribuição por classe
            if "contribuicao_por_classe" in carga_data:
                st.subheader("Contribuição por Classe")
                contrib = carga_data["contribuicao_por_classe"]

                for classe, dados in contrib.items():
                    with st.expander(f"📊 {classe}"):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("UCs", format_integer(dados['qtd_ucs']))
                        with col2:
                            st.metric("Pico", format_kw(dados['pico_kw'], decimals=1))
                        with col3:
                            st.metric("Média", format_kw(dados['media_kw'], decimals=1))

    # Tab: Comparativos
    with tab_comparativo:
        st.subheader("Comparativos e Validações")
        st.info("🚧 Em desenvolvimento: Comparação com carga ONS e validações cruzadas")
