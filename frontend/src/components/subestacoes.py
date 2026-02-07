"""
Componente para exibir e gerenciar subestações no Streamlit.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
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
    # st.subheader("⚡ Análise de Subestações")
    
    # col1, col2, col3 = st.columns([1, 1, 1])
    
    # with col1:
    #     if st.button("🔄 Atualizar Detecção", use_container_width=True, key="btn_atualizar_deteccao"):
    #         atualizar_subestacoes_detectadas(client, distribuidora)
    
    # with col2:
    #     eps_km = st.number_input("Raio de detecção (km)", min_value=1.0, max_value=20.0, value=5.0, step=0.5)
    
    # with col3:
    #     limite = st.number_input("Limite de registros", min_value=10, max_value=500, value=100, step=10)
    
    # Abas para diferentes visualizações
    # tab_ons, tab_detectadas, tab_geo = st.tabs(["🏢 ONS (Oficial)", "🔍 Detectadas (Clustering)", "🗺️ Mapa"])
    tab_ons, tab_geo = st.tabs(["🏢 Visão Geral", "🗺️ Mapa"])
    
    # Aba ONS
    with tab_ons:
        # render_tab_subestacoes_ons(client, distribuidora, limite)
        render_analise_local_subestacao(client, distribuidora)
    
    # Aba Detectadas
    # with tab_detectadas:
    #     render_tab_subestacoes_detectadas(client, distribuidora, limite)
    
    # Aba Mapa
    with tab_geo:
        render_tab_mapa_subestacoes(client)
    
    # Resumo
    # render_resumo_subestacoes(client)


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
    
    # Sanitizar tipos de dados
    if "tensao_kv" in df.columns:
        df["tensao_kv"] = pd.to_numeric(df["tensao_kv"], errors="coerce").fillna(0.0).astype(float)
    for str_col in ["nome", "sigla_se", "subsistema", "distribuidora"]:
        if str_col in df.columns:
            df[str_col] = df[str_col].fillna("").astype(str)
    
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
    
    # Sanitizar tipos de dados
    numeric_cols = ["cluster_id", "quantidade_gd", "potencia_total_mw", "raio_deteccao_km"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)
    for str_col in ["nome", "subsistema", "distribuidora"]:
        if str_col in df.columns:
            df[str_col] = df[str_col].fillna("").astype(str)
    
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
        
        # Sanitizar tipos de dados
        if "total" in df_resumo.columns:
            df_resumo["total"] = pd.to_numeric(df_resumo["total"], errors="coerce").fillna(0).astype(int)
        if "distribuidora" in df_resumo.columns:
            df_resumo["distribuidora"] = df_resumo["distribuidora"].fillna("").astype(str)
        
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

    # st.info("""
    # **Análise Centrada na Subestação** (FASE 2)

    # Abandonamos visões agregadas por distribuidora/região para focar em análises
    # locais defensáveis e compatíveis com o escopo do MVP.
    # """)

    # Buscar lista de subestações
    params = {}
    if distribuidora:
        params["distribuidora"] = distribuidora

    result = client.get("/subestacoes/ons", params={**params, "limite": 500})

    if result.error:
        st.error(f"Erro ao buscar subestações: {result.error}")
        return
        
    if not result.data:
        if distribuidora:
            st.warning(f"⚠️ Nenhuma subestação encontrada para a distribuidora **'{distribuidora}'**")
            st.info("💡 **Dica**: O sistema busca por partes do nome (ex: 'LIGHT' encontra 'LIGHT SESA')")
            
            # Sugerir buscar todas
            if st.button("🔍 Mostrar todas as subestações disponíveis"):
                result_all = client.get("/subestacoes/ons", params={"limite": 20})
                if result_all.data:
                    st.write("**Primeiras 20 subestações cadastradas:**")
                    df_sample = pd.DataFrame(result_all.data)
                    st.dataframe(df_sample[["nome", "distribuidora", "tensao_kv"]].head(20), use_container_width=True)
        else:
            st.warning("Nenhuma subestação cadastrada no sistema.")
        return

    subestacoes = result.data
    df_subs = pd.DataFrame(subestacoes)

    # Debug info
    with st.expander(f"ℹ️ Debug: {len(subestacoes)} subestações encontradas"):
        st.write(f"**Filtro aplicado**: {distribuidora or 'Sem filtro'}")
        st.write(f"**Total de registros**: {len(subestacoes)}")
        if distribuidora and len(subestacoes) > 0:
            distribuidoras_unicas = df_subs["distribuidora"].unique()
            st.write(f"**Distribuidoras encontradas**: {', '.join(distribuidoras_unicas[:5])}")

    # Seletor de subestação
    if not df_subs.empty:
        # Criar display com nome + ID
        df_subs["display"] = df_subs.apply(
            lambda row: f"{row.get('nome', 'SE ' + str(row['id']))} (ID: {row['id']}) - {row.get('tensao_kv', 0):.0f} kV - {row.get('distribuidora', 'N/A')}",
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

    # st.divider()

    # Tab de análises
    tab_visao, tab_carga, tab_geo, tab_paineis = st.tabs([
        "📊 Visão Geral",
        "📈 Curva de Carga",
        "🗺️ Mapa",
        "☀️ Painéis Solares"
    ])

    # Tab: Visão Geral da Subestação (replicando a visão do distribuidor)
    with tab_visao:
        st.subheader("📊 Visão Geral da Subestação")
        
        # Buscar dados consolidados
        visao_result = client.get(f"/subestacoes/{subestacao_id}/visao-geral")
        
        if visao_result.error:
            st.error(f"Erro: {visao_result.error}")
        elif not visao_result.data:
            st.warning("Nenhum dado disponível para esta subestação.")
        else:
            visao_data = visao_result.data
            subestacao_info = visao_data.get("subestacao", {})
            carga_info = visao_data.get("carga", {})
            mmgd_info = visao_data.get("mmgd", {})
            consumidores_info = visao_data.get("consumidores", {})
            
            # Header com informações da subestação
            st.markdown(f"### {subestacao_info.get('nome', 'Subestação')} - {subestacao_info.get('distribuidora', 'N/A')}")
            
            # ===== KPIs PRINCIPAIS (similar à Visão Geral do distribuidor) =====
            st.markdown("#### 📊 Indicadores Principais")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                carga_pico = carga_info.get("pico_mw", 0)
                hora_pico = carga_info.get("hora_pico", 0)
                st.metric(
                    "⚡ Carga de Pico",
                    format_mw(carga_pico, decimals=1),
                    delta=f"Hora: {hora_pico}h",
                    help="Máxima demanda estimada na subestação"
                )
            
            with col2:
                mmgd_detectada = mmgd_info.get("potencia_detectada_mw", 0)
                paineis_count = mmgd_info.get("paineis_count", 0)
                confianca = mmgd_info.get("confianca_media", 0)
                
                if paineis_count > 0:
                    delta_text = f"{paineis_count:,} painéis (conf: {int(confianca * 100)}%)"
                else:
                    delta_text = "Aguardando detecção"
                
                st.metric(
                    "☀️ MMGD Detectada",
                    format_mw(mmgd_detectada, decimals=1),
                    delta=delta_text,
                    help="Potência de painéis solares detectados por IA"
                )
            
            with col3:
                # Calcular capacidade estimada baseada no pico
                # Assumindo que 85% da capacidade é utilizada no pico
                capacidade_estimada = carga_pico / 0.85 if carga_pico > 0 else 0
                st.metric(
                    "⚡ Capacidade Instalada",
                    format_mw(capacidade_estimada, decimals=1),
                    help="Capacidade instalada estimada da subestação (baseada no pico de carga / 0.85)"
                )
            
            with col4:
                fator_carga = carga_info.get("fator_carga", 0)
                st.metric(
                    "📊 Fator de Carga",
                    format_factor(fator_carga, decimals=2),
                    help="Relação entre carga média e pico"
                )
            
            st.divider()
            
            # ===== MIX DE CONSUMIDORES =====
            st.markdown("#### 🏘️ Mix de Unidades Consumidoras")
            
            if consumidores_info.get("erro"):
                st.warning(f"⚠️ {consumidores_info.get('erro')}. Execute 'Associar UCs' primeiro.")
                mix = {}  # Definir mix vazio
            elif consumidores_info.get("total_ucs", 0) == 0:
                st.info("Nenhuma Unidade Consumidora associada a esta subestação. Nos dados BDGD atualizados da ANEEL.")
                mix = {}  # Definir mix vazio
            else:
                # Métricas totais
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("Total de Instalações", format_integer(consumidores_info.get('total_instalacoes', 0)))
                with col_b:
                    st.metric("Total de UCs", format_integer(consumidores_info.get('total_ucs', 0)))
                with col_c:
                    st.metric("Potência Total", format_mw(consumidores_info.get('potencia_total_mw', 0), decimals=2))
                
                # Gráfico de pizza
                mix = consumidores_info.get("mix_por_classe", {})
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
            
            st.divider()
            
            # ===== ANÁLISE POR ESTABELECIMENTOS =====
            st.markdown("#### 🏢 Análise por Tipo de Estabelecimento")
            
            if mix and consumidores_info.get("total_ucs", 0) > 0:
                # Agregar dados por tipo de estabelecimento
                estabelecimentos_data = {}
                for classe, dados in mix.items():
                    if "por_tipo" in dados:
                        for tipo, info in dados["por_tipo"].items():
                            if tipo not in estabelecimentos_data:
                                estabelecimentos_data[tipo] = {
                                    "qtd_instalacoes": 0,
                                    "qtd_unidades_consumidoras": 0,
                                    "potencia_total_mw": 0
                                }
                            estabelecimentos_data[tipo]["qtd_instalacoes"] += info.get("qtd_instalacoes", 0)
                            estabelecimentos_data[tipo]["qtd_unidades_consumidoras"] += info.get("qtd_unidades_consumidoras", 0)
                            estabelecimentos_data[tipo]["potencia_total_mw"] += info.get("potencia_total_mw", 0)
                
                if estabelecimentos_data:
                    # Criar DataFrame para visualização
                    df_estab = pd.DataFrame.from_dict(estabelecimentos_data, orient='index')
                    df_estab = df_estab.sort_values('potencia_total_mw', ascending=False)
                    
                    # Top 10 estabelecimentos
                    df_top10 = df_estab.head(10)
                    
                    # Gráfico de barras
                    fig_estab = go.Figure(data=[
                        go.Bar(
                            x=df_top10.index,
                            y=df_top10['potencia_total_mw'],
                            text=df_top10['potencia_total_mw'].apply(lambda x: f"{x:.2f} MW"),
                            textposition='auto',
                            marker_color='lightblue'
                        )
                    ])
                    
                    fig_estab.update_layout(
                        title="Top 10 Estabelecimentos por Potência",
                        xaxis_title="Tipo de Estabelecimento",
                        yaxis_title="Potência (MW)",
                        template="plotly_dark",
                        height=400,
                        separators=",."
                    )
                    
                    apply_plotly_locale(fig_estab)
                    st.plotly_chart(fig_estab, use_container_width=True)
                    
                    # Tabela detalhada
                    with st.expander("📋 Ver todos os estabelecimentos"):
                        df_display = df_estab.copy()
                        df_display.columns = ["Instalações", "UCs", "Potência (MW)"]
                        df_display["Potência (MW)"] = df_display["Potência (MW)"].apply(lambda x: f"{x:.2f}")
                        st.dataframe(df_display, use_container_width=True)
                else:
                    st.info("Nenhum dado de estabelecimentos disponível")
            else:
                st.info("A subestação precisa ter Unidades Consumidoras associadas primeiro para ver análise de estabelecimentos")

    # Tab: Curva de Carga Sintética
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

    # Tab: Mapa
    with tab_geo:
        st.subheader("🗺️ Mapa de Transformadores da Subestação")
        
        # Buscar transformadores
        transf_result = client.get(f"/subestacoes/{subestacao_id}/transformadores")
        
        if transf_result.error:
            st.error(f"Erro ao buscar transformadores: {transf_result.error}")
        elif not transf_result.data or not transf_result.data.get("transformadores"):
            st.warning("Nenhum transformador encontrado para esta subestação.")
            st.info("Os transformadores são obtidos dos dados BDGD da ANEEL.")
        else:
            import folium
            from streamlit_folium import st_folium
            
            transformadores = transf_result.data.get("transformadores", [])
            total = transf_result.data.get("total", 0)
            
            st.info(f"📍 **{total} transformadores** encontrados nesta subestação")
            
            # Filtrar transformadores com coordenadas válidas
            transf_validos = [
                t for t in transformadores 
                if t.get("latitude") and t.get("longitude")
            ]
            
            if not transf_validos:
                st.warning("Nenhum transformador possui coordenadas geográficas válidas.")
            else:
                # Calcular centro do mapa
                lats = [t["latitude"] for t in transf_validos]
                lngs = [t["longitude"] for t in transf_validos]
                center_lat = sum(lats) / len(lats)
                center_lng = sum(lngs) / len(lngs)
                
                # Criar mapa
                m = folium.Map(
                    location=[center_lat, center_lng],
                    zoom_start=13,
                    tiles="OpenStreetMap"
                )
                
                # Adicionar marcadores para cada transformador
                for idx, transf in enumerate(transf_validos):
                    lat = transf["latitude"]
                    lng = transf["longitude"]
                    transf_id = transf.get("id", idx)
                    codigo = transf.get("codigo", f"T-{transf_id}")
                    potencia = transf.get("potencia_kva", 0)
                    
                    # Popup com informações
                    popup_html = f"""
                    <div style="font-family: Arial; font-size: 12px; min-width: 200px;">
                        <h4 style="margin: 0 0 10px 0; color: #1f77b4;">⚡ Transformador</h4>
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr>
                                <td style="padding: 3px; font-weight: bold;">ID:</td>
                                <td style="padding: 3px;">{transf_id}</td>
                            </tr>
                            <tr>
                                <td style="padding: 3px; font-weight: bold;">Código:</td>
                                <td style="padding: 3px;">{codigo}</td>
                            </tr>
                            <tr>
                                <td style="padding: 3px; font-weight: bold;">Potência:</td>
                                <td style="padding: 3px;">{potencia:.1f} kVA</td>
                            </tr>
                            <tr>
                                <td style="padding: 3px; font-weight: bold;">Lat/Lng:</td>
                                <td style="padding: 3px;">{lat:.5f}, {lng:.5f}</td>
                            </tr>
                        </table>
                    </div>
                    """
                    
                    # Cor baseada na potência
                    if potencia > 100:
                        color = 'red'
                    elif potencia > 50:
                        color = 'orange'
                    else:
                        color = 'blue'
                    
                    folium.Marker(
                        location=[lat, lng],
                        popup=folium.Popup(popup_html, max_width=250),
                        tooltip=f"🔌 {codigo} - {potencia:.0f} kVA",
                        icon=folium.Icon(color=color, icon='bolt', prefix='fa')
                    ).add_to(m)
                
                # Renderizar mapa e capturar cliques
                st.caption("💡 **Dica**: Clique nos marcadores para ver detalhes do transformador")
                
                map_data = st_folium(
                    m,
                    width=None,
                    height=500,
                    returned_objects=["last_object_clicked"]
                )
                
                # Mostrar informações do transformador clicado
                if map_data and map_data.get("last_object_clicked"):
                    clicked_lat = map_data["last_object_clicked"]["lat"]
                    clicked_lng = map_data["last_object_clicked"]["lng"]
                    
                    # Encontrar o transformador clicado
                    transf_clicado = None
                    for t in transf_validos:
                        if abs(t["latitude"] - clicked_lat) < 0.00001 and abs(t["longitude"] - clicked_lng) < 0.00001:
                            transf_clicado = t
                            break
                    
                    if transf_clicado:
                        st.divider()
                        st.markdown("### 📌 Transformador Selecionado")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("ID", transf_clicado.get("id", "N/A"))
                        with col2:
                            st.metric("Código", transf_clicado.get("codigo", "N/A"))
                        with col3:
                            st.metric("Potência", f"{transf_clicado.get('potencia_kva', 0):.1f} kVA")
                        with col4:
                            status = transf_clicado.get("status", "Desconhecido")
                            st.metric("Status", status)
                        
                        # Detalhes adicionais em expander
                        with st.expander("🔍 Ver mais detalhes"):
                            st.json(transf_clicado)
                
                # Estatísticas dos transformadores
                st.divider()
                st.markdown("### 📊 Estatísticas dos Transformadores")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    potencia_total = sum(t.get("potencia_kva", 0) for t in transf_validos)
                    st.metric("Potência Total", f"{potencia_total:.1f} kVA")
                
                with col2:
                    potencia_media = potencia_total / len(transf_validos) if transf_validos else 0
                    st.metric("Potência Média", f"{potencia_media:.1f} kVA")
                
                with col3:
                    st.metric("Com Coordenadas", f"{len(transf_validos)}/{total}")

    # Tab: Painéis Solares Detectados
    with tab_paineis:
        st.subheader("☀️ Painéis Solares Detectados por Transformador")
        
        # Buscar transformadores da subestação
        with st.spinner("Carregando painéis solares..."):
            try:
                import requests
                
                # Fazer requisição ao backend para obter dados dos painéis por transformador
                response = requests.get(
                    f"http://127.0.0.1:8000/api/v1/subestacoes/{subestacao_id}/paineis-por-transformador",
                    timeout=30
                )
                
                resultados = []
                if response.status_code == 200:
                    dados = response.json()
                    transformadores_data = dados.get("data", {}).get("transformadores", [])
                    
                    # Converter para formato de tupla esperado
                    for t in transformadores_data:
                        resultados.append((
                            t.get("transformador_id"),
                            t.get("transformador_codigo"),
                            t.get("latitude"),
                            t.get("longitude"),
                            t.get("total_paineis", 0),
                            t.get("area_total_m2", 0),
                            t.get("potencia_total_kw", 0),
                            t.get("confianca_media", 0),
                            t.get("total_telhados", 0)
                        ))
                elif response.status_code == 404:
                    resultados = []
                else:
                    st.error(f"Erro ao buscar dados: Status {response.status_code}")
                    resultados = []
                
                if not resultados:
                    st.info("Nenhum painel solar detectado para transformadores desta subestação ainda.")
                    st.markdown("""
                    ### 🔍 Como detectar painéis solares:
                    
                    1. **Detectar telhados** primeiro:
                       ```bash
                       POST /api/v1/telhados/detectar
                       Body: {"transformador_id": <ID>, "grid_size": 2, "zoom": 20}
                       ```
                    
                    2. **Detectar painéis** nos telhados:
                       ```bash
                       POST /api/v1/paineis-solares/detectar
                       Body: {"transformador_id": <ID>, "confianca_minima": 0.3}
                       ```
                    """)
                else:
                    # Calcular totais
                    total_paineis = sum(r[4] for r in resultados)
                    total_area = sum(r[5] for r in resultados)
                    total_potencia = sum(r[6] for r in resultados)
                    confianca_geral = sum(r[7] * r[4] for r in resultados) / total_paineis if total_paineis > 0 else 0
                    
                    # KPIs principais
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric(
                            "⚡ Potência Total",
                            f"{total_potencia:.2f} kW",
                            help="Potência instalada total dos painéis detectados"
                        )
                    
                    with col2:
                        st.metric(
                            "☀️ Painéis Detectados",
                            format_integer(total_paineis),
                            help="Número total de painéis solares identificados por IA"
                        )
                    
                    with col3:
                        st.metric(
                            "🔌 Transformadores",
                            format_integer(len(resultados)),
                            help="Transformadores com painéis detectados"
                        )
                    
                    with col4:
                        st.metric(
                            "📊 Confiança Média",
                            f"{int(confianca_geral * 100)}%",
                            help="Confiança média das detecções"
                        )
                    
                    st.divider()
                    
                    # Lista de transformadores com painéis
                    st.markdown("### 📋 Transformadores com Painéis Detectados")
                    
                    for idx, row in enumerate(resultados):
                        trafo_id, trafo_codigo, lat, lon, num_paineis, area_m2, potencia_kw, confianca, num_telhados = row
                        
                        with st.expander(f"🔌 Transformador {trafo_codigo} - {num_paineis} painéis ({potencia_kw:.2f} kW)", expanded=(idx == 0)):
                            # Métricas do transformador
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric("ID", trafo_id)
                            with col2:
                                st.metric("Painéis", num_paineis)
                            with col3:
                                st.metric("Área", f"{area_m2:.1f} m²")
                            with col4:
                                st.metric("Confiança", f"{int(confianca * 100)}%")
                            
                            # Buscar detalhes dos painéis deste transformador via API
                            paineis_response = requests.get(
                                f"http://127.0.0.1:8000/api/v1/transformadores/{trafo_id}/paineis-detalhes",
                                timeout=30
                            )
                            
                            paineis = []
                            if paineis_response.status_code == 200:
                                paineis_data = paineis_response.json()
                                paineis_list = paineis_data.get("data", {}).get("paineis", [])
                                
                                # Converter para formato tupla
                                for p in paineis_list:
                                    paineis.append((
                                        p.get("id"),
                                        p.get("telhado_id"),
                                        p.get("area_m2"),
                                        p.get("potencia_w"),
                                        p.get("confianca"),
                                        p.get("telhado_lat"),
                                        p.get("telhado_lon"),
                                        p.get("url_imagem")
                                    ))
                            
                            # Tabela de painéis
                            if paineis:
                                st.markdown("**Top 5 Painéis por Confiança:**")
                                
                                df_paineis = pd.DataFrame(paineis, columns=[
                                    "ID", "Telhado ID", "Área (m²)", "Potência (W)", 
                                    "Confiança", "Lat", "Lon", "URL Imagem"
                                ])
                                
                                df_paineis["Confiança"] = df_paineis["Confiança"].apply(lambda x: f"{int(x * 100)}%")
                                df_paineis["Área (m²)"] = df_paineis["Área (m²)"].apply(lambda x: f"{x:.1f}")
                                df_paineis["Potência (W)"] = df_paineis["Potência (W)"].apply(lambda x: f"{x:.0f}")
                                
                                st.dataframe(
                                    df_paineis[["ID", "Telhado ID", "Área (m²)", "Potência (W)", "Confiança"]],
                                    use_container_width=True,
                                    hide_index=True
                                )
                                
                                # Mostrar imagens dos telhados
                                st.markdown("**📸 Imagens dos Telhados com Painéis:**")
                                
                                # Mostrar até 3 imagens
                                cols = st.columns(min(3, len(paineis)))
                                for i, painel in enumerate(paineis[:3]):
                                    url_imagem = painel[7]
                                    if url_imagem:
                                        with cols[i]:
                                            st.image(
                                                url_imagem,
                                                caption=f"Telhado {painel[1]} - {int(painel[4] * 100)}% conf.",
                                                use_column_width=True
                                            )
                    
                    st.divider()
                    
                    # Gráfico de distribuição
                    st.markdown("### 📊 Distribuição de Painéis por Transformador")
                    
                    import plotly.graph_objects as go
                    
                    # Top 10 transformadores
                    top_10 = resultados[:10]
                    labels = [f"T-{r[1]}" for r in top_10]
                    valores = [r[4] for r in top_10]
                    potencias = [r[6] for r in top_10]
                    
                    fig = go.Figure()
                    
                    fig.add_trace(go.Bar(
                        x=labels,
                        y=valores,
                        name='Quantidade',
                        marker_color='lightgreen',
                        text=valores,
                        textposition='auto',
                        yaxis='y1'
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=labels,
                        y=potencias,
                        name='Potência (kW)',
                        marker_color='orange',
                        mode='lines+markers',
                        yaxis='y2'
                    ))
                    
                    fig.update_layout(
                        title="Top 10 Transformadores - Painéis e Potência",
                        xaxis_title="Transformador",
                        yaxis_title="Número de Painéis",
                        yaxis2=dict(
                            title="Potência (kW)",
                            overlaying='y',
                            side='right'
                        ),
                        template="plotly_dark",
                        height=400,
                        separators=",.",
                        hovermode='x unified'
                    )
                    
                    apply_plotly_locale(fig)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Informações técnicas
                    with st.expander("ℹ️ Informações sobre a Detecção"):
                        st.markdown(f"""
                        ### Como funciona a detecção:
                        
                        1. **Detecção de Telhados**: Modelo YOLO fine-tuned identifica telhados em grid do Google Maps
                        2. **Detecção de Painéis**: Modelo YOLO stage2 refinado detecta painéis nos telhados
                        3. **Resolução**: ~0.6m/pixel (zoom 20) para máxima precisão
                        4. **Cálculo**: Área do painel × 200 W/m² (eficiência padrão)
                        
                        ### Estatísticas da Subestação:
                        - **Total de painéis**: {total_paineis:,}
                        - **Área total**: {total_area:,.1f} m²
                        - **Potência total**: {total_potencia:.2f} kW ({total_potencia/1000:.3f} MW)
                        - **Transformadores com painéis**: {len(resultados)}
                        - **Confiança média**: {int(confianca_geral * 100)}%
                        
                        ### Modelos Utilizados:
                        - **Telhados**: `yolov8n_finetuned/weights/best.pt`
                        - **Painéis**: `stage2_panel_solar_refined/weights/best.pt`
                        """)
                    
            except Exception as e:
                st.error(f"Erro ao carregar painéis: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
                st.info("""
                ### 🔍 Para detectar painéis solares:
                
                Use os endpoints da API:
                1. `POST /api/v1/telhados/detectar` - Detectar telhados
                2. `POST /api/v1/paineis-solares/detectar` - Detectar painéis
                """)


