from typing import Any, Dict, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from services.api_client import ApiClient
from utils.errors import show_error
from utils.formatters import (
    format_mw,
    format_kw,
    format_wm2,
    format_integer,
    format_percentage,
    format_factor,
    apply_plotly_locale
)


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
    if not df.empty:
        # Converter 'hora' para datetime
        if "hora" in df.columns:
            df["hora"] = pd.to_datetime(df["hora"], format="mixed", errors="coerce")
        
        # Sanitizar colunas numéricas (remover valores inválidos)
        numeric_cols = ["carga_ons", "estimativa_solar_mw", "consumo_estimado_mw", "carga_real_estimada", "sol_wm2", "sol_wm2_final", "percentual_total"]
        for col in numeric_cols:
            if col in df.columns:
                # Converter para float, colocando NaN para valores inválidos
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Substituir NaN por 0
                df[col] = df[col].fillna(0.0)
                # Garantir tipo float64 explicitamente para Arrow compatibility
                df[col] = df[col].astype('float64')
        
        # Remover linhas com todos os valores numéricos como 0 (dados inválidos)
        numeric_check = df[numeric_cols].sum(axis=1)
        df = df[numeric_check > 0].reset_index(drop=True)
        
        # Buscar dados de carga por classe (agregado diário)
        try:
            params_classe = {"distribuidora": distribuidora} if distribuidora else {}
            result_classe = client.get("/analise/carga-por-classe", params=params_classe)
            if not result_classe.error and result_classe.data:
                df_classe = pd.DataFrame(result_classe.data)
                if not df_classe.empty:
                    # Armazenar dados de classe como atributo do DataFrame
                    df.attrs["carga_por_classe"] = df_classe
        except Exception as e:
            # Se falhar ao buscar dados de classe, continuar sem eles
            import logging
            logging.warning(f"Aviso: Não foi possível buscar carga por classe: {e}")
        
        # Buscar dados de carga distribuidora horária (série temporal real)
        try:
            params_dist = {"distribuidora": distribuidora} if distribuidora else {}
            result_dist = client.get("/analise/carga-distribuidora-horaria", params=params_dist)
            if not result_dist.error and result_dist.data:
                df_dist = pd.DataFrame(result_dist.data)
                if not df_dist.empty:
                    # Converter data_medicao para datetime
                    if "data_medicao" in df_dist.columns:
                        df_dist["data_medicao"] = pd.to_datetime(
                            df_dist["data_medicao"],
                            format="mixed",
                            errors="coerce"
                        )
                    # Sanitizar colunas numéricas
                    numeric_cols = ["carga_liquida_mw", "carga_estimada_total_mw"]
                    for col in numeric_cols:
                        if col in df_dist.columns:
                            df_dist[col] = pd.to_numeric(df_dist[col], errors="coerce").fillna(0.0).astype('float64')
                    # Garantir que distribuidora é string
                    if "distribuidora" in df_dist.columns:
                        df_dist["distribuidora"] = df_dist["distribuidora"].astype(str)
                    # Armazenar dados de distribuidora como atributo do DataFrame
                    df.attrs["carga_distribuidora_horaria"] = df_dist
        except Exception as e:
            # Se falhar ao buscar dados de distribuidora, continuar sem eles
            import logging
            logging.warning(f"Aviso: Não foi possível buscar carga distribuidora horária: {e}")
        
        # Buscar dados de carga ONS em tempo real
        try:
            result_ons = client.get("/analise/carga-ons-realtime", params={"limite": 288})
            if not result_ons.error and result_ons.data:
                df_ons = pd.DataFrame(result_ons.data)
                if not df_ons.empty:
                    # Converter data_medicao para datetime
                    if "data_medicao" in df_ons.columns:
                        df_ons["data_medicao"] = pd.to_datetime(
                            df_ons["data_medicao"],
                            format="mixed",
                            errors="coerce"
                        )
                    # Sanitizar colunas numéricas
                    numeric_cols = ["carga_mw", "percentual"]
                    for col in numeric_cols:
                        if col in df_ons.columns:
                            df_ons[col] = pd.to_numeric(df_ons[col], errors="coerce").fillna(0.0).astype(float)
                    # Garantir que subsistema e distribuidora são strings
                    for str_col in ["subsistema", "distribuidora"]:
                        if str_col in df_ons.columns:
                            df_ons[str_col] = df_ons[str_col].fillna("").astype(str)
                    # Armazenar dados do ONS como atributo do DataFrame
                    df.attrs["carga_ons_realtime"] = df_ons
        except Exception as e:
            # Se falhar ao buscar dados do ONS, continuar sem eles
            import logging
            logging.warning(f"Aviso: Não foi possível buscar carga ONS em tempo real: {e}")
        
    
    return df


def render_carga_section(
    df_carga: pd.DataFrame,
    impacto_projecao_mw: float,
    multiplicador: int,
    subsistema: str,
    distribuidora: str | None = None,
    client: ApiClient = None,
) -> None:
    """
    Renderiza gráfico comparativo de cargas mostrando a separação entre:
    - Carga Proporcional: Calculada com base em dados ANEEL (transformadores)
    - Geração MMGD: Produção solar/eólica distribuída
    - Consumo Real: Demanda total = Carga Proporcional + MMGD

    Args:
        df_carga: DataFrame com dados de carga (colunas: hora, carga_ons, estimativa_solar_mw, carga_real_estimada)
        impacto_projecao_mw: Impacto estimado de MMGD nao mapeada (opcional)
        multiplicador: Fator multiplicador para projeção de MMGD nao mapeada
        subsistema: Nome do subsistema elétrico
        distribuidora: Nome da distribuidora (opcional)
        client: Cliente API para buscar dados adicionais (opcional)
    """
    st.header(f"Análise Diária de Carga: Líquida vs Real")

    if df_carga.empty:
        st.info("Sem dados de carga para o período selecionado.")
        return
    
    # Validar colunas obrigatórias
    required_cols = ["hora", "carga_ons", "estimativa_solar_mw", "carga_real_estimada"]
    missing_cols = [col for col in required_cols if col not in df_carga.columns]
    if missing_cols:
        st.error(f"Colunas faltando no DataFrame: {missing_cols}")
        return
    
    # Validar que temos dados válidos (não apenas zeros)
    if df_carga[["carga_ons", "estimativa_solar_mw", "carga_real_estimada"]].sum().sum() == 0:
        st.warning("Dados de carga contêm apenas valores zeros. Verifique a API.")
        return

    # Calcular carga auditada se houver projeção de fraude
    if impacto_projecao_mw > 0:
        df_carga["carga_auditada"] = (df_carga["carga_real_estimada"] + impacto_projecao_mw).astype('float64')

    # ===== MÉTRICAS PRINCIPAIS =====
    col1, col2, col3, col4 = st.columns(4)

    # Valores atuais (última leitura)
    carga_liquida_atual = df_carga.iloc[-1]["carga_ons"]
    geracao_mmgd_atual = df_carga.iloc[-1]["estimativa_solar_mw"]
    consumo_real_atual = df_carga.iloc[-1]["carga_real_estimada"]

    # Pico solar às 12h (meio-dia)
    df_carga_12h = df_carga[df_carga["hora"].dt.hour == 12]
    if not df_carga_12h.empty:
        pico_solar_dia = df_carga_12h["estimativa_solar_mw"].values[0]
    else:
        # Fallback: usar o máximo do dia se não houver dado às 12h
        pico_solar_dia = df_carga["estimativa_solar_mw"].max()
    hora_pico = "12h"

    col1.metric(
        "⚡ Carga Proporcional",
        format_mw(carga_liquida_atual, decimals=0),
        help="Carga calculada proporcionalmente baseada em transformadores ANEEL"
    )
    
    # Buscar dados reais de painéis detectados
    try:
        if distribuidora and client:
            result_paineis = client.get("/analise/mmgd-detectada-paineis", params={"distribuidora": distribuidora})
            if not result_paineis.error and result_paineis.data:
                data_paineis = result_paineis.data
                potencia_mw = data_paineis.get("potencia_detectada_mw", 0.0)
                paineis_count = data_paineis.get("paineis_detectados", 0)
                confianca = data_paineis.get("confianca_media", 0.0)
                
                # Formatar delta text
                if paineis_count > 0:
                    delta_text = f"{paineis_count:,} painéis (conf: {int(confianca * 100)}%)"
                
                col2.metric(
                    "☀️ MMGD Detectada",
                    format_mw(potencia_mw, decimals=1),
                    delta=delta_text,
                    help="Potência de painéis solares detectados por IA"
                )
            else:
                col2.metric(
                    "☀️ MMGD Detectada",
                    "0.0 MW",
                    help="Potência de painéis solares detectados por IA"
                )
        else:
            # Sem distribuidora selecionada
            col2.metric(
                "☀️ MMGD Detectada",
                "0.0 MW",
                help="Potência de painéis solares detectados por IA"
            )
    except Exception as e:
        # Fallback: mostrar 0 com log do erro
        import logging
        logging.error(f"Erro ao buscar MMGD detectada: {e}")
        col2.metric(
            "☀️ MMGD Detectada",
            "0.0 MW",
            help="Potência de painéis solares detectados por IA"
        )
    
    # Calcular potência instalada estimada com base no pico solar
    # Assumindo que o pico solar representa ~85% da capacidade instalada em condições ideais
    potencia_instalada_estimada = pico_solar_dia / 0.85 if pico_solar_dia > 0 else 0
    
    col3.metric(
        "⚡ Capacidade MMGD (Estimada)",
        format_mw(potencia_instalada_estimada, decimals=0),
        help="Capacidade instalada estimada de MMGD baseada no pico solar do dia"
    )
    col4.metric(
        f"☀️ Pico Solar (às {hora_pico})",
        format_mw(pico_solar_dia, decimals=0),
        help="Máxima geração MMGD no dia"
    )

    # ===== TOOLTIP EDUCATIVO =====
    with st.expander("ℹ️ Entenda os conceitos"):
        st.markdown("""
        ### Conceitos Fundamentais

        #### 🔵 Carga Proporcional
        - **O que é**: Carga calculada proporcionalmente baseada em dados ANEEL
        - **Fonte**: Dados de transformadores e consumo granular ANEEL (atualização anual)
        - **Método**: Distribuição proporcional baseada em número de transformadores
        - **O que NÃO é**: Não é medição direta do ONS

        #### 🟡 Geração MMGD (Micro e Minigeração Distribuída)
        - **O que é**: Painéis solares e pequenas usinas no ponto de consumo
        - **Capacidade**: Até 5 MW (mini) ou até 75 kW (micro)
        - **Características**: Geram energia consumida localmente, não passa pela transmissão
        - **Impacto**: Durante o dia, reduzem a carga vista pelo ONS

        #### 🟢 Consumo Real (Estimado)
        - **O que é**: Demanda TOTAL de energia pelos consumidores
        - **Fórmula**: Consumo Real = Carga Proporcional + Geração MMGD
        - **Inclui**: Energia da rede + energia da MMGD local
        - **Relevância**: Representa a demanda real que precisa ser atendida

        #### 📊 "Carga Oculta"
        - **O que é**: Diferença entre consumo real e carga líquida
        - **Valor**: Igual à geração MMGD
        - **Por que "oculta"**: Está "escondida" do ONS (não passa pela transmissão)

        ---

        **Exemplo prático às 12h (pico solar):**
        - Consumo Real: **100 MW** (o que os consumidores realmente usam)
        - Geração MMGD: **30 MW** (painéis solares gerando)
        - Carga Proporcional: **70 MW** (100 - 30 = estimativa baseada em ANEEL)

        O cálculo proporcional estima 70 MW, mas o consumo real é 100 MW.
        """)

    # ===== GRÁFICO 1: CARGA PROPORCIONAL VS CONSUMO REAL (LINHAS SIMPLES) =====
    fig1 = go.Figure()

    # Trace 1: Carga Proporcional
    fig1.add_trace(
        go.Scatter(
            x=df_carga["hora"],
            y=df_carga["carga_ons"],
            mode="lines",
            name="Carga Proporcional",
            line={"color": "#1e3a8a", "width": 3},
            hovertemplate=(
                "<b>Carga Proporcional</b><br>"
                "Hora: %{x}<br>"
                "Carga: %{y:.0f} MW<br>"
                "<extra></extra>"
            )
        )
    )

    # Trace 2: Consumo Real (linha superior)
    fig1.add_trace(
        go.Scatter(
            x=df_carga["hora"],
            y=df_carga["carga_real_estimada"],
            mode="lines",
            name="Consumo Real (Estimado)",
            line={"color": "#16a34a", "width": 3},
            hovertemplate=(
                "<b>Consumo Real</b><br>"
                "Hora: %{x}<br>"
                "Consumo: %{y:.0f} MW<br>"
                "<extra></extra>"
            )
        )
    )

    # Trace 3 (opcional): Carga Auditada com fraudes
    if impacto_projecao_mw > 0 and "carga_auditada" in df_carga.columns:
        fig1.add_trace(
            go.Scatter(
                x=df_carga["hora"],
                y=df_carga["carga_auditada"],
                mode="lines",
                name=f"Cenário com Fraudes ({multiplicador}x)",
                line={"color": "#dc2626", "width": 2, "dash": "dash"},
                hovertemplate=(
                    "<b>Cenário com Fraudes</b><br>"
                    "Hora: %{x}<br>"
                    "Carga: %{y:.0f} MW<br>"
                    "<extra></extra>"
                )
            )
        )

    fig1.update_layout(
        title={"text": "Comparativo: Carga Proporcional vs Consumo Real", "x": 0.5, "xanchor": "center"},
        xaxis_title="Data/Hora",
        yaxis_title="Potência Média (MW)",
        template="plotly_dark",
        hovermode="x unified",
        height=400,
        showlegend=True,
        legend={"orientation": "h", "yanchor": "bottom", "y": -0.2, "xanchor": "center", "x": 0.5},
        separators=",.",
        yaxis={"ticksuffix": " MW", "rangemode": "tozero"}
    )

    apply_plotly_locale(fig1)
    st.plotly_chart(fig1, use_container_width=True)

    # ===== GRÁFICO 2: DISTRIBUIÇÃO POR CLASSE (BARRAS EMPILHADAS) =====
    
    # ===== GRÁFICO 2: SÉRIE TEMPORAL - CARGA DISTRIBUIDORA REAL =====
    if "carga_distribuidora_horaria" in df_carga.attrs:
        df_dist = df_carga.attrs["carga_distribuidora_horaria"]
        if not df_dist.empty:
            df_dist_sorted = df_dist.sort_values("data_medicao")
            
            fig2 = go.Figure()
            
            # Linha de carga líquida (medida real)
            fig2.add_trace(
                go.Scatter(
                    x=df_dist_sorted["data_medicao"],
                    y=df_dist_sorted["carga_liquida_mw"],
                    mode="lines",
                    name="Carga Líquida (Real)",
                    line=dict(color="#3b82f6", width=2),
                    hovertemplate=(
                        "<b>Carga Líquida</b><br>"
                        "Hora: %{x|%d/%m %H:%M}<br>"
                        "Carga: %{y:.2f} MW<br>"
                        "<extra></extra>"
                    )
                )
            )
            
            # Linha de carga estimada total (com MMGD)
            fig2.add_trace(
                go.Scatter(
                    x=df_dist_sorted["data_medicao"],
                    y=df_dist_sorted["carga_estimada_total_mw"],
                    mode="lines",
                    name="Carga Total Estimada",
                    line=dict(color="#10b981", width=2, dash="dash"),
                    hovertemplate=(
                        "<b>Carga Total Estimada</b><br>"
                        "Hora: %{x|%d/%m %H:%M}<br>"
                        "Carga: %{y:.2f} MW<br>"
                        "<extra></extra>"
                    )
                )
            )
            
            # Adicionar dados do ONS se disponíveis
            if "carga_ons_realtime" in df_carga.attrs:
                df_ons = df_carga.attrs["carga_ons_realtime"]
                if not df_ons.empty:
                    # Agrupar por data_medicao para sumarizar todos os subsistemas
                    df_ons_resumo = df_ons.groupby("data_medicao")["carga_mw"].sum().reset_index()
                    df_ons_resumo = df_ons_resumo.sort_values("data_medicao")
                    
                    fig2.add_trace(
                        go.Scatter(
                            x=df_ons_resumo["data_medicao"],
                            y=df_ons_resumo["carga_mw"],
                            mode="lines",
                            name="Carga Total ONS (SIN)",
                            line=dict(color="#f59e0b", width=2, dash="dot"),
                            hovertemplate=(
                                "<b>Carga ONS (SIN)</b><br>"
                                "Hora: %{x|%d/%m %H:%M}<br>"
                                "Carga: %{y:.2f} MW<br>"
                                "<extra></extra>"
                            )
                        )
                    )
            
            fig2.update_layout(
                title={"text": "Série Temporal de Carga: Distribuidora vs ONS (SIN)", "x": 0.5, "xanchor": "center"},
                xaxis_title="Data/Hora",
                yaxis_title="Carga (MW)",
                template="plotly_dark",
                hovermode="x unified",
                height=400,
                showlegend=True,
                legend={"orientation": "h", "yanchor": "bottom", "y": -0.2, "xanchor": "center", "x": 0.5},
                yaxis={"ticksuffix": " MW", "rangemode": "tozero"}
            )
            
            apply_plotly_locale(fig2)
            st.plotly_chart(fig2, use_container_width=True)


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
    
    # Sanitizar tipos de dados
    if "mw" in df_classes.columns:
        df_classes["mw"] = pd.to_numeric(df_classes["mw"], errors="coerce").fillna(0.0).astype(float)
    if "classe" in df_classes.columns:
        df_classes["classe"] = df_classes["classe"].fillna("").astype(str)

    st.markdown("---")
    st.header("Detalhamento da Distribuidora")
    c1, c2 = st.columns([1, 2])
    with c1:
        st.dataframe(df_classes, use_container_width=True, hide_index=True)
    with c2:
        fig_pie = px.pie(df_classes, values="mw", names="classe", hole=0.4, title="Perfil de Consumo")
        fig_pie.update_layout(template="plotly_dark", separators=",.")
        apply_plotly_locale(fig_pie)
        st.plotly_chart(fig_pie, use_container_width=True)


def render_estabelecimentos_section(client: ApiClient, distribuidora: str) -> None:
    """Renderiza análise de estabelecimentos por tipo."""
    st.header("Análise de Estabelecimentos por Tipo")

    result_resumo = client.get("/analise/estabelecimentos/resumo", params={"distribuidora": distribuidora})
    result_contagem = client.get("/analise/estabelecimentos/contagem", params={"distribuidora": distribuidora})

    if result_resumo.error or result_contagem.error:
        show_error(result_resumo.error or result_contagem.error)
        return

    if not result_resumo.data or not result_contagem.data:
        st.info("Sem dados de estabelecimentos disponíveis.")
        return

    resumo = result_resumo.data
    contagens = result_contagem.data

    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Instalações MMGD", format_integer(resumo.get('total_instalacoes', 0)))
    col2.metric("Unidades Consumidoras (UC)", format_integer(resumo.get('total_unidades_consumidoras', 0)))
    col3.metric("Potência Total", format_mw(resumo.get('total_mw', 0), decimals=0))
    total_uc = resumo.get('total_unidades_consumidoras', 1)
    total_mw = resumo.get('total_mw', 0)
    col4.metric("Média por UC", format_kw((total_mw / total_uc * 1000) if total_uc > 0 else 0, decimals=1))

    df_contagens = pd.DataFrame(contagens)
    
    # Sanitizar tipos de dados
    numeric_cols = ["quantidade", "total_unidades", "total_mw"]
    for col in numeric_cols:
        if col in df_contagens.columns:
            df_contagens[col] = pd.to_numeric(df_contagens[col], errors="coerce").fillna(0.0).astype(float)
    if "tipo" in df_contagens.columns:
        df_contagens["tipo"] = df_contagens["tipo"].fillna("").astype(str)

    # Mapear labels
    tipo_labels = {
        "residencia": "Residências",
        "predio_residencial": "Prédios Residenciais",
        "comercio": "Comércios",
        "predio_comercial": "Prédios Comerciais",
        "industria": "Indústrias",
        "outro": "Outros"
    }
    df_contagens["tipo_label"] = df_contagens["tipo"].map(tipo_labels)

    # Gráficos lado a lado
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Distribuição por Quantidade")
        fig_qty = px.pie(
            df_contagens,
            values="quantidade",
            names="tipo_label",
            title="Número de Instalações",
            hole=0.4
        )
        fig_qty.update_layout(template="plotly_dark", separators=",.")
        apply_plotly_locale(fig_qty)
        st.plotly_chart(fig_qty, use_container_width=True)

    with col_right:
        st.subheader("Distribuição por Potência")
        fig_mw = px.pie(
            df_contagens,
            values="total_mw",
            names="tipo_label",
            title="Capacidade Instalada (%)",
            hole=0.4
        )
        fig_mw.update_layout(template="plotly_dark", separators=",.")
        apply_plotly_locale(fig_mw)
        st.plotly_chart(fig_mw, use_container_width=True)

    # Tabela detalhada
    st.subheader("Tabela Detalhada")
    df_display = df_contagens[["tipo_label", "quantidade", "total_unidades", "total_mw"]].copy()
    df_display.columns = ["Tipo", "Instalações", "Unidades Consumidoras (UC)", "Potência (MW)"]
    st.dataframe(df_display, use_container_width=True, hide_index=True)


def render_perfis_carga(client: ApiClient, classes: Optional[list[str]] = None) -> None:
    """
    Renderiza gráfico de perfis de carga típicos por classe de consumo.

    Args:
        client: Cliente da API
        classes: Lista de classes a exibir. Se None, exibe todas.
    """
    st.header("Perfis de Carga por Classe de Consumo")

    # Info explicativa
    with st.expander("ℹ️ O que são perfis de carga?"):
        st.markdown("""
        **Perfis de carga** são curvas horárias que representam o padrão típico de consumo
        de energia ao longo do dia para cada classe de consumidor.

        - **Fatores normalizados**: Os valores são fatores multiplicativos com média = 1.0
        - **Para obter MW/kW**: Multiplique o fator pelo consumo médio da classe
        - **Baseado em estudos**: EPE (Empresa de Pesquisa Energética) e ANEEL

        **Características por classe:**
        - 🏠 **Residencial**: Pico noturno (18h-22h) - retorno do trabalho, jantar, lazer
        - 🏢 **Comercial**: Pico diurno (9h-18h) - horário comercial
        - 🏭 **Industrial**: Perfil mais plano - operação contínua (3 turnos)
        - 🌾 **Rural**: Dois picos - matinal (5h-7h) e vespertino (17h-19h)
        - 🏛️ **Poder Público**: Similar ao comercial com iluminação pública noturna
        """)

    # Seletor de classes
    todas_classes = ["residencial", "comercial", "industrial", "rural", "poder_publico"]
    classes_labels = {
        "residencial": "🏠 Residencial",
        "comercial": "🏢 Comercial",
        "industrial": "🏭 Industrial",
        "rural": "🌾 Rural",
        "poder_publico": "🏛️ Poder Público"
    }

    col1, col2 = st.columns([3, 1])
    with col1:
        if classes is None:
            classes_selecionadas = st.multiselect(
                "Selecione as classes para comparar:",
                options=todas_classes,
                default=["residencial", "comercial", "industrial"],
                format_func=lambda x: classes_labels.get(x, x)
            )
        else:
            classes_selecionadas = classes

    if not classes_selecionadas:
        st.warning("Selecione pelo menos uma classe para visualizar o perfil.")
        return

    # Buscar dados da API
    params = {"classes": ",".join(classes_selecionadas)}
    result = client.get("/analise/perfis-carga", params=params)

    if result.error:
        show_error(result.error)
        return

    if not result.data or not result.data.get("perfis"):
        st.warning("Nenhum perfil encontrado.")
        return

    perfis = result.data["perfis"]

    # Cores por classe
    cores = {
        "residencial": "#1f77b4",  # Azul
        "comercial": "#ff7f0e",    # Laranja
        "industrial": "#2ca02c",   # Verde
        "rural": "#d62728",        # Vermelho
        "poder_publico": "#9467bd" # Roxo
    }

    # Criar gráfico
    fig = go.Figure()

    for perfil in perfis:
        classe = perfil["classe"]
        curva = perfil["curva"]
        horas = list(range(24))

        fig.add_trace(go.Scatter(
            x=horas,
            y=curva,
            mode='lines+markers',
            name=classes_labels.get(classe, classe.title()),
            line=dict(
                color=cores.get(classe, "#888888"),
                width=3
            ),
            marker=dict(size=6),
            hovertemplate=(
                f"<b>{classes_labels.get(classe, classe.title())}</b><br>"
                "Hora: %{x}h<br>"
                "Fator: %{y:.2f} p.u.<br>"
                "<extra></extra>"
            )
        ))

    # Layout do gráfico
    fig.update_layout(
        title="Curvas de Carga Típicas por Classe de Consumo",
        xaxis_title="Hora do Dia",
        yaxis_title="Fator de Carga (p.u.)",
        template="plotly_dark",
        hovermode="x unified",
        height=500,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        separators=",.",
        xaxis=dict(
            tickmode='linear',
            tick0=0,
            dtick=2,
            ticksuffix="h",
            range=[-0.5, 23.5]
        ),
        yaxis=dict(
            range=[0, max([max(p["curva"]) for p in perfis]) * 1.1]
        )
    )

    apply_plotly_locale(fig)
    st.plotly_chart(fig, use_container_width=True)

    # Métricas de cada perfil
    st.subheader("Características dos Perfis")

    cols = st.columns(len(perfis))
    for idx, perfil in enumerate(perfis):
        with cols[idx]:
            classe = perfil["classe"]
            st.markdown(f"**{classes_labels.get(classe, classe.title())}**")
            st.metric("Hora de Pico", f"{perfil['hora_pico']}h")
            st.metric("Fator no Pico", format_factor(perfil['fator_pico']))
            st.metric("Hora de Vale", f"{perfil['hora_vale']}h")
            st.metric("Fator no Vale", format_factor(perfil['fator_vale']))
            st.metric("Amplitude", format_factor(perfil['amplitude']))

    # Tabela de dados
    with st.expander("📊 Ver dados tabulares"):
        for perfil in perfis:
            st.markdown(f"**{classes_labels.get(perfil['classe'], perfil['classe'].title())}**")
            df_perfil = pd.DataFrame({
                "Hora": [f"{h}h" for h in range(24)],
                "Fator": perfil["curva"]
            })
            st.dataframe(df_perfil.T, use_container_width=True)


def render_classes_consumo_compact(client: ApiClient, distribuidora: str) -> None:
    """
    Versão compacta do componente de classes de consumo.
    Ideal para uso em layouts lado a lado (sem header grande).
    """
    result = client.get("/analise/classes-consumo", params={"distribuidora": distribuidora})
    if result.error:
        st.error(f"Erro ao carregar classes: {result.error}")
        return

    if not result.data:
        st.info("Sem dados de classes disponíveis")
        return

    df_classes = pd.DataFrame(result.data)
    if df_classes.empty:
        st.info("Sem dados de classes disponíveis")
        return
    
    # Sanitizar tipos de dados
    if "mw" in df_classes.columns:
        df_classes["mw"] = pd.to_numeric(df_classes["mw"], errors="coerce").fillna(0.0).astype(float)
    if "classe" in df_classes.columns:
        df_classes["classe"] = df_classes["classe"].fillna("").astype(str)

    # Gráfico de pizza (maior destaque)
    fig_pie = px.pie(
        df_classes,
        values="mw",
        names="classe",
        hole=0.4,
        title="Distribuição por Classe"
    )
    fig_pie.update_layout(
        template="plotly_dark",
        separators=",.",
        height=350,
        showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5)
    )
    apply_plotly_locale(fig_pie)
    st.plotly_chart(fig_pie, use_container_width=True)

    # Tabela compacta
    with st.expander("📊 Ver detalhes"):
        st.dataframe(df_classes, use_container_width=True, hide_index=True)


def render_estabelecimentos_compact(client: ApiClient, distribuidora: str) -> None:
    """
    Versão compacta do componente de estabelecimentos.
    Ideal para uso em layouts lado a lado (sem header grande).
    """
    result_resumo = client.get("/analise/estabelecimentos/resumo", params={"distribuidora": distribuidora})
    result_contagem = client.get("/analise/estabelecimentos/contagem", params={"distribuidora": distribuidora})

    if result_resumo.error or result_contagem.error:
        st.error("Erro ao carregar estabelecimentos")
        return

    if not result_resumo.data or not result_contagem.data:
        st.info("Sem dados de estabelecimentos disponíveis")
        return

    resumo = result_resumo.data
    contagens = result_contagem.data

    # Métricas compactas (2x2)
    col1, col2 = st.columns(2)
    col1.metric(
        "Total Instalações",
        format_integer(resumo.get('total_instalacoes', 0)),
        help="Número total de instalações MMGD"
    )
    col2.metric(
        "Potência Total",
        format_mw(resumo.get('total_mw', 0), decimals=0),
        help="Capacidade instalada total"
    )

    df_contagens = pd.DataFrame(contagens)
    
    # Sanitizar tipos de dados
    numeric_cols = ["quantidade", "total_unidades", "total_mw"]
    for col in numeric_cols:
        if col in df_contagens.columns:
            df_contagens[col] = pd.to_numeric(df_contagens[col], errors="coerce").fillna(0.0).astype(float)
    if "tipo" in df_contagens.columns:
        df_contagens["tipo"] = df_contagens["tipo"].fillna("").astype(str)

    # Mapear labels
    tipo_labels = {
        "residencia": "Residências",
        "predio_residencial": "Prédios Resid.",
        "comercio": "Comércios",
        "predio_comercial": "Prédios Comer.",
        "industria": "Indústrias",
        "outro": "Outros"
    }
    df_contagens["tipo_label"] = df_contagens["tipo"].map(tipo_labels)

    # Gráfico de pizza (potência)
    fig_mw = px.pie(
        df_contagens,
        values="total_mw",
        names="tipo_label",
        title="Distribuição por Tipo",
        hole=0.4
    )
    fig_mw.update_layout(
        template="plotly_dark",
        separators=",.",
        height=350,
        showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5)
    )
    apply_plotly_locale(fig_mw)
    st.plotly_chart(fig_mw, use_container_width=True)

    # Tabela detalhada
    with st.expander("📊 Ver detalhes"):
        df_display = df_contagens[["tipo_label", "quantidade", "total_unidades", "total_mw"]].copy()
        df_display.columns = ["Tipo", "Instalações", "UCs", "MW"]
        st.dataframe(df_display, use_container_width=True, hide_index=True)