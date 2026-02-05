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
    if not df.empty and "hora" in df.columns:
        df["hora"] = pd.to_datetime(df["hora"])
    return df


def render_carga_section(
    df_carga: pd.DataFrame,
    impacto_projecao_mw: float,
    multiplicador: int,
    subsistema: str,
) -> None:
    """
    Renderiza gráfico comparativo de cargas mostrando a separação entre:
    - Carga Líquida (ONS): O que o sistema de transmissão "vê"
    - Geração MMGD: Produção solar/eólica distribuída
    - Consumo Real: Demanda total = Carga Líquida + MMGD

    Args:
        df_carga: DataFrame com dados de carga (colunas: hora, carga_ons, estimativa_solar_mw, carga_real_estimada)
        impacto_projecao_mw: Impacto estimado de MMGD nao mapeada (opcional)
        multiplicador: Fator multiplicador para projeção de MMGD nao mapeada
        subsistema: Nome do subsistema elétrico
    """
    st.header(f"Análise de Carga: Líquida vs Real ({subsistema})")

    if df_carga.empty:
        st.info("Sem dados de carga para o período selecionado.")
        return

    # Calcular carga auditada se houver projeção de fraude
    if impacto_projecao_mw > 0:
        df_carga["carga_auditada"] = df_carga["carga_real_estimada"] + impacto_projecao_mw

    # ===== MÉTRICAS PRINCIPAIS =====
    col1, col2, col3, col4 = st.columns(4)

    # Valores atuais (última leitura)
    carga_liquida_atual = df_carga.iloc[-1]["carga_ons"]
    geracao_mmgd_atual = df_carga.iloc[-1]["estimativa_solar_mw"]
    consumo_real_atual = df_carga.iloc[-1]["carga_real_estimada"]

    # Estatísticas diárias
    pico_solar_dia = df_carga["estimativa_solar_mw"].max()
    hora_pico = df_carga.loc[df_carga["estimativa_solar_mw"].idxmax(), "hora"].strftime("%Hh")

    col1.metric(
        "⚡ Carga Líquida (ONS)",
        format_mw(carga_liquida_atual, decimals=0),
        help="Carga medida pelo ONS nos pontos de entrega (transmissão)"
    )
    col2.metric(
        "🏭 Geração MMGD (Agora)",
        format_mw(geracao_mmgd_atual, decimals=0),
        help="Geração distribuída (painéis solares, mini-usinas)"
    )
    col3.metric(
        "🔌 Consumo Real (Estimado)",
        format_mw(consumo_real_atual, decimals=0),
        help="Demanda total = Carga Líquida + MMGD"
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

        #### 🔵 Carga Líquida (ONS)
        - **O que é**: Medida nos pontos de entrega do sistema de transmissão
        - **O que inclui**: Energia fornecida pela rede (usinas + transmissão)
        - **O que NÃO inclui**: Geração distribuída (MMGD) consumida localmente
        - **Quem mede**: Operador Nacional do Sistema (ONS)

        #### 🟡 Geração MMGD (Micro e Minigeração Distribuída)
        - **O que é**: Painéis solares e pequenas usinas no ponto de consumo
        - **Capacidade**: Até 5 MW (mini) ou até 75 kW (micro)
        - **Características**: Geram energia consumida localmente, não passa pela transmissão
        - **Impacto**: Durante o dia, reduzem a carga vista pelo ONS

        #### 🟢 Consumo Real (Estimado)
        - **O que é**: Demanda TOTAL de energia pelos consumidores
        - **Fórmula**: Consumo Real = Carga Líquida ONS + Geração MMGD
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
        - Carga Líquida ONS: **70 MW** (100 - 30 = o que vem da rede)

        O ONS só "vê" 70 MW, mas o consumo real é 100 MW.
        """)

    # ===== GRÁFICO COMPARATIVO =====
    fig = go.Figure()

    # Trace 1: Carga Líquida ONS (azul escuro, área preenchida)
    fig.add_trace(
        go.Scatter(
            x=df_carga["hora"],
            y=df_carga["carga_ons"],
            mode="lines",
            name="Carga Líquida (ONS)",
            line=dict(color="#1e3a8a", width=3),  # Azul escuro
            fill="tozeroy",
            fillcolor="rgba(30, 58, 138, 0.3)",
            hovertemplate=(
                "<b>Carga Líquida (ONS)</b><br>"
                "Hora: %{x}<br>"
                "Carga: %{y:.0f} MW<br>"
                "<extra></extra>"
            )
        )
    )

    # Trace 2: Geração MMGD (amarelo, área preenchida acima da carga líquida)
    fig.add_trace(
        go.Scatter(
            x=df_carga["hora"],
            y=df_carga["carga_real_estimada"],
            mode="none",
            name="Geração MMGD",
            fill="tonexty",  # Preenche entre carga_ons e carga_real
            fillcolor="rgba(250, 204, 21, 0.5)",  # Amarelo translúcido
            hovertemplate=(
                "<b>Geração MMGD</b><br>"
                "Hora: %{x}<br>"
                "Geração: %{customdata:.0f} MW<br>"
                "<extra></extra>"
            ),
            customdata=df_carga["estimativa_solar_mw"]
        )
    )

    # Trace 3: Linha do Consumo Real (verde, borda superior)
    fig.add_trace(
        go.Scatter(
            x=df_carga["hora"],
            y=df_carga["carga_real_estimada"],
            mode="lines+markers",
            name="Consumo Real (Estimado)",
            line=dict(color="#16a34a", width=3, dash="solid"),  # Verde
            marker=dict(size=5, color="#16a34a"),
            hovertemplate=(
                "<b>Consumo Real</b><br>"
                "Hora: %{x}<br>"
                "Consumo: %{y:.0f} MW<br>"
                "<extra></extra>"
            )
        )
    )

    # Trace 4 (opcional): Carga Auditada com fraudes
    if impacto_projecao_mw > 0 and "carga_auditada" in df_carga.columns:
        fig.add_trace(
            go.Scatter(
                x=df_carga["hora"],
                y=df_carga["carga_auditada"],
                mode="lines",
                name=f"Cenário com Fraudes ({multiplicador}x)",
                line=dict(color="#dc2626", width=2, dash="dash"),  # Vermelho
                hovertemplate=(
                    "<b>Cenário com Fraudes</b><br>"
                    "Hora: %{x}<br>"
                    "Carga: %{y:.0f} MW<br>"
                    "<extra></extra>"
                )
            )
        )

    # Layout do gráfico
    fig.update_layout(
        title={
            "text": "Comparativo: Carga Líquida vs Consumo Real",
            "x": 0.5,
            "xanchor": "center"
        },
        xaxis_title="Data/Hora",
        yaxis_title="Potência Média (MW)",
        template="plotly_dark",
        hovermode="x unified",
        height=500,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.25,
            xanchor="center",
            x=0.5
        ),
        separators=",.",
        yaxis=dict(
            ticksuffix=" MW",
            rangemode="tozero"
        )
    )

    apply_plotly_locale(fig)
    st.plotly_chart(fig, use_container_width=True)

    # ===== LEGENDA VISUAL EXPLICATIVA =====
    st.markdown("""
    **Legenda do Gráfico:**
    - 🔵 **Área Azul**: Carga Líquida (o que o sistema de transmissão fornece)
    - 🟡 **Área Amarela**: Geração MMGD (energia gerada e consumida localmente)
    - 🟢 **Linha Verde**: Consumo Real Total (Carga Líquida + MMGD)

    A área amarela representa a "carga oculta" - energia que os consumidores usam mas o ONS não vê.
    """)


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
    col1.metric("Total Instalações MMGD", format_integer(resumo['total_instalacoes']))
    col2.metric("Unidades Consumidoras (UC)", format_integer(resumo['total_unidades_consumidoras']))
    col3.metric("Potência Total", format_mw(resumo['total_mw'], decimals=0))
    col4.metric("Média por UC", format_kw(resumo['total_mw'] / resumo['total_unidades_consumidoras'] * 1000, decimals=1))

    df_contagens = pd.DataFrame(contagens)

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
        format_integer(resumo['total_instalacoes']),
        help="Número total de instalações MMGD"
    )
    col2.metric(
        "Potência Total",
        format_mw(resumo['total_mw'], decimals=0),
        help="Capacidade instalada total"
    )

    df_contagens = pd.DataFrame(contagens)

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