"""
Dashboard principal do sistema de Monitoramento de Carga Líquida.
Reorganizado com tabs para melhor UX e redução de scroll.
"""

import streamlit as st

from components.alerts import fetch_alerta, render_alerta
from components.audit import render_auditoria, render_historico_alertas
from components.charts import (
    load_carga_data,
    render_carga_section,
    render_classes_consumo,
    render_classes_consumo_compact,
    render_estabelecimentos_section,
    render_estabelecimentos_compact,
    render_perfis_carga,
)
from components.kpis import render_executive_kpis
from components.realtime import render_realtime_dashboard
from components.sidebar import render_sidebar
from components.subestacoes import render_analise_local_subestacao, render_subestacoes_section
from config import API_URL, APP_TITLE, LAYOUT
from services.api_client import ApiClient


# ==================== CONFIGURAÇÃO INICIAL ====================
st.set_page_config(page_title=APP_TITLE, layout=LAYOUT, initial_sidebar_state="expanded")

st.title("⚡ Monitoramento Avançado de Carga Líquida")

# ==================== TOOLTIP EDUCATIVO (SEMPRE VISÍVEL) ====================
with st.expander("ℹ️ Entenda os conceitos do sistema", expanded=False):
    col_a, col_b, col_c = st.columns(3)

    with col_a:
        st.markdown("""
        ### 🔵 Carga Líquida (ONS)
        **O que é**: Medida nos pontos de entrega do sistema de transmissão.
        É o que o Operador Nacional do Sistema (ONS) "enxerga" e registra oficialmente.

        **O que inclui**:
        - Energia fornecida pela rede de transmissão
        - Proveniente de usinas (hidro, térmica, eólica, solar centralizadas)

        **O que NÃO inclui**:
        - Geração distribuída (MMGD) consumida localmente
        - Painéis solares em residências/comércios
        """)

    with col_b:
        st.markdown("""
        ### 🟡 Geração MMGD
        **O que é**: Micro e Minigeração Distribuída. Painéis solares e pequenas
        usinas instaladas no ponto de consumo.

        **Características**:
        - Capacidade: até 75 kW (micro) ou até 5 MW (mini)
        - Geram energia consumida localmente
        - Não passam pela transmissão

        **Impacto**:
        - Durante o dia solar, reduzem a carga vista pelo ONS
        - Criam uma "carga oculta"
        """)

    with col_c:
        st.markdown("""
        ### 🟢 Consumo Real
        **O que é**: Demanda TOTAL de energia pelos consumidores finais.

        **Fórmula**:
        ```
        Consumo Real =
          Carga Líquida ONS +
          Geração MMGD
        ```

        **Relevância**:
        - Representa a demanda real que precisa ser atendida
        - Essencial para planejamento energético
        - Base para análises de eficiência
        """)

    st.divider()

    st.markdown("""
    ### 📊 Exemplo Prático (12h - Pico Solar)

    | Variável | Valor | Explicação |
    |----------|-------|------------|
    | **Consumo Real** | 100 MW | O que os consumidores realmente usam |
    | **Geração MMGD** | 30 MW | Painéis solares gerando |
    | **Carga Líquida ONS** | 70 MW | 100 - 30 = o que vem da rede |

    **Conclusão**: O ONS só "vê" 70 MW, mas o consumo real é 100 MW.
    Os 30 MW de diferença são a "carga oculta" - energia gerada e consumida localmente.
    """)

# ==================== SIDEBAR E ESTADO ====================
client = ApiClient(API_URL)
state = render_sidebar(client)

# ==================== ALERTAS (SEMPRE VISÍVEL) ====================
dados_ia = fetch_alerta(client, state.distribuidora)
_, impacto_projecao_mw = render_alerta(dados_ia, state.multiplicador)

st.divider()

# ==================== DASHBOARD PRINCIPAL ====================
if state.refresh:
    # ==================== KPIs EXECUTIVOS (TOPO) ====================
    kpis_data = render_executive_kpis(client, state.subsistema, state.distribuidora)

    st.divider()

    # ==================== BREADCRUMB ====================
    # Será atualizado dentro de cada tab
    breadcrumb_placeholder = st.empty()

    # ==================== TABS PRINCIPAIS ====================
    tab_visao, tab_tempo, tab_subestacoes, tab_perfis, tab_auditoria = st.tabs([
        "📊 Visão Geral",
        "⚡ Tempo Real",
        "🏭 Subestações",
        "📈 Perfis & Análise",
        "🔍 Auditoria"
    ])

    # ==================== TAB 1: VISÃO GERAL ====================
    with tab_visao:
        st.caption("Principais indicadores de carga, classes de consumo e estabelecimentos")

        # Análise de Carga (Líquida vs Real)
        df_carga = load_carga_data(client, state.subsistema, state.distribuidora)
        render_carga_section(df_carga, impacto_projecao_mw, state.multiplicador, state.subsistema)

        st.divider()

        # Classes de Consumo e Estabelecimentos lado a lado
        st.markdown("#### 🏘️ Distribuição por Classes e Estabelecimentos")

        col_classes, col_estab = st.columns(2)

        with col_classes:
            st.markdown("##### Classes de Consumo")
            render_classes_consumo_compact(client, state.distribuidora)

        with col_estab:
            st.markdown("##### Estabelecimentos")
            render_estabelecimentos_compact(client, state.distribuidora)

    # ==================== TAB 2: TEMPO REAL ====================
    with tab_tempo:
        st.caption("Dashboard operacional com estimativas em tempo real e auto-refresh")

        render_realtime_dashboard(
            client,
            subsistema=state.subsistema,
            distribuidora=state.distribuidora,
            auto_refresh=False
        )

    # ==================== TAB 3: SUBESTAÇÕES ====================
    with tab_subestacoes:
        st.caption("Subestações oficiais (ONS), detectadas por clustering e análise local")

        # Seção de subestações (com sub-tabs internas)
        render_subestacoes_section(client, state.distribuidora)

        st.divider()

        # Análise local por subestação (FASE 2)
        render_analise_local_subestacao(client, state.distribuidora)

    # ==================== TAB 4: PERFIS & ANÁLISE ====================
    with tab_perfis:
        st.caption("Perfis típicos por classe de consumo e ferramentas analíticas")

        # Perfis de carga típicos
        render_perfis_carga(client)

    # ==================== TAB 5: AUDITORIA ====================
    with tab_auditoria:
        st.caption("Detecção de fraudes e análise de integridade do sistema")

        # Verificar se há dados de auditoria
        if dados_ia and dados_ia.get("status") == "ALERTA":
            render_auditoria(dados_ia, impacto_projecao_mw, state.multiplicador)
        else:
            st.success("✅ Sistema sem alertas de integridade detectados")

            # Mostrar informações mesmo sem alertas
            st.info("""
            **Monitoramento Ativo**

            O sistema está monitorando continuamente:
            - 🔍 Detecção de fraudes via IA
            - ⚖️ Análise de integridade de dados
            - 📊 Comparação com padrões esperados
            - 🚨 Alertas automáticos em caso de anomalias
            """)

            # Mostrar métricas básicas mesmo sem alerta
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Status Geral", "Normal", delta="✓ Sem anomalias", help="Sistema operando normalmente")
            with col2:
                st.metric("Última Verificação", "Agora", help="Monitoramento contínuo ativo")
            with col3:
                st.metric("Nível de Risco", "Baixo", delta="0% detectado", help="Nenhuma irregularidade identificada")

        # Histórico de alertas (sempre visível)
        render_historico_alertas(client, state.distribuidora)

else:
    # ==================== MENSAGEM INICIAL ====================
    st.info("👈 Selecione os filtros na barra lateral e clique em **'Atualizar Dashboard'** para iniciar.")

    # Instruções de uso
    with st.expander("📖 Como usar este dashboard", expanded=True):
        st.markdown("""
        ### Guia Rápido

        1. **Selecione o Subsistema** (SUDESTE, SUL, NORDESTE, NORTE)
        2. **Escolha a Distribuidora** (ou deixe "Todas")
        3. **Ajuste o Multiplicador de Fraude** (para simulações)
        4. **Clique em "Atualizar Dashboard"**

        ### Navegação por Tabs

        - **📊 Visão Geral**: Principais métricas e gráficos de carga
        - **⚡ Tempo Real**: Monitoramento operacional com auto-refresh
        - **🏭 Subestações**: Análise geoespacial e clustering
        - **📈 Perfis & Análise**: Perfis de carga típicos e ferramentas analíticas
        - **🔍 Auditoria**: Detecção de fraudes e integridade

        ### Recursos Disponíveis

        - ✅ KPIs executivos sempre visíveis no topo
        - ✅ Breadcrumb de navegação contextual
        - ✅ Gráficos interativos (zoom, pan, hover)
        - ✅ Tabelas exportáveis
        - ✅ Expanders educativos em cada seção
        - ✅ Auto-refresh opcional no tempo real
        """)
