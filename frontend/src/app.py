
import streamlit as st

from components.alerts import fetch_alerta, render_alerta
from components.audit import render_auditoria
from components.charts import (
    load_carga_data,
    render_carga_section,
    render_classes_consumo,
    render_estabelecimentos_section,
    render_perfis_carga,
)
from components.realtime import render_realtime_dashboard
from components.sidebar import render_sidebar
from components.subestacoes import render_subestacoes_section, render_analise_local_subestacao
from config import API_URL, APP_TITLE, LAYOUT
from services.api_client import ApiClient


st.set_page_config(page_title=APP_TITLE, layout=LAYOUT)
st.title("Monitoramento Avançado de Carga Líquida")

# Tooltip educativo sobre conceitos fundamentais
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

client = ApiClient(API_URL)
state = render_sidebar(client)

dados_ia = fetch_alerta(client, state.distribuidora)
_, impacto_projecao_mw = render_alerta(dados_ia, state.multiplicador)

if state.refresh:
    # Monitoramento em Tempo Real (novo!)
    render_realtime_dashboard(
        client,
        subsistema=state.subsistema,
        distribuidora=state.distribuidora,
        auto_refresh=False
    )

    st.divider()

    df_carga = load_carga_data(client, state.subsistema, state.distribuidora)
    render_carga_section(df_carga, impacto_projecao_mw, state.multiplicador, state.subsistema)
    render_classes_consumo(client, state.distribuidora)

    # Renderizar análise de estabelecimentos
    st.divider()
    render_estabelecimentos_section(client, state.distribuidora)

    # Renderizar perfis de carga típicos
    st.divider()
    render_perfis_carga(client)

    render_auditoria(dados_ia, impacto_projecao_mw, state.multiplicador)

    # Renderizar seção de subestações
    st.divider()
    render_subestacoes_section(client, state.distribuidora)

    # Renderizar análise local por subestação (FASE 2)
    st.divider()
    render_analise_local_subestacao(client, state.distribuidora)
else:
    st.info("Selecione os filtros e clique em 'Atualizar Dashboard' para iniciar.")