"""
Componentes de monitoramento em tempo real.

Exibe o estado atual do sistema com atualizações automáticas.
"""

from datetime import datetime
from typing import Dict, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from services.api_client import ApiClient
from utils.errors import show_error
from utils.formatters import (
    format_mw,
    format_wm2,
    format_temperature,
    format_percentage,
    apply_plotly_locale
)


def render_realtime_dashboard(
    client: ApiClient,
    subsistema: str = "SUDESTE",
    distribuidora: Optional[str] = None,
    auto_refresh: bool = False
) -> None:
    """
    Renderiza dashboard de monitoramento em tempo real.

    Args:
        client: Cliente da API
        subsistema: Subsistema elétrico
        distribuidora: Distribuidora (opcional)
        auto_refresh: Se True, atualiza automaticamente
    """
    st.header("⏱️ Monitoramento em Tempo Real")

    # Info explicativa
    with st.expander("ℹ️ Como funciona o 'Tempo Real'?"):
        st.markdown("""
        ### Estimativa Informada em Tempo Real

        Como **smart meters não estão disponíveis publicamente** no Brasil, criamos uma
        "visão em tempo real" combinando:

        1. **Última carga medida** do ONS (atualizada a cada hora)
        2. **Irradiância solar atual** via API meteorológica (Open-Meteo)
        3. **Perfis de carga típicos** aplicados à hora atual
        4. **Geração MMGD estimada** com base na irradiância e potência instalada

        **Resultado**: Uma estimativa defensável do estado atual do sistema que
        permite decisões operacionais mesmo sem medição direta por UC.

        Esta é a mesma abordagem usada pelo ONS e EPE para planejamento.
        """)

    # Parâmetros
    params = {"subsistema": subsistema}
    if distribuidora:
        params["distribuidora"] = distribuidora

    # Buscar estado atual
    result = client.get("/analise/estado-atual", params=params)

    if result.error:
        show_error(result.error)
        return

    if not result.data:
        st.warning("Sem dados disponíveis para o estado atual.")
        return

    estado = result.data

    # Timestamp de atualização
    timestamp_str = estado.get("timestamp", "")
    if timestamp_str:
        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        minutos_atras = (datetime.now(timestamp.tzinfo) - timestamp).seconds // 60

        st.info(
            f"**Última atualização**: {timestamp.strftime('%d/%m/%Y %H:%M:%S')} "
            f"(há {minutos_atras} minutos)"
        )
    else:
        st.info("**Status**: Dados atuais")

    # Métricas principais
    estimativas = estado.get("estimativas", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="☀️ Irradiância",
            value=format_wm2(estimativas.get('irradiancia_atual_wm2', 0)),
            help="Irradiância solar atual da API Open-Meteo"
        )

    with col2:
        st.metric(
            label="⚡ Carga ONS",
            value=format_mw(estimativas.get('carga_ons_mw', 0)),
            help="Última leitura de carga do ONS (carga líquida)"
        )

    with col3:
        st.metric(
            label="🔌 Consumo Real",
            value=format_mw(estimativas.get('consumo_estimado_mw', 0)),
            help="Carga ONS + Geração MMGD estimada"
        )

    with col4:
        st.metric(
            label="🏭 Geração MMGD",
            value=format_mw(estimativas.get('geracao_mmgd_mw', 0)),
            help="Geração solar distribuída estimada pela irradiância"
        )

    # Indicadores secundários
    st.divider()

    col_a, col_b, col_c = st.columns(3)

    with col_a:
        fator_cap = estimativas.get('fator_capacidade_solar', 0) * 100  # Converter para percentual
        st.metric(
            "Fator de Capacidade Solar",
            format_percentage(fator_cap),
            help="Irradiância / 1000 W/m² (referência)"
        )

    with col_b:
        st.metric(
            "Temperatura",
            format_temperature(estimativas.get('temperatura_c', 0))
        )

    with col_c:
        metadados = estado.get("metadados", {})
        mmgd_instalada = metadados.get("potencia_mmgd_instalada_mw", 0)
        st.metric(
            "MMGD Instalada",
            format_mw(mmgd_instalada),
            help="Potência total de micro e minigeração distribuída"
        )

    # Gráfico da curva do dia
    st.subheader("Curva do Dia")
    _render_daily_curve(
        estado.get("hora_atual", 0),
        estimativas,
        subsistema
    )

    # Auto-refresh
    if auto_refresh:
        import time
        st.info("🔄 Auto-refresh ativo (5 minutos)")
        time.sleep(300)  # 5 minutos
        st.rerun()

    # Controles
    st.divider()
    col_refresh, col_auto = st.columns([1, 3])

    with col_refresh:
        if st.button("🔄 Atualizar Agora", use_container_width=True, key="btn_refresh_realtime"):
            # Apenas faz rerun do componente sem resetar o estado do dashboard
            st.rerun()

    with col_auto:
        # Usar session_state para controlar auto-refresh
        if "auto_refresh_realtime" not in st.session_state:
            st.session_state.auto_refresh_realtime = False

        new_auto_refresh = st.checkbox(
            "Ativar auto-refresh (5 min)",
            value=st.session_state.auto_refresh_realtime,
            key="checkbox_auto_refresh"
        )

        if new_auto_refresh != st.session_state.auto_refresh_realtime:
            st.session_state.auto_refresh_realtime = new_auto_refresh
            st.rerun()


def _render_daily_curve(
    hora_atual: int,
    estimativas: Dict,
    subsistema: str
) -> None:
    """
    Renderiza curva estimada do dia com indicação da hora atual.

    Args:
        hora_atual: Hora atual do dia (0-23)
        estimativas: Estimativas do estado atual
        subsistema: Subsistema elétrico
    """
    # Gerar curva estimada usando perfil residencial como proxy
    # (simplificado - idealmente usar perfil agregado real)
    horas = list(range(24))

    # Perfil normalizado (0-2 aproximadamente)
    perfil_base = [
        0.5, 0.4, 0.4, 0.4, 0.4, 0.5,  # 00-05h: Madrugada
        0.7, 1.0, 1.2,                  # 06-08h: Manhã
        1.0, 0.9, 0.8,                  # 09-11h: Meio dia
        0.9, 1.0, 0.9,                  # 12-14h: Tarde
        0.8, 0.8, 0.9,                  # 15-17h: Final tarde
        1.3, 1.7, 1.8, 1.7, 1.5,        # 18-22h: PICO noturno
        1.0                              # 23h
    ]

    # Escalar para MW (usando carga atual como referência)
    carga_atual = estimativas.get("carga_ons_mw", 1000)
    fator_escala = carga_atual / perfil_base[hora_atual] if perfil_base[hora_atual] > 0 else 1
    carga_estimada = [p * fator_escala for p in perfil_base]

    # Criar figura
    fig = go.Figure()

    # Linha da curva
    fig.add_trace(go.Scatter(
        x=horas,
        y=carga_estimada,
        mode='lines',
        name='Carga Estimada',
        line=dict(color='#1f77b4', width=3),
        hovertemplate='%{x}h: %{y:.1f} MW<extra></extra>'
    ))

    # Marcador da hora atual
    fig.add_trace(go.Scatter(
        x=[hora_atual],
        y=[carga_estimada[hora_atual]],
        mode='markers',
        name='Agora',
        marker=dict(
            size=15,
            color='red',
            symbol='diamond',
            line=dict(width=2, color='white')
        ),
        hovertemplate=f'<b>AGORA ({hora_atual}h)</b><br>%{{y:.1f}} MW<extra></extra>'
    ))

    # Linha vertical na hora atual
    fig.add_vline(
        x=hora_atual,
        line_dash="dash",
        line_color="red",
        opacity=0.5,
        annotation_text=f"Agora ({hora_atual}h)",
        annotation_position="top"
    )

    # Layout
    fig.update_layout(
        title=f"Curva de Carga Estimada - {subsistema}",
        xaxis_title="Hora do Dia",
        yaxis_title="Carga (MW)",
        template="plotly_dark",
        hovermode="x unified",
        height=400,
        showlegend=True,
        separators=",."  # Formato brasileiro: decimal=vírgula, milhares=ponto
    )

    fig.update_xaxes(
        tickmode='linear',
        tick0=0,
        dtick=2,
        ticksuffix="h",
        range=[-0.5, 23.5]
    )

    fig.update_yaxes(
        ticksuffix=" MW"
    )

    # Aplicar formatação brasileira
    fig = apply_plotly_locale(fig)

    st.plotly_chart(fig, use_container_width=True)


def render_realtime_metrics_compact(
    client: ApiClient,
    subsistema: str = "SUDESTE"
) -> None:
    """
    Versão compacta das métricas em tempo real (para sidebar ou header).

    Args:
        client: Cliente da API
        subsistema: Subsistema elétrico
    """
    params = {"subsistema": subsistema}
    result = client.get("/analise/estado-atual", params=params)

    if result.error or not result.data:
        st.caption("⚠️ Dados em tempo real indisponíveis")
        return

    estado = result.data
    estimativas = estado.get("estimativas", {})

    # Layout compacto
    st.caption("**Estado Atual:**")

    cols = st.columns(3)
    with cols[0]:
        st.metric(
            "☀️",
            f"{estimativas.get('irradiancia_atual_wm2', 0):.0f}",
            "W/m²",
            label_visibility="collapsed"
        )

    with cols[1]:
        st.metric(
            "⚡",
            f"{estimativas.get('carga_ons_mw', 0):.0f}",
            "MW ONS",
            label_visibility="collapsed"
        )

    with cols[2]:
        st.metric(
            "🔌",
            f"{estimativas.get('consumo_estimado_mw', 0):.0f}",
            "MW Real",
            label_visibility="collapsed"
        )
