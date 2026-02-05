"""
Componente para exibir histórico de carga da distribuidora.
Mostra gráfico com carga ONS, geração MMGD e consumo estimado ao longo do tempo.
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from services.api_client import ApiClient
from datetime import datetime, timedelta


def render_historico_carga(
    client: ApiClient,
    subsistema: str,
    distribuidora: str | None = None,
):
    """
    Renderiza gráfico interativo do histórico de carga da distribuidora.

    Exibe:
    - Carga ONS (linha principal)
    - Geração MMGD (área)
    - Consumo Estimado (linha tracejada)

    Args:
        client: Cliente da API
        subsistema: Subsistema elétrico
        distribuidora: Distribuidora (opcional)
    """
    try:
        # Buscar histórico de carga
        params = {"subsistema": subsistema}
        if distribuidora:
            params["distribuidora"] = distribuidora

        response = client.get("/analise/carga-oculta", params=params)

        if response.error:
            st.error(f"Erro ao carregar histórico: {response.error}")
            return

        # Verificar dados
        if not response.data or len(response.data) == 0:
            st.info("📊 Nenhum dado histórico disponível para a distribuidora selecionada.")
            return

        # Converter para DataFrame
        df = pd.DataFrame(response.data)

        # Garantir que temos as colunas necessárias
        required_cols = ['hora', 'carga_ons', 'estimativa_solar_mw', 'carga_real_estimada']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            st.warning(f"⚠️ Colunas faltando: {missing_cols}")
            st.info(f"Colunas disponíveis: {list(df.columns)}")
            st.dataframe(df.head(10))
            return

        # Converter data para datetime se necessário
        if df['hora'].dtype == 'object':
            df['hora'] = pd.to_datetime(df['hora'])

        # Ordenar por data
        df = df.sort_values('hora')

        # Criar figura com Plotly
        fig = go.Figure()

        # Adicionar carga ONS (linha principal - azul)
        fig.add_trace(go.Scatter(
            x=df['hora'],
            y=df['carga_ons'],
            mode='lines',
            name='Carga ONS',
            line=dict(color='#1f77b4', width=3),
            hovertemplate='<b>Carga ONS</b><br>%{x|%d/%m %H:%M}<br>%{y:.1f} MW<extra></extra>'
        ))

        # Adicionar geração MMGD (área - verde)
        fig.add_trace(go.Scatter(
            x=df['hora'],
            y=df['estimativa_solar_mw'],
            mode='lines',
            name='Geração MMGD',
            fill='tozeroy',
            line=dict(color='#2ca02c', width=0),
            fillcolor='rgba(44, 160, 44, 0.3)',
            hovertemplate='<b>Geração MMGD</b><br>%{x|%d/%m %H:%M}<br>%{y:.1f} MW<extra></extra>'
        ))

        # Adicionar consumo estimado (linha tracejada - vermelho)
        fig.add_trace(go.Scatter(
            x=df['hora'],
            y=df['carga_real_estimada'],
            mode='lines',
            name='Consumo Total',
            line=dict(color='#d62728', width=2, dash='dash'),
            hovertemplate='<b>Consumo Total</b><br>%{x|%d/%m %H:%M}<br>%{y:.1f} MW<extra></extra>'
        ))

        # Atualizar layout
        fig.update_layout(
            title=dict(
                text=f"📈 Histórico de Carga - {distribuidora or subsistema}",
                x=0.5,
                xanchor='center',
                font=dict(size=18, color='rgba(255, 255, 255, 0.95)')
            ),
            xaxis_title="Data/Hora",
            yaxis_title="Potência (MW)",
            hovermode='x unified',
            template='plotly_dark',
            height=500,
            margin=dict(l=60, r=40, t=80, b=60),
            xaxis=dict(
                showgrid=True,
                gridwidth=1,
                gridcolor='rgba(255, 255, 255, 0.1)',
                showline=True,
                linewidth=1,
                linecolor='rgba(255, 255, 255, 0.2)'
            ),
            yaxis=dict(
                showgrid=True,
                gridwidth=1,
                gridcolor='rgba(255, 255, 255, 0.1)',
                showline=True,
                linewidth=1,
                linecolor='rgba(255, 255, 255, 0.2)'
            ),
            legend=dict(
                x=0.02,
                y=0.98,
                bgcolor='rgba(20, 20, 20, 0.8)',
                bordercolor='rgba(255, 255, 255, 0.2)',
                borderwidth=1,
                font=dict(size=12)
            ),
            plot_bgcolor='rgba(20, 20, 20, 0.5)',
            paper_bgcolor='rgba(20, 20, 20, 0.3)',
        )

        # Exibir gráfico
        st.plotly_chart(fig, use_container_width=True, key="historico_carga")

        # Exibir estatísticas
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="📊 Carga Máxima",
                value=f"{df['carga_ons'].max():.1f} MW",
                help="Maior valor de carga registrado no período"
            )

        with col2:
            st.metric(
                label="📉 Carga Mínima",
                value=f"{df['carga_ons'].min():.1f} MW",
                help="Menor valor de carga registrado no período"
            )

        with col3:
            st.metric(
                label="⚡ Carga Média",
                value=f"{df['carga_ons'].mean():.1f} MW",
                help="Média da carga no período"
            )

        with col4:
            media_mmgd = df['estimativa_solar_mw'].mean()
            st.metric(
                label="☀️ MMGD Média",
                value=f"{media_mmgd:.1f} MW",
                help="Geração solar média no período"
            )

        # Opção para baixar dados
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📥 Baixar CSV", use_container_width=True):
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Clique para baixar",
                    data=csv,
                    file_name=f"historico_carga_{distribuidora or subsistema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

        with col2:
            if st.button("📊 Ver dados brutos", use_container_width=True):
                with st.expander("Dados detalhados"):
                    st.dataframe(
                        df.sort_values('hora', ascending=False),
                        use_container_width=True,
                        height=400
                    )

    except Exception as e:
        st.error(f"❌ Erro ao renderizar histórico: {e}")
        import traceback
        with st.expander("Detalhes do erro"):
            st.code(traceback.format_exc())


def render_comparacao_distribuidoras(
    client: ApiClient,
    subsistema: str,
    distribuidoras: list[str],
):
    """
    Renderiza gráfico comparativo de carga entre múltiplas distribuidoras.

    Args:
        client: Cliente da API
        subsistema: Subsistema elétrico
        distribuidoras: Lista de distribuidoras para comparar
    """
    try:
        # Buscar dados para cada distribuidora
        dados_dist = {}
        
        for dist in distribuidoras:
            response = client.get(
                "/analise/carga-oculta",
                params={"subsistema": subsistema, "distribuidora": dist}
            )
            
            if response.data and len(response.data) > 0:
                df = pd.DataFrame(response.data)
                if 'data_criacao' in df.columns:
                    df['data_criacao'] = pd.to_datetime(df['data_criacao'])
                    dados_dist[dist] = df

        if not dados_dist:
            st.info("❌ Nenhum dado disponível para as distribuidoras selecionadas.")
            return

        # Criar figura comparativa
        fig = go.Figure()

        # Cores diferentes para cada distribuidora
        cores = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']

        for i, (dist, df) in enumerate(dados_dist.items()):
            df = df.sort_values('hora')
            
            fig.add_trace(go.Scatter(
                x=df['hora'],
                y=df['carga_ons'],
                mode='lines',
                name=dist,
                line=dict(color=cores[i % len(cores)], width=2),
                hovertemplate=f'<b>{dist}</b><br>%{{x|%d/%m %H:%M}}<br>%{{y:.1f}} MW<extra></extra>'
            ))

        # Atualizar layout
        fig.update_layout(
            title=dict(
                text=f"⚖️ Comparação de Carga - {subsistema}",
                x=0.5,
                xanchor='center',
                font=dict(size=18, color='rgba(255, 255, 255, 0.95)')
            ),
            xaxis_title="Data/Hora",
            yaxis_title="Potência (MW)",
            hovermode='x unified',
            template='plotly_dark',
            height=500,
            margin=dict(l=60, r=40, t=80, b=60),
            xaxis=dict(
                showgrid=True,
                gridwidth=1,
                gridcolor='rgba(255, 255, 255, 0.1)',
            ),
            yaxis=dict(
                showgrid=True,
                gridwidth=1,
                gridcolor='rgba(255, 255, 255, 0.1)',
            ),
            legend=dict(
                x=0.02,
                y=0.98,
                bgcolor='rgba(20, 20, 20, 0.8)',
                bordercolor='rgba(255, 255, 255, 0.2)',
                borderwidth=1,
            ),
            plot_bgcolor='rgba(20, 20, 20, 0.5)',
            paper_bgcolor='rgba(20, 20, 20, 0.3)',
        )

        st.plotly_chart(fig, use_container_width=True, key="comparacao_distribuidoras")

    except Exception as e:
        st.error(f"❌ Erro ao comparar distribuidoras: {e}")
