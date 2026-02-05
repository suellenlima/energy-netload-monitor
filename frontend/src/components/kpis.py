"""
Componente de KPIs executivos no topo do dashboard.
Exibe métricas principais sempre visíveis acima das tabs.
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from services.api_client import ApiClient
from utils.errors import show_error

logger = logging.getLogger(__name__)


def _apply_kpi_card_style():
    """Aplica CSS customizado para cards de KPIs - estilo minimalista."""
    st.markdown("""
        <style>
        /* Container principal dos KPIs */
        div[data-testid="column"] {
            padding: 0 6px;
        }

        /* Card minimalista */
        div[data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 8px;
            padding: 20px 16px;
        }

        /* Label da métrica */
        div[data-testid="stMetric"] > label {
            font-size: 0.8rem;
            font-weight: 500;
            color: rgba(255, 255, 255, 0.6);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        /* Valor da métrica */
        div[data-testid="stMetric"] > div[data-testid="stMetricValue"] {
            font-size: 1.8rem;
            font-weight: 700;
            margin: 8px 0;
            color: rgba(255, 255, 255, 0.95);
        }

        /* Delta da métrica */
        div[data-testid="stMetric"] > div[data-testid="stMetricDelta"] {
            font-size: 0.75rem;
            font-weight: 400;
            color: rgba(255, 255, 255, 0.5);
        }
        </style>
    """, unsafe_allow_html=True)


def render_executive_kpis(
    client: ApiClient,
    subsistema: str,
    distribuidora: str | None = None,
) -> dict:
    """
    Renderiza cards de KPIs executivos no topo do dashboard.

    Exibe métricas principais em cards side-by-side:
    - Carga Atual (ONS)
    - Consumo Real Estimado
    - Geração MMGD
    - Status do Sistema

    Args:
        client: Cliente da API
        subsistema: Subsistema elétrico
        distribuidora: Distribuidora (opcional)

    Returns:
        Dict com valores dos KPIs para uso posterior
    """
    # Aplicar estilo de cards
    _apply_kpi_card_style()

    try:
        # Buscar dados de tempo real - versão simplificada para distribuidora
        if distribuidora:
            # Use endpoint específico para distribuidora
            response = client.get(
                "/analise/carga-atual-distribuidora",
                params={"distribuidora": distribuidora}
            )
        else:
            # Use endpoint geral por subsistema
            params = {"subsistema": subsistema}
            response = client.get("/analise/estado-atual", params=params)

        # Verificar erro primeiro
        if response.error:
            st.error(f"Erro ao carregar KPIs executivos: {response.error}")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Carga Atual", "--", help="Dados indisponíveis")
            with col2:
                st.metric("Consumo Real", "--", help="Dados indisponíveis")
            with col3:
                st.metric("Geração MMGD", "--", help="Dados indisponíveis")
            with col4:
                st.metric("Status", "Erro", help="Falha ao carregar dados")
            return {}

        # Verificar se há dados
        if not response.data:
            st.warning("Sem dados disponíveis para os KPIs")
            return {}

        # Suportar ambos os formatos
        if isinstance(response.data, dict):
            # Formato do novo endpoint carga-atual-distribuidora
            if "carga_ons_mw" in response.data:
                hora_atual = "--"
                carga_ons = response.data.get("carga_ons_mw", 0)
                consumo_estimado = 0  # Não disponível neste endpoint
                geracao_mmgd = 0  # Não disponível neste endpoint
                irradiancia = 0  # Não disponível neste endpoint
            # Formato do endpoint estado-atual
            elif "estimativas" in response.data:
                estimativas = response.data["estimativas"]
                hora_atual = response.data.get("hora_atual", "--")
                carga_ons = estimativas.get("carga_ons_mw", 0)
                consumo_estimado = estimativas.get("consumo_estimado_mw", 0)
                geracao_mmgd = estimativas.get("geracao_mmgd_mw", 0)
                irradiancia = estimativas.get("irradiancia_atual_wm2", 0)
            else:
                # Formato desconhecido
                st.warning("Formato de resposta não reconhecido")
                return {}

            # Calcular status do sistema
            if geracao_mmgd > 0:
                percentual_mmgd = (geracao_mmgd / consumo_estimado * 100) if consumo_estimado > 0 else 0
            else:
                percentual_mmgd = 0

            # Buscar carga do dia anterior para comparação
            carga_anterior = 0
            delta_carga = 0
            try:
                # Se for endpoint de distribuidora com data_medicao
                if distribuidora and "data_medicao" in response.data:
                    data_atual = pd.to_datetime(response.data.get("data_medicao"))
                    
                    # Buscar dados históricos para comparação
                    response_historico = client.get(
                        "/analise/carga-oculta",
                        params={"distribuidora": distribuidora}
                    )
                    
                    if response_historico.data and len(response_historico.data) > 0:
                        df_hist = pd.DataFrame(response_historico.data)
                        if not df_hist.empty and "hora" in df_hist.columns:
                            df_hist["hora"] = pd.to_datetime(df_hist["hora"])
                            
                            # Filtrar para 24h atrás
                            data_anterior = data_atual - timedelta(days=1)
                            df_anterior = df_hist[
                                (df_hist["hora"] >= data_anterior) & 
                                (df_hist["hora"] < data_atual)
                            ]
                            
                            if not df_anterior.empty:
                                carga_anterior = df_anterior["carga_ons"].mean()
                                delta_carga = carga_ons - carga_anterior
            except Exception as e:
                logger.debug(f"Não foi possível calcular delta: {e}")
                delta_carga = None

            # Determinar status
            if percentual_mmgd >= 25:
                status = "Ótimo"
                status_icon = "🟢"
            elif percentual_mmgd >= 15:
                status = "Bom"
                status_icon = "🟡"
            else:
                status = "Normal"
                status_icon = "🔵"

            # Renderizar KPIs em 4 colunas
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                # Formatar delta da carga
                if delta_carga is not None and delta_carga != 0:
                    delta_text = f"{delta_carga:+.1f} MW vs ontem"
                else:
                    delta_text = "Tempo real diário"
                
                st.metric(
                    label="Carga Atual (ONS)",
                    value=f"{carga_ons:,.1f} MW",
                    delta=delta_text,
                    help="Carga líquida medida pelo ONS nos pontos de entrega"
                )

            with col2:
                st.metric(
                    label="Consumo Real Estimado",
                    value=f"{consumo_estimado:,.1f} MW",
                    delta=f"+{geracao_mmgd:,.1f} MW MMGD",
                    help="Consumo total incluindo geração distribuída local"
                )

            with col3:
                st.metric(
                    label="Geração MMGD Atual",
                    value=f"{geracao_mmgd:,.1f} MW",
                    delta=f"{percentual_mmgd:.1f}% do consumo",
                    help="Geração distribuída (solar) consumida localmente"
                )

            with col4:
                st.metric(
                    label="Status Sistema",
                    value=status,
                    delta=f"{irradiancia:.0f} W/m² irradiância",
                    help="Avaliação baseada na contribuição da MMGD"
                )

            return {
                "carga_ons": carga_ons,
                "consumo_estimado": consumo_estimado,
                "geracao_mmgd": geracao_mmgd,
                "percentual_mmgd": percentual_mmgd,
                "status": status,
                "hora_atual": hora_atual
            }

        else:
            # Fallback: buscar dados históricos
            response_carga = client.get("/analise/carga-oculta", params=params)

            # Verificar erro no fallback
            if response_carga.error:
                st.error(f"Erro ao carregar KPIs executivos: {response_carga.error}")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("⚡ Carga Atual", "Erro", help="Falha ao carregar dados")
                with col2:
                    st.metric("📊 Consumo Real", "Erro", help="Falha ao carregar dados")
                with col3:
                    st.metric("☀️ Geração MMGD", "Erro", help="Falha ao carregar dados")
                with col4:
                    st.metric("📌 Status", "Erro", help="Falha ao carregar dados")
                return {}

            # Verificar se há dados no fallback
            if response_carga.data and len(response_carga.data) > 0:
                ultimo = response_carga.data[-1]
                carga_ons = ultimo.get("carga_ons", 0)
                geracao = ultimo.get("estimativa_solar_mw", 0)
                consumo = ultimo.get("carga_real_estimada", 0)

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric(
                        label="Carga Líquida (Última)",
                        value=f"{carga_ons:,.1f} MW",
                        help="Última medição da carga líquida ONS"
                    )

                with col2:
                    st.metric(
                        label="Consumo Real (Último)",
                        value=f"{consumo:,.1f} MW",
                        help="Último consumo real estimado"
                    )

                with col3:
                    st.metric(
                        label="MMGD (Última)",
                        value=f"{geracao:,.1f} MW",
                        help="Última geração MMGD estimada"
                    )

                with col4:
                    st.metric(
                        label="Status",
                        value="Histórico",
                        help="Dados históricos (tempo real indisponível)"
                    )

                return {
                    "carga_ons": carga_ons,
                    "consumo_estimado": consumo,
                    "geracao_mmgd": geracao,
                    "status": "Histórico"
                }

            else:
                # Sem dados disponíveis
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Carga Atual", "--", help="Dados indisponíveis")
                with col2:
                    st.metric("Consumo Real", "--", help="Dados indisponíveis")
                with col3:
                    st.metric("Geração MMGD", "--", help="Dados indisponíveis")
                with col4:
                    st.metric("Status", "Sem dados", help="Aguardando dados")

                return {}

    except Exception as e:
        st.error(f"Erro ao carregar KPIs executivos: {e}")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Carga Atual", "Erro", help="Falha ao carregar dados")
        with col2:
            st.metric("Consumo Real", "Erro", help="Falha ao carregar dados")
        with col3:
            st.metric("Geração MMGD", "Erro", help="Falha ao carregar dados")
        with col4:
            st.metric("Status", "Erro", help="Falha ao carregar dados")
        return {}