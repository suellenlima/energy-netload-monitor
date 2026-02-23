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
        # Buscar dados de tempo real
        if distribuidora:
            # Use endpoint específico para distribuidora (carga)
            response = client.get(
                "/analise/carga-atual-distribuidora",
                params={"distribuidora": distribuidora}
            )
            # Também buscar estimativas (geracao_mmgd, consumo_estimado, irradiancia)
            # do endpoint estado-atual
            response_estado = client.get(
                "/analise/estado-atual",
                params={"subsistema": subsistema}
            )
            # Buscar dados de carga oculta/MMGD
            response_carga_oculta = client.get(
                "/analise/carga-oculta",
                params={"distribuidora": distribuidora}
            )
        else:
            # Use endpoint geral por subsistema
            params = {"subsistema": subsistema}
            response = client.get("/analise/estado-atual", params=params)
            response_estado = response  # Usar a mesma resposta
            response_carga_oculta = client.get("/analise/carga-oculta", params=params)

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
            # Formato do novo endpoint carga-atual-distribuidora (com dados separados)
            if "carga_granular_mw" in response.data:
                hora_atual = "--"
                # Separar carga granular e líquida
                carga_granular = response.data.get("carga_granular_mw", 0)  # Carga Atual (ONS) - só granular
                carga_liquida = response.data.get("carga_liquida_mw", 0)    # Carga Líquida
                carga_ons = carga_granular  # Carga Atual = apenas granular
                
                # Consumo Real Estimado = granular + líquida
                consumo_estimado = carga_granular + carga_liquida
                
                # Priorizar dados de carga_oculta para MMGD (dados reais)
                if distribuidora and response_carga_oculta and response_carga_oculta.data:
                    if isinstance(response_carga_oculta.data, list) and len(response_carga_oculta.data) > 0:
                        ultimo_oculta = response_carga_oculta.data[-1]
                        geracao_mmgd = ultimo_oculta.get("estimativa_solar_mw", 0)
                    else:
                        geracao_mmgd = 0
                else:
                    geracao_mmgd = 0
                
                # Irradiância vem do estado-atual
                irradiancia = 0
                if response_estado and response_estado.data:
                    if isinstance(response_estado.data, dict) and "estimativas" in response_estado.data:
                        estimativas = response_estado.data["estimativas"]
                        irradiancia = estimativas.get("irradiancia_atual_wm2", 0)
            # Formato antigo com carga_mw (fallback)
            elif "carga_mw" in response.data:
                hora_atual = "--"
                carga_liquida_mw = response.data.get("carga_mw", 0)
                carga_ons = carga_liquida_mw
                
                # Priorizar dados de carga_oculta para MMGD (dados reais)
                if distribuidora and response_carga_oculta and response_carga_oculta.data:
                    if isinstance(response_carga_oculta.data, list) and len(response_carga_oculta.data) > 0:
                        ultimo_oculta = response_carga_oculta.data[-1]
                        geracao_mmgd = ultimo_oculta.get("estimativa_solar_mw", 0)
                        consumo_estimado = ultimo_oculta.get("consumo_estimado_mw", 0)
                        # Se não tem consumo_estimado em carga_oculta, buscar em estado-atual
                        if consumo_estimado == 0 and response_estado and response_estado.data:
                            if isinstance(response_estado.data, dict) and "estimativas" in response_estado.data:
                                estimativas = response_estado.data["estimativas"]
                                consumo_estimado = estimativas.get("consumo_estimado_mw", 0)
                    else:
                        geracao_mmgd = 0
                        consumo_estimado = 0
                else:
                    geracao_mmgd = 0
                    consumo_estimado = 0
                
                # Irradiância vem do estado-atual
                irradiancia = 0
                if response_estado and response_estado.data:
                    if isinstance(response_estado.data, dict) and "estimativas" in response_estado.data:
                        estimativas = response_estado.data["estimativas"]
                        irradiancia = estimativas.get("irradiancia_atual_wm2", 0)
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
                    data_atual = pd.to_datetime(response.data.get("data_medicao"), format="mixed", errors="coerce")
                    
                    # Buscar dados históricos do novo endpoint carga-distribuidor-historico
                    response_historico = client.get(
                        "/analise/carga-distribuidor-historico",
                        params={"distribuidora": distribuidora, "dias": 7}
                    )
                    
                    if response_historico.data and len(response_historico.data) > 0:
                        df_hist = pd.DataFrame(response_historico.data)
                        if not df_hist.empty and "hora" in df_hist.columns:
                            df_hist["hora"] = pd.to_datetime(df_hist["hora"], format="mixed", errors="coerce")
                            
                            # Sanitizar colunas numéricas
                            numeric_cols = ["carga_ons", "estimativa_solar_mw", "consumo_estimado_mw", "carga_real_estimada"]
                            for col in numeric_cols:
                                if col in df_hist.columns:
                                    df_hist[col] = pd.to_numeric(df_hist[col], errors="coerce").fillna(0.0).astype(float)
                            
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

            # Buscar carga ANEEL se disponível
            carga_aneel = 0
            try:
                if distribuidora:
                    response_aneel = client.get(
                        "/analise/carga-distribuidor-historico",
                        params={"distribuidora": distribuidora, "dias": 1}
                    )
                    if response_aneel.data and len(response_aneel.data) > 0:
                        ultimo_aneel = response_aneel.data[-1]
                        carga_aneel = ultimo_aneel.get("carga_ons", 0)
            except Exception as e:
                logger.debug(f"Não foi possível carregar carga ANEEL: {e}")

            # Renderizar KPIs em 4 colunas
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                # Usar MMGD como delta
                delta_text = f"+{geracao_mmgd:,.1f} MW MMGD"
                
                st.metric(
                    label="Demanda Estimada",
                    value=f"~{carga_ons:,.1f} MW",
                    delta=delta_text,
                    help="Demanda calculada baseada em transformadores ANEEL, consumo granular e MMGD para o dia atual"
                )

            with col2:
                st.metric(
                    label="🏭 Geração MMGD (Agora)",
                    value=f"{geracao_mmgd:,.1f} MW",
                    delta=f"+{geracao_mmgd:,.1f} MW geração",
                    help="Geração distribuída estimada (painéis solares, mini-usinas)"
                )

            with col3:
                st.metric(
                    label="Penetração Solar",
                    value=f"{status_icon} {status}",
                    delta=f"{irradiancia:.0f} W/m² irradiância",
                    help="Avaliação baseada na contribuição da MMGD"
                )
            with col4:
                # Potência total ANEEL - não comparar com carga (são grandezas diferentes)
                # Calcular utilização: carga_atual / potencia_total * 100
                utilizacao = (carga_ons / carga_aneel * 100) if carga_aneel > 0 else 0
                delta_text_aneel = f"{utilizacao:.1f}% utilização"
                
                st.metric(
                    label="Potência Total",
                    value=f"{carga_aneel:,.1f} MW",
                    delta=delta_text_aneel,
                    help="Potência total instalada da distribuidora (capacidade)"
                )

            return {
                "carga_ons": carga_ons,
                "carga_aneel": carga_aneel,
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
                        label="Carga Proporcional",
                        value=f"{carga_ons:,.1f} MW",
                        help="Última carga calculada proporcionalmente (baseada em transformadores ANEEL)"
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