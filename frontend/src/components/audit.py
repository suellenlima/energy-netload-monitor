from typing import Any, Dict, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from services.api_client import ApiClient


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def render_auditoria(dados_ia: Optional[Dict[str, Any]], impacto_projecao_mw: float, multiplicador: int) -> None:
    if not dados_ia or dados_ia.get("status") != "ALERTA":
        st.info("Selecione uma Distribuidora na barra lateral para iniciar a auditoria.")
        return

    with st.container():
        c1, c2 = st.columns([1.5, 1])

        with c1:
            st.subheader("Evidência Detectada (Satélite)")
            kpi1, kpi2 = st.columns(2)
            kpi3, kpi4 = st.columns(2)

            fraude_kw = _to_float(dados_ia.get("fraude_kw"))
            data_inspecao = dados_ia.get("data")
            area_m2 = fraude_kw / 0.2 if fraude_kw else 0
            classe_ia = str(dados_ia.get("classe_ia", "N/A"))
            classe_icone = "IND" if "Industrial" in classe_ia or "Comercial" in classe_ia else "RES"

            kpi1.metric("Classificação (IA)", f"{classe_icone} {classe_ia}")
            if data_inspecao:
                kpi2.metric("Data da Varredura", pd.to_datetime(data_inspecao, format="mixed", errors="coerce").strftime("%d/%m/%Y"))
            else:
                kpi2.metric("Data da Varredura", "N/A")
            kpi3.metric("Área Estimada", f"{area_m2:.0f} m2")
            kpi4.metric("Potência Oculta", f"{fraude_kw:.2f} kW", delta="Desvio", delta_color="inverse")

            st.info("O algoritmo identificou padrões visuais incompatíveis na coordenada analisada.")

        with c2:
            st.subheader("Nível de Risco Projetado")
            max_risco_mw = 500
            fig_gauge = go.Figure(
                go.Indicator(
                    mode="gauge+number+delta",
                    value=impacto_projecao_mw,
                    title={"text": f"Carga Fantasma ({multiplicador}x)"},
                    delta={"reference": 0, "increasing": {"color": "red"}},
                    gauge={
                        "axis": {"range": [None, max_risco_mw], "tickwidth": 1},
                        "bar": {"color": "#ff2b2b"},
                        "bgcolor": "rgba(0,0,0,0)",
                        "steps": [
                            {"range": [0, max_risco_mw * 0.3], "color": "rgba(0, 255, 0, 0.3)"},
                            {"range": [max_risco_mw * 0.3, max_risco_mw * 0.7], "color": "rgba(255, 165, 0, 0.3)"},
                            {"range": [max_risco_mw * 0.7, max_risco_mw], "color": "rgba(255, 0, 0, 0.3)"},
                        ],
                        "threshold": {
                            "line": {"color": "red", "width": 4},
                            "thickness": 0.75,
                            "value": impacto_projecao_mw,
                        },
                    },
                )
            )
            fig_gauge.update_layout(height=300, margin=dict(t=50, b=20, l=20, r=20), template="plotly_dark")
            st.plotly_chart(fig_gauge, use_container_width=True)

            # Indicar fonte da detecção
            fonte = dados_ia.get("fonte", "desconhecida")
            if fonte == "deteccao_automatica":
                st.caption("🤖 Alerta gerado por detecção automática de anomalias")
            elif fonte == "auditoria_manual":
                st.caption("👁️ Alerta gerado por inspeção visual manual")


def render_historico_alertas(client: ApiClient, distribuidora: str | None = None) -> None:
    """
    Renderiza histórico de alertas de anomalias/fraudes.

    Args:
        client: Cliente da API
        distribuidora: Filtrar por distribuidora (opcional)
    """
    st.markdown("---")
    st.subheader("📋 Histórico de Alertas")

    # Controles
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        dias = st.slider("Período (dias)", min_value=7, max_value=90, value=30, step=7)

    with col2:
        limite = st.number_input("Máximo alertas", min_value=10, max_value=100, value=50, step=10)

    with col3:
        if st.button("🔄 Atualizar", type="primary"):
            st.rerun()

    # Buscar alertas
    try:
        params = {"dias": dias, "limite": limite}
        if distribuidora:
            params["distribuidora"] = distribuidora

        result = client.get("/analise/alertas-historico", params=params)

        if result.error:
            st.error(f"Erro ao carregar histórico: {result.error}")
            return

        if not result.data or not result.data.get("alertas"):
            st.info("Nenhum alerta no período selecionado.")
            return

        dados = result.data
        alertas = dados.get("alertas", [])

        # Métricas resumo
        col1, col2, col3, col4 = st.columns(4)

        total = len(alertas)
        ativos = sum(1 for a in alertas if a.get("status") == "ativo")
        resolvidos = sum(1 for a in alertas if a.get("status") == "resolvido")

        # Severidade
        severidade_counts = {"alto": 0, "medio": 0, "baixo": 0}
        for a in alertas:
            sev = a.get("severidade", "baixo")
            if sev in severidade_counts:
                severidade_counts[sev] += 1

        col1.metric("Total Alertas", total)
        col2.metric("🔴 Ativos", ativos)
        col3.metric("🟢 Resolvidos", resolvidos)
        col4.metric("⚠️ Severidade Alta", severidade_counts["alto"])

        st.divider()

        # Filtros adicionais
        col_tipo, col_sev, col_status = st.columns(3)

        with col_tipo:
            tipos = list(set(a.get("tipo", "").replace("_", " ").title() for a in alertas))
            filtro_tipo = st.multiselect("Filtrar por Tipo", options=tipos, default=tipos)

        with col_sev:
            filtro_sev = st.multiselect(
                "Filtrar por Severidade",
                options=["alto", "medio", "baixo"],
                default=["alto", "medio", "baixo"]
            )

        with col_status:
            status_opts = list(set(a.get("status", "") for a in alertas))
            filtro_status = st.multiselect("Filtrar por Status", options=status_opts, default=status_opts)

        # Aplicar filtros
        alertas_filtrados = [
            a for a in alertas
            if a.get("tipo", "").replace("_", " ").title() in filtro_tipo
            and a.get("severidade", "") in filtro_sev
            and a.get("status", "") in filtro_status
        ]

        st.caption(f"Mostrando {len(alertas_filtrados)} de {total} alertas")

        # Tabela de alertas
        if alertas_filtrados:
            df_alertas = pd.DataFrame(alertas_filtrados)
            
            # Sanitizar tipos de dados
            if "impacto_kw" in df_alertas.columns:
                df_alertas["impacto_kw"] = pd.to_numeric(df_alertas["impacto_kw"], errors="coerce").fillna(0.0).astype(float)
            for str_col in ["distribuidora", "tipo", "severidade", "descricao", "status"]:
                if str_col in df_alertas.columns:
                    df_alertas[str_col] = df_alertas[str_col].fillna("").astype(str)

            # Preparar colunas para display
            colunas_exibir = []
            if "data_deteccao" in df_alertas.columns:
                df_alertas["Data"] = pd.to_datetime(df_alertas["data_deteccao"], format="mixed", errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
                colunas_exibir.append("Data")

            if "distribuidora" in df_alertas.columns:
                colunas_exibir.append("distribuidora")

            if "tipo" in df_alertas.columns:
                df_alertas["Tipo"] = df_alertas["tipo"].str.replace("_", " ").str.title()
                colunas_exibir.append("Tipo")

            if "severidade" in df_alertas.columns:
                # Emoji por severidade
                sev_map = {"alto": "🔴", "medio": "🟡", "baixo": "🟢"}
                df_alertas["Severidade"] = df_alertas["severidade"].apply(lambda x: f"{sev_map.get(x, '')} {x.title()}")
                colunas_exibir.append("Severidade")

            if "descricao" in df_alertas.columns:
                colunas_exibir.append("descricao")

            if "status" in df_alertas.columns:
                df_alertas["Status"] = df_alertas["status"].str.title()
                colunas_exibir.append("Status")

            # Adicionar impacto se disponível
            if "impacto_kw" in df_alertas.columns:
                df_alertas["Impacto (kW)"] = df_alertas["impacto_kw"].round(2)
                colunas_exibir.append("Impacto (kW)")

            # Renomear colunas
            rename_map = {
                "distribuidora": "Distribuidora",
                "descricao": "Descrição"
            }
            df_display = df_alertas[colunas_exibir].rename(columns=rename_map)

            st.dataframe(
                df_display,
                use_container_width=True,
                hide_index=True,
                height=400
            )

            # Gráfico de distribuição por tipo
            st.markdown("#### 📊 Distribuição de Alertas")

            col_grafico1, col_grafico2 = st.columns(2)

            with col_grafico1:
                # Por tipo
                tipo_counts = df_alertas["tipo"].value_counts()
                fig_tipo = go.Figure(
                    go.Bar(
                        x=tipo_counts.index.str.replace("_", " ").str.title(),
                        y=tipo_counts.values,
                        marker_color="#636EFA"
                    )
                )
                fig_tipo.update_layout(
                    title="Alertas por Tipo",
                    xaxis_title="Tipo",
                    yaxis_title="Quantidade",
                    template="plotly_dark",
                    height=300
                )
                st.plotly_chart(fig_tipo, use_container_width=True)

            with col_grafico2:
                # Por severidade
                sev_counts = df_alertas["severidade"].value_counts()
                cores_sev = {"alto": "#FF4B4B", "medio": "#FFA500", "baixo": "#00CC00"}
                cores = [cores_sev.get(s, "#636EFA") for s in sev_counts.index]

                fig_sev = go.Figure(
                    go.Bar(
                        x=sev_counts.index.str.title(),
                        y=sev_counts.values,
                        marker_color=cores
                    )
                )
                fig_sev.update_layout(
                    title="Alertas por Severidade",
                    xaxis_title="Severidade",
                    yaxis_title="Quantidade",
                    template="plotly_dark",
                    height=300
                )
                st.plotly_chart(fig_sev, use_container_width=True)

        else:
            st.info("Nenhum alerta corresponde aos filtros selecionados.")

    except Exception as e:
        st.error(f"Erro ao carregar histórico de alertas: {e}")