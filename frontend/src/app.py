import streamlit as st
import pandas as pd
import requests
import plotly.graph_objects as go
import plotly.express as px

# 1. Configuração da Página
st.set_page_config(page_title="Energy Monitor Pro", layout="wide", page_icon="⚡")
API_URL = "http://backend:8000"

st.title("⚡ Monitoramento Avançado de Carga Líquida")

# =========================================================
# SIDEBAR (Filtros e Slider)
# =========================================================
st.sidebar.header("⚙️ Configurações")

# 1. Filtro de Subsistema
subsistema = st.sidebar.selectbox("Subsistema (ONS)", ["SUDESTE", "SUL", "NORDESTE", "NORTE"])

# 2. Filtro de Distribuidora (Dinâmico do Banco)
st.sidebar.subheader("Análise por Distribuidora")
try:
    # Busca lista atualizada do backend
    resp_dist = requests.get(f"{API_URL}/auxiliar/distribuidoras", timeout=2)
    opcoes_distribuidoras = resp_dist.json() if resp_dist.status_code == 200 else [""]
except:
    opcoes_distribuidoras = ["", "ENEL", "CEMIG"] # Fallback

distribuidora_filtro = st.sidebar.selectbox("Concessão (GD):", opcoes_distribuidoras)

st.sidebar.markdown("---")

# 3. O SLIDER (Simulador de Impacto)
st.sidebar.subheader("🤖 Cruzamento com IA")
multiplicador = st.sidebar.slider("Projeção de Fraudes (Qtd de casos)", 1, 5000, 1)
st.sidebar.info("Arraste para simular o impacto de múltiplas fraudes na rede.")

# =========================================================
# LÓGICA DE ALERTA (Topo da Página)
# =========================================================
try:
    # Passa a distribuidora selecionada para filtrar o alerta
    params_alerta = {}
    if distribuidora_filtro:
        params_alerta["distribuidora"] = distribuidora_filtro
        
    resp_alerta = requests.get(f"{API_URL}/analise/alertas-fraude", params=params_alerta, timeout=2)
    dados_ia = resp_alerta.json() if resp_alerta.status_code == 200 and resp_alerta.json() else None
except:
    dados_ia = None

# Variáveis globais de impacto
fraude_mw = 0
impacto_projecao_mw = 0

if dados_ia and dados_ia.get('status') == 'ALERTA':
    # Cálculo do Impacto
    fraude_mw = dados_ia['fraude_kw'] / 1000 
    impacto_projecao_mw = fraude_mw * multiplicador
    
    with st.container():
        st.error(f"🚨 ALERTA: FRAUDE DETECTADA NA CONCESSÃO {dados_ia.get('distribuidora', 'DESCONHECIDA')}")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Detecção Unitária (IA)", f"{dados_ia['fraude_kw']:.2f} kW")
        # O slider altera este valor visualmente
        c2.metric(f"Projeção ({multiplicador}x)", f"{impacto_projecao_mw:.2f} MW", delta="Carga Fantasma")
        c3.metric("Potência Oficial", f"{dados_ia['oficial_kw']:.2f} kW")
        c4.metric("Ação", "Despacho Térmico", delta="Urgente", delta_color="inverse")
        st.divider()

# =========================================================
# BOTÃO DE ATUALIZAR E GRÁFICOS PRINCIPAIS
# =========================================================
if st.sidebar.button("Atualizar Dashboard", type="primary"):
    
    # 1. Buscar Dados de Carga (Oficial + Oculta)
    try:
        params_carga = {"subsistema": subsistema}
        if distribuidora_filtro:
            params_carga["distribuidora"] = distribuidora_filtro

        resp_carga = requests.get(f"{API_URL}/analise/carga-oculta", params=params_carga)
        if resp_carga.status_code == 200:
            df_carga = pd.DataFrame(resp_carga.json())
            
            # --- CORREÇÃO AQUI: Converter texto para Data ---
            if not df_carga.empty and 'hora' in df_carga.columns:
                df_carga['hora'] = pd.to_datetime(df_carga['hora'])
        else:
            df_carga = pd.DataFrame()
            
    except Exception as e:
        st.error(f"Erro ao buscar dados de carga: {e}")
        df_carga = pd.DataFrame()

    # 2. Injetar a Fraude no DataFrame do Gráfico
    if not df_carga.empty and impacto_projecao_mw > 0:
        df_carga['carga_auditada'] = df_carga['carga_real_estimada'] + impacto_projecao_mw

    # 3. Renderizar Gráfico (Curva do Pato)
    st.header(f"📉 Curva de Carga Líquida ({subsistema})")
    
    if not df_carga.empty:
        # --- ALTERAÇÃO AQUI: Mudamos para 4 colunas ---
        col1, col2, col3, col4 = st.columns(4)
        
        # Cálculos
        carga_atual = df_carga.iloc[-1]['carga_ons']
        oculta_oficial = df_carga.iloc[-1]['estimativa_solar_mw']
        
        # Pegamos o PICO do dia para mostrar o potencial solar
        pico_solar_dia = df_carga['estimativa_solar_mw'].max()
        hora_pico = df_carga.loc[df_carga['estimativa_solar_mw'].idxmax(), 'hora'].strftime('%Hh')
        
        # Coluna 1: Carga Rede
        col1.metric("Carga Rede (ONS)", f"{carga_atual:,.0f} MW")
        
        # Coluna 2: Carga Oculta ANEEL (Agora)
        col2.metric("GD Distribuída (Agora)", f"{oculta_oficial:,.0f} MW", delta="Oficial")
        
        # Coluna 3: Oculta Auditada (Com a Fraude)
        if impacto_projecao_mw > 0:
             col3.metric("Carga Auditada (IA)", f"{oculta_oficial + impacto_projecao_mw:,.0f} MW", delta="RISCO", delta_color="inverse")
        else:
             col3.metric("Carga Auditada", "Sem desvios")

        # Coluna 4: INDICADOR SOLAR (Novo!) ☀️
        # Mostra o máximo que a energia solar vai atingir hoje
        col4.metric(f"Pico Solar (às {hora_pico})", f"{pico_solar_dia:,.0f} MW", delta="Atividade Solar")

        # Plotly
        fig = go.Figure()
        
        # Linha Azul (ONS)
        fig.add_trace(go.Scatter(x=df_carga['hora'], y=df_carga['carga_ons'], mode='lines', name='Carga Rede (ONS)', line=dict(color='#00BFFF', width=3), fill='tozeroy'))
        # Linha Laranja (Oficial)
        fig.add_trace(go.Scatter(x=df_carga['hora'], y=df_carga['carga_real_estimada'], mode='lines', name='Consumo Real (ANEEL)', line=dict(color='#FFA500', width=2, dash='dot')))
        
        # Linha Vermelha (Projeção)
        if impacto_projecao_mw > 0 and 'carga_auditada' in df_carga.columns:
            fig.add_trace(go.Scatter(x=df_carga['hora'], y=df_carga['carga_auditada'], mode='lines', name=f'Cenário Projetado ({multiplicador}x)', line=dict(color='#FF0000', width=2, dash='dashdot')))
            fig.add_trace(go.Scatter(x=df_carga['hora'], y=df_carga['carga_auditada'], fill='tonexty', fillcolor='rgba(255, 0, 0, 0.3)', name='Carga Fantasma', mode='none'))

        fig.update_layout(height=450, template="plotly_dark", title="Impacto das Fraudes na Curva de Carga")
        st.plotly_chart(fig, use_container_width=True)

    # 4. Gráfico de Rosca (Classificação Oficial)
    try:
        resp_classe = requests.get(f"{API_URL}/analise/classes-consumo", params={"distribuidora": distribuidora_filtro})
        if resp_classe.status_code == 200 and resp_classe.json():
            df_classes = pd.DataFrame(resp_classe.json())
            st.markdown("---")
            st.header(f"☀️ Detalhamento da Concessão")
            c1, c2 = st.columns([1, 2])
            with c1: st.dataframe(df_classes, use_container_width=True, hide_index=True)
            with c2: 
                fig_pie = px.pie(df_classes, values='mw', names='classe', hole=0.4, title="Perfil de Consumo")
                fig_pie.update_layout(template="plotly_dark")
                st.plotly_chart(fig_pie, use_container_width=True)
    except:
        pass

    # =========================================================
    # 5. AUDITORIA VISUAL (Layout "Cockpit")
    # =========================================================
    st.markdown("---")
    st.header(f"🔍 Relatório de Auditoria de Integridade")

    if dados_ia and dados_ia.get('status') == 'ALERTA':
        with st.container():
            c1, c2 = st.columns([1.5, 1])

            # COLUNA 1: DADOS TÉCNICOS DA FRAUDE (Cartões)
            with c1:
                st.subheader("📡 Evidência Detectada (Satélite)")
                kpi1, kpi2 = st.columns(2)
                kpi3, kpi4 = st.columns(2)
                
                area_m2 = dados_ia['fraude_kw'] / 0.2
                classe_icone = "🏭" if "Industrial" in str(dados_ia.get('classe_ia')) or "Comercial" in str(dados_ia.get('classe_ia')) else "🏠"
                
                kpi1.metric("Classificação (IA)", f"{classe_icone} {dados_ia.get('classe_ia', 'N/A')}")
                kpi2.metric("Data da Varredura", pd.to_datetime(dados_ia['data']).strftime('%d/%m/%Y'))
                kpi3.metric("Área Estimada", f"{area_m2:.0f} m²")
                kpi4.metric("Potência Oculta", f"{dados_ia['fraude_kw']:.2f} kW", delta="Desvio", delta_color="inverse")
                
                st.info(f"ℹ️ O algoritmo identificou padrões visuais incompatíveis na coordenada analisada.")

            # COLUNA 2: O VELOCÍMETRO DE RISCO (Substitui a Pizza que dava problema de escala)
            with c2:
                st.subheader("⚠️ Nível de Risco Projetado")
                max_risco_mw = 500 # Define o teto do velocímetro
                
                fig_gauge = go.Figure(go.Indicator(
                    mode = "gauge+number+delta",
                    value = impacto_projecao_mw,
                    title = {'text': f"Carga Fantasma ({multiplicador}x)"},
                    delta = {'reference': 0, 'increasing': {'color': "red"}},
                    gauge = {
                        'axis': {'range': [None, max_risco_mw], 'tickwidth': 1},
                        'bar': {'color': "#ff2b2b"},
                        'bgcolor': "rgba(0,0,0,0)",
                        'steps': [
                            {'range': [0, max_risco_mw*0.3], 'color': 'rgba(0, 255, 0, 0.3)'},
                            {'range': [max_risco_mw*0.3, max_risco_mw*0.7], 'color': 'rgba(255, 165, 0, 0.3)'},
                            {'range': [max_risco_mw*0.7, max_risco_mw], 'color': 'rgba(255, 0, 0, 0.3)'}
                        ],
                        'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': impacto_projecao_mw}
                    }
                ))
                fig_gauge.update_layout(height=300, margin=dict(t=50, b=20, l=20, r=20), template="plotly_dark")
                st.plotly_chart(fig_gauge, use_container_width=True)

    else:
        st.info("ℹ️ Selecione uma Distribuidora na barra lateral para iniciar a auditoria.")

else:
    st.info("👈 Selecione os filtros e clique em 'Atualizar Dashboard' para iniciar.")