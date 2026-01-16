
import streamlit as st

from components.alerts import fetch_alerta, render_alerta
from components.audit import render_auditoria
from components.charts import load_carga_data, render_carga_section, render_classes_consumo
from components.sidebar import render_sidebar
from config import API_URL, APP_TITLE, LAYOUT
from services.api_client import ApiClient


st.set_page_config(page_title=APP_TITLE, layout=LAYOUT)
st.title("Monitoramento Avançado de Carga Líquida")

client = ApiClient(API_URL)
state = render_sidebar(client)

dados_ia = fetch_alerta(client, state.distribuidora)
_, impacto_projecao_mw = render_alerta(dados_ia, state.multiplicador)

if state.refresh:
    df_carga = load_carga_data(client, state.subsistema, state.distribuidora)
    render_carga_section(df_carga, impacto_projecao_mw, state.multiplicador, state.subsistema)
    render_classes_consumo(client, state.distribuidora)
    render_auditoria(dados_ia, impacto_projecao_mw, state.multiplicador)
else:
    st.info("Selecione os filtros e clique em 'Atualizar Dashboard' para iniciar.")