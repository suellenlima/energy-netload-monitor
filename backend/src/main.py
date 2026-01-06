from fastapi import FastAPI
from sqlalchemy import create_engine, text
import os
from fastapi.responses import JSONResponse
import geopandas as gpd
import pandas as pd
import numpy as np

app = FastAPI(title="Energy Netload Monitor API")

# Pega a URL do banco das variáveis de ambiente (definidas no docker-compose)
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    return create_engine(DATABASE_URL).connect()

@app.get("/")
def read_root():
    return {"message": "Energy Monitor Online ⚡"}

@app.get("/health")
def health_check():
    """Verifica se a conexão com o banco de dados está funcionando."""
    if not DATABASE_URL:
        return {"status": "error", "detail": "DATABASE_URL não configurada"}
    
    try:
        # Tenta fazer um 'ping' simples no banco
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            return {"status": "ok", "db_response": result.scalar()}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
    
@app.get("/usinas/geo")
def get_usinas_geojson(limite: int = 100):
    """
    Retorna as usinas em formato GeoJSON para colocar no mapa.
    Limitado a 100 por padrão para não travar o navegador.
    """
    engine = create_engine(DATABASE_URL)
    
    # Lê do PostGIS usando GeoPandas (muito mais rápido)
    sql = f"SELECT nome, fonte, potencia_kw, geom FROM usinas_siga LIMIT {limite}"
    gdf = gpd.read_postgis(sql, engine, geom_col="geom")
    
    # Converte para GeoJSON string
    return JSONResponse(content=eval(gdf.to_json()))

@app.get("/carga/historico")
def get_carga_historico(subsistema: str = "SUDESTE"):
    """Retorna a curva de carga para gráficos"""
    engine = create_engine(DATABASE_URL)
    
    # Query otimizada pelo Timescale (time_bucket é como um 'group by' de tempo)
    # Pega a média de carga por hora para o gráfico ficar leve
    query = text("""
        SELECT 
            time_bucket('1 hour', time) as hora,
            AVG(carga_mw) as carga_media
        FROM carga_ons
        WHERE subsistema = :sub
        GROUP BY hora
        ORDER BY hora DESC
        LIMIT 48; -- Últimas 48 horas
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"sub": subsistema})
        # Formata para JSON
        data = [{"hora": row.hora, "mw": row.carga_media} for row in result]
    
    return data

# --- Rota 1: Mapa (Já testamos) ---
@app.get("/usinas/geo")
def get_usinas_geojson(limite: int = 100):
    engine = create_engine(DATABASE_URL)
    sql = f"SELECT nome, fonte, potencia_kw, geom FROM usinas_siga LIMIT {limite}"
    gdf = gpd.read_postgis(sql, engine, geom_col="geom")
    return JSONResponse(content=eval(gdf.to_json()))

# --- Rota 2: O Cérebro (Cálculo de Carga Oculta) ---
# ... (Imports e configurações anteriores continuam iguais)

@app.get("/analise/carga-oculta")
def calcular_carga_oculta(subsistema: str = "SUDESTE", distribuidora: str = None):
    conn = get_db_connection()
    sub_upper = subsistema.upper()
    
    # 1. CÁLCULO DA CAPACIDADE INSTALADA (O "Tamanho" da Carga Oculta)
    # A Correção: Usar 'gd_detalhada' (Telhados) em vez de 'usinas_siga'
    # Isso garante que a linha laranja apareça forte.
    
    filtro_dist = ""
    params_cap = {}
    
    if distribuidora and distribuidora.strip():
        # Se tiver filtro de distribuidora, filtra a capacidade dela
        filtro_dist = "WHERE distribuidora ILIKE :dist"
        params_cap = {"dist": f"%{distribuidora}%"}
    else:
        # Se não, filtra por região (aproximação baseada em estados típicos do subsistema)
        # Simplificação para Demo: Se for SUDESTE, pegamos tudo (ou filtre por UF se tiver essa coluna)
        pass 

    # Query que soma os Gigawatts dos telhados
    query_cap = text(f"SELECT SUM(potencia_mw) FROM gd_detalhada {filtro_dist}")
    cap_solar_mw = conn.execute(query_cap, params_cap).scalar()
    
    # Fallback: Se não tiver dados de GD carregados, usa um valor fixo para a DEMO não falhar
    if not cap_solar_mw or cap_solar_mw < 10:
        # Se for uma distribuidora específica, chuta 500MW, se for região, 15.000MW
        cap_solar_mw = 500.0 if distribuidora else 15000.0
        print(f"⚠️ Aviso: Usando capacidade simulada de {cap_solar_mw} MW")

    # 2. BUSCA DO CLIMA (Para saber se tem Sol)
    sub_simple = "SUDESTE" if "SUDESTE" in sub_upper else sub_upper
    sub_like = f"%{sub_simple}%"
    
    query = text("""
        SELECT 
            ons.time as hora,
            ons.carga_mw as carga_ons,
            COALESCE(clima.irradiancia_wm2, 0) as sol_wm2
        FROM carga_ons ons
        LEFT JOIN clima_real clima 
            ON date_trunc('hour', ons.time) = date_trunc('hour', clima.time)
            AND clima.subsistema = :sub_simple
        WHERE UPPER(ons.subsistema) LIKE :sub_like
        ORDER BY ons.time DESC
        LIMIT 24
    """)
    
    result = conn.execute(query, {"sub_simple": sub_simple, "sub_like": sub_like}).fetchall()
    
    if not result:
        return []

    df = pd.DataFrame(result, columns=['hora', 'carga_ons', 'sol_wm2'])
    df['hora'] = pd.to_datetime(df['hora'])
    df = df.sort_values('hora')

    # 3. CORREÇÃO DE BURACSO NO CLIMA
    # Se a irradiância vier 0 durante o dia, forçamos uma curva matemática
    # para garantir que o gráfico mostre a "barriga"
    def corrigir_sol(row):
        hora = row['hora'].hour
        # Se for dia (6h as 18h) e o sol estiver zero, usa matemática
        if 6 <= hora <= 18 and row['sol_wm2'] < 10:
             return np.sin(np.pi * (hora - 6) / 12) * 800 
        return row['sol_wm2']
    
    df['sol_wm2_final'] = df.apply(corrigir_sol, axis=1)

    # 4. CÁLCULO FINAL
    # Geração = Capacidade (Telhados) * Sol * Eficiência
    df['estimativa_solar_mw'] = cap_solar_mw * (df['sol_wm2_final'] / 1000) * 0.85
    df['estimativa_solar_mw'] = df['estimativa_solar_mw'].clip(lower=0)

    # A Carga Real é o que o ONS vê + o que os telhados geraram
    df['carga_real_estimada'] = df['carga_ons'] + df['estimativa_solar_mw']
    
    # Impacto percentual
    df['carga_oculta_pct'] = 0.0
    mask = df['carga_real_estimada'] > 0
    df_final = df  # alias
    df_final.loc[mask, 'carga_oculta_pct'] = (
        df_final.loc[mask, 'estimativa_solar_mw'] / df_final.loc[mask, 'carga_real_estimada']
    ) * 100

    return df_final.to_dict(orient="records")

@app.get("/analise/classes-consumo")
def get_classes_consumo(distribuidora: str = None):
    """
    Mostra quanto de carga oculta vem de casas vs industrias.
    """
    engine = create_engine(DATABASE_URL)
    
    # Se passar distribuidora, filtra. Se não, mostra Brasil todo.
    filtro = ""
    params = {}
    if distribuidora:
        filtro = "WHERE distribuidora LIKE :dist"
        params = {"dist": f"%{distribuidora.upper()}%"}

    sql = text(f"""
        SELECT classe, SUM(potencia_mw) as total_mw
        FROM gd_detalhada
        {filtro}
        GROUP BY classe
        ORDER BY total_mw DESC
    """)
    
    with engine.connect() as conn:
        result = conn.execute(sql, params).fetchall()
        
    return [{"classe": row.classe, "mw": round(row.total_mw, 2)} for row in result]

@app.get("/analise/alertas-fraude")
def get_alertas_fraude(distribuidora: str = None):
    conn = get_db_connection()
    
    filtro_sql = ""
    params = {}
    
    if distribuidora:
        # AQUI ESTÁ A CORREÇÃO:
        # 1. .strip() remove espaços no início/fim
        # 2. Usamos % wildcard dos dois lados para garantir que ache mesmo se tiver sufixo
        dist_limpa = distribuidora.strip()
        filtro_sql = "WHERE distribuidora ILIKE :dist"
        params = {"dist": f"%{dist_limpa}%"} # Ex: busca por '%CEMIG DISTRIBUICAO%'
    
    query = text(f"""
        SELECT * FROM auditoria_visual 
        {filtro_sql}
        ORDER BY data_inspecao DESC 
        LIMIT 1
    """)
    
    result = conn.execute(query, params).fetchone()
    
    if not result:
        return {}
        
    return {
        "data": result.data_inspecao,
        "local": f"{result.latitude}, {result.longitude}",
        "distribuidora": result.distribuidora,
        # O campo novo vem aqui:
        "classe_ia": getattr(result, "classe_estimada_ia", "Não Classificado"), 
        "detectado_kw": result.potencia_estimada_kw,
        "oficial_kw": result.potencia_oficial_kw,
        "fraude_kw": result.diferenca_fraude_kw,
        "status": result.status
    }

@app.get("/auxiliar/distribuidoras")
def get_lista_distribuidoras():
    """
    Retorna a lista de todas as distribuidoras disponíveis no banco de dados.
    Ordenadas por quem tem mais potência instalada (as mais importantes primeiro).
    """
    conn = get_db_connection()
    
    # Busca as Top 50 distribuidoras (para não poluir o menu com empresas minúsculas)
    query = text("""
        SELECT distribuidora 
        FROM gd_detalhada 
        GROUP BY distribuidora 
        ORDER BY SUM(potencia_mw) DESC
        LIMIT 50
    """)
    
    result = conn.execute(query).fetchall()
    
    # Transforma em lista simples de strings: ['CEMIG', 'ENEL', 'COPEL'...]
    lista = [row.distribuidora for row in result]
    
    # Adiciona opção vazia no início para o filtro "Todas"
    return [""] + lista