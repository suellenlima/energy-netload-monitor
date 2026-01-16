from fastapi import FastAPI, HTTPException, Request, Depends
from sqlalchemy import create_engine, text
import os
from fastapi.responses import JSONResponse
import geopandas as gpd
import pandas as pd
import numpy as np
import json

app = FastAPI(title="Energy Netload Monitor API")

# Pega a URL do banco das variáveis de ambiente
DATABASE_URL = os.getenv("DATABASE_URL")
_ENGINE = None

def get_engine():
    global _ENGINE
    if _ENGINE is None:
        if not DATABASE_URL:
            raise_server_error("DATABASE_URL não configurada.")
        _ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True)
    return _ENGINE

def get_db():
    engine = get_engine()
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()

def raise_server_error(detail: str):
    raise HTTPException(status_code=500, detail=detail)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "detail": exc.detail},
    )

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    print(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"status": "error", "detail": "Erro interno do servidor."},
    )

@app.get("/")
def read_root():
    return {"message": "Energy Monitor Online ⚡"}

@app.get("/health")
def health_check(conn = Depends(get_db)):
    """Verifica se a conexão com o banco de dados está funcionando."""
    try:
        result = conn.execute(text("SELECT 1"))
        return {"status": "ok", "db_response": result.scalar()}
    except Exception as e:
        print(f"Erro health check: {e}")
        raise_server_error("Falha ao conectar ao banco.")

@app.get("/usinas/geo")
def get_usinas_geojson(limite: int = 100, conn = Depends(get_db)):
    """
    Retorna as usinas em formato GeoJSON para colocar no mapa.
    """
    try:
        sql = f"SELECT nome, fonte, potencia_kw, geom FROM usinas_siga LIMIT {limite}"
        gdf = gpd.read_postgis(sql, conn, geom_col="geom")
        # Converte para dict sem usar eval
        return JSONResponse(content=json.loads(gdf.to_json()))
    except Exception as e:
        print(f"Erro no GeoJSON: {e}")
        raise_server_error("Falha ao gerar GeoJSON de usinas.")

@app.get("/carga/historico")
def get_carga_historico(subsistema: str = "SUDESTE", conn = Depends(get_db)):
    """Retorna a curva de carga para gráficos"""
    query = text("""
        SELECT 
            time_bucket('1 hour', time) as hora,
            AVG(carga_mw) as carga_media
        FROM carga_ons
        WHERE subsistema = :sub
        GROUP BY hora
        ORDER BY hora DESC
        LIMIT 48;
    """)
    
    try:
        result = conn.execute(query, {"sub": subsistema})
        data = [{"hora": row.hora, "mw": row.carga_media} for row in result]
        
        return data
    except Exception as e:
        print(f"Erro ao consultar carga histórica: {e}")
        raise_server_error("Falha ao consultar carga histórica.")

# --- O CÉREBRO: CÁLCULO DE CARGA OCULTA ---
@app.get("/analise/carga-oculta")
def calcular_carga_oculta(subsistema: str = "SUDESTE", distribuidora: str = None, conn = Depends(get_db)):
    sub_upper = subsistema.upper()
    
    # 1. CÁLCULO DA CAPACIDADE INSTALADA (GD)
    filtro_dist = ""
    params_cap = {}
    
    if distribuidora and distribuidora.strip():
        dist_limpa = distribuidora.strip()
        filtro_dist = "WHERE distribuidora ILIKE :dist"
        params_cap = {"dist": f"%{dist_limpa}%"}
    
    # --- CORREÇÃO AQUI: Try/Except robusto ---
    try:
        query_cap = text(f"SELECT SUM(potencia_mw) FROM gd_detalhada {filtro_dist}")
        cap_solar_mw = conn.execute(query_cap, params_cap).scalar()
    except Exception as e:
        print(f"Erro ao ler GD: {e}")
        raise_server_error("Falha ao consultar capacidade instalada (GD).")

    # Fallback (Simulação para Demo)
    if not cap_solar_mw or cap_solar_mw < 10:
        # Se for distribuidora específica (ex: CEMIG), chuta 3000MW
        # Se for região (SUDESTE), chuta 15000MW
        cap_solar_mw = 3000.0 if distribuidora else 15000.0
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
    
    try:
        result = conn.execute(query, {"sub_simple": sub_simple, "sub_like": sub_like}).fetchall()
    except Exception as e:
        print(f"Erro ao consultar carga/clima: {e}")
        raise_server_error("Falha ao consultar dados de carga e clima.")
    
    if not result:
        return []

    df = pd.DataFrame(result, columns=['hora', 'carga_ons', 'sol_wm2'])
    df['hora'] = pd.to_datetime(df['hora'])
    df = df.sort_values('hora')

    # 3. CORREÇÃO MATEMÁTICA (Para garantir a curva mesmo sem dados perfeitos de clima)
    def corrigir_sol(row):
        hora = row['hora'].hour
        # Se for dia (6h as 18h) e o sensor de clima falhou (0), usa curva seno
        if 6 <= hora <= 18 and row['sol_wm2'] < 10:
             return np.sin(np.pi * (hora - 6) / 12) * 800 
        return row['sol_wm2']
    
    df['sol_wm2_final'] = df.apply(corrigir_sol, axis=1)

    # 4. CÁLCULO FINAL
    # Geração = Capacidade * (Irradiação / 1000) * Eficiência (0.85)
    df['estimativa_solar_mw'] = cap_solar_mw * (df['sol_wm2_final'] / 1000) * 0.85
    df['estimativa_solar_mw'] = df['estimativa_solar_mw'].clip(lower=0)

    # A Carga Real = ONS + Oculta
    df['carga_real_estimada'] = df['carga_ons'] + df['estimativa_solar_mw']
    
    return df.to_dict(orient="records")

@app.get("/analise/classes-consumo")
def get_classes_consumo(distribuidora: str = None, conn = Depends(get_db)):
    
    filtro = ""
    params = {}
    if distribuidora and distribuidora.strip():
        filtro = "WHERE distribuidora ILIKE :dist"
        params = {"dist": f"%{distribuidora.strip()}%"}

    try:
        sql = text(f"""
            SELECT classe, SUM(potencia_mw) as total_mw
            FROM gd_detalhada
            {filtro}
            GROUP BY classe
            ORDER BY total_mw DESC
        """)
        
        result = conn.execute(sql, params).fetchall()
            
        return [{"classe": row.classe, "mw": round(row.total_mw, 2)} for row in result]
    except Exception as e:
        print(f"Erro ao consultar classes de consumo: {e}")
        raise_server_error("Falha ao consultar classes de consumo.")

@app.get("/analise/alertas-fraude")
def get_alertas_fraude(distribuidora: str = None, conn = Depends(get_db)):
    
    filtro_sql = ""
    params = {}
    
    if distribuidora:
        dist_limpa = distribuidora.strip()
        filtro_sql = "WHERE distribuidora ILIKE :dist"
        params = {"dist": f"%{dist_limpa}%"}
    
    try:
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
            "classe_ia": getattr(result, "classe_estimada_ia", "Não Classificado"), 
            "fraude_kw": result.diferenca_fraude_kw,
            "oficial_kw": result.potencia_oficial_kw,
            "status": result.status
        }
    except Exception as e:
        print(f"Erro alerta: {e}")
        raise_server_error("Falha ao consultar alertas de fraude.")

@app.get("/auxiliar/distribuidoras")
def get_lista_distribuidoras(conn = Depends(get_db)):
    try:
        query = text("""
            SELECT distribuidora 
            FROM gd_detalhada 
            GROUP BY distribuidora 
            ORDER BY SUM(potencia_mw) DESC
            LIMIT 50
        """)
        result = conn.execute(query).fetchall()
        lista = [row.distribuidora for row in result]
        return [""] + lista
    except Exception as e:
        print(f"Erro ao consultar distribuidoras: {e}")
        raise_server_error("Falha ao consultar distribuidoras.")