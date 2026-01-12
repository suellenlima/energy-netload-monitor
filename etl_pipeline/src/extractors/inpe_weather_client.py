import pandas as pd
from sqlalchemy import create_engine, text
import requests
import os
from datetime import datetime, timedelta

# Configuração
DB_URL = os.getenv("DATABASE_URL")
OFFSET_ANOS = 2  # Diferença entre a Simulação (2026) e os Dados Reais (2024)
DIAS_ATRAS = 7   # Quantos dias de histórico queremos atualizar a cada execução

# Coordenadas (Centróides Regionais)
REGIOES = {
    "SUDESTE": {"lat": -23.55, "lon": -46.63},
    "SUL":     {"lat": -30.03, "lon": -51.22},
    "NORDESTE":{"lat": -8.04,  "lon": -34.87},
    "NORTE":   {"lat": -1.45,  "lon": -48.50}
}

def create_table_if_not_exists(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS clima_real (
                time TIMESTAMPTZ NOT NULL,
                subsistema VARCHAR(20),
                irradiancia_wm2 FLOAT,
                temperatura_c FLOAT,
                CONSTRAINT clima_real_unique UNIQUE (time, subsistema)
            );
        """))
        try:
            conn.execute(text("SELECT create_hypertable('clima_real', 'time', if_not_exists => TRUE);"))
        except:
            pass
        conn.commit()

def run_extraction():
    print("☁️ Extração de Clima Dinâmica (Janela Móvel)...")
    
    if not DB_URL: raise ValueError("DATABASE_URL off.")
    engine = create_engine(DB_URL)
    create_table_if_not_exists(engine)

    # --- Lógica Dinâmica de Datas ---
    agora = datetime.now()
    
    # 1. Definimos o intervalo no "Tempo da Simulação" (2026)
    data_fim_sim = agora
    data_inicio_sim = agora - timedelta(days=DIAS_ATRAS)
    
    # 2. Traduzimos para o "Tempo Real da API" (2024)
    # A API precisa receber strings YYYY-MM-DD
    real_start_date = (data_inicio_sim.replace(year=data_inicio_sim.year - OFFSET_ANOS)).strftime('%Y-%m-%d')
    real_end_date = (data_fim_sim.replace(year=data_fim_sim.year - OFFSET_ANOS)).strftime('%Y-%m-%d')
    
    print(f"📅 Janela Dinâmica: Buscando dados de {real_start_date} a {real_end_date}")
    print(f"✨ Transformando para: {data_inicio_sim.strftime('%Y-%m-%d')} a {data_fim_sim.strftime('%Y-%m-%d')}")

    for nome_sub, coords in REGIOES.items():
        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "start_date": real_start_date, # Data Dinâmica
                "end_date": real_end_date,     # Data Dinâmica
                "hourly": "shortwave_radiation,temperature_2m",
                "timezone": "America/Sao_Paulo"
            }
            
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            if "hourly" not in data:
                print(f"⚠️ Sem dados para {nome_sub}")
                continue

            df = pd.DataFrame({
                "time": data["hourly"]["time"],
                "irradiancia_wm2": data["hourly"]["shortwave_radiation"],
                "temperatura_c": data["hourly"]["temperature_2m"]
            })
            
            # --- O TRUQUE DE VOLTA PARA O FUTURO ---
            # Convertemos string para data
            df["time"] = pd.to_datetime(df["time"])
            
            # Adicionamos os 2 anos de volta para ficar compatível com o ONS (2026)
            df["time"] = df["time"].apply(lambda x: x.replace(year=x.year + OFFSET_ANOS))
            
            df["subsistema"] = nome_sub
            
            # Limpeza e Upsert
            df_final = df[["time", "subsistema", "irradiancia_wm2", "temperatura_c"]].dropna()
            
            # Usamos ON CONFLICT DO UPDATE (ou ignoramos duplicatas com INSERT simples + UNIQUE)
            # Para simplificar no Pandas, usamos o método 'append'. 
            # Como definimos uma UNIQUE CONSTRAINT no banco, precisamos lidar com duplicatas.
            # O jeito mais 'preguiçoso' e eficaz aqui é deletar o período e reinserir, 
            # mas vamos tentar inserção direta ignorando erros de chave duplicada seria complexo com to_sql puro.
            # Vamos inserir em chunks e deixar o banco rejeitar duplicatas silenciosamente se configurado,
            # ou limpar a tabela temporariamente (para MVP).
            
            # Estratégia MVP Robusta: Deletar dados desse período específico antes de inserir novos
            # Isso garante que se o clima mudou (correção de dados), a gente atualiza.
            with engine.connect() as conn:
                delete_query = text("""
                    DELETE FROM clima_real 
                    WHERE subsistema = :sub 
                    AND time >= :inicio AND time <= :fim
                """)
                conn.execute(delete_query, {
                    "sub": nome_sub,
                    "inicio": data_inicio_sim,
                    "fim": data_fim_sim
                })
                conn.commit()

            df_final.to_sql("clima_real", engine, if_exists="append", index=False, method="multi")
            print(f"   ✅ {nome_sub}: {len(df_final)} registros atualizados.")

        except Exception as e:
            print(f"❌ Erro {nome_sub}: {e}")

if __name__ == "__main__":
    run_extraction()