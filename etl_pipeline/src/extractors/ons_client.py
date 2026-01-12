import pandas as pd
from sqlalchemy import create_engine, text
import os
import requests
import io
from datetime import datetime

# Configurações
CKAN_API_URL = "https://dados.ons.org.br/api/3/action/package_show?id=carga-energia"
DB_URL = os.getenv("DATABASE_URL")

def get_dynamic_url():
    """Consulta API do ONS para achar a URL do CSV mais recente"""
    print("🔎 Consultando API do ONS...")
    try:
        response = requests.get(CKAN_API_URL, timeout=20)
        data = response.json()
        resources = data['result']['resources']
        ano_atual = str(datetime.now().year)
        ano_anterior = str(datetime.now().year - 1)
        
        # Tenta pegar 2026, se não tiver, pega 2025
        for res in resources:
            if ano_atual in res['name'] and res['format'].upper() == 'CSV':
                return res['url']
        for res in resources:
            if ano_anterior in res['name'] and res['format'].upper() == 'CSV':
                return res['url']
        return None
    except Exception as e:
        print(f"⚠️ Erro na API: {e}")
        return None

def encontrar_coluna_carga(df):
    """
    Procura a coluna de carga de forma inteligente, 
    sem depender do nome exato que muda toda hora.
    """
    colunas = df.columns
    # 1. Tenta nomes conhecidos exatos
    candidatos = [
        'val_cargaenergiamw', 'val_cargaenergiamediamw', 
        'val_cargaeneergiamwmed', 'val_cargaenergiammwmed', 'val_cargaenergiamwmed'
    ]
    for c in candidatos:
        if c in colunas:
            return c
            
    # 2. Se falhar, procura qualquer coluna que comece com 'val_carga' e termine com 'mw' ou 'med'
    for col in colunas:
        if col.startswith('val_carga'):
            print(f"🧐 Coluna de carga detectada dinamicamente: {col}")
            return col
            
    return None

def run_extraction():
    print("⚡ Iniciando extração ONS (Versão Blindada)...")
    
    if not DB_URL:
        raise ValueError("DATABASE_URL não configurada.")
    engine = create_engine(DB_URL)

    target_url = get_dynamic_url()
    if not target_url:
        print("❌ Nenhum link encontrado.")
        return

    try:
        print(f"⬇️ Baixando: {target_url}")
        response = requests.get(target_url, timeout=60)
        
        df = pd.read_csv(io.BytesIO(response.content), sep=';', decimal=',')
        
        # Padroniza nomes básicos
        df = df.rename(columns={
            'din_instante': 'time', 'nom_subsistema': 'subsistema',
            'subsistema': 'subsistema', 'time': 'time'
        })
        
        # --- AQUI ESTA A MAGIA ---
        col_carga_original = encontrar_coluna_carga(df)
        
        if not col_carga_original:
            print(f"⚠️ IMPOSSÍVEL achar a coluna de carga. Colunas no arquivo: {list(df.columns)}")
            return

        # Renomeia a coluna encontrada para o padrão 'carga_mw'
        df = df.rename(columns={col_carga_original: 'carga_mw'})

        # Limpeza final
        df['time'] = pd.to_datetime(df['time'])
        df['carga_mw'] = pd.to_numeric(df['carga_mw'], errors='coerce')
        df_final = df[['time', 'subsistema', 'carga_mw']].dropna()
        df_final = df_final.drop_duplicates(subset=['time', 'subsistema'])

        if not df_final.empty:
            min_time = df_final['time'].min()
            max_time = df_final['time'].max()
            subsistemas = df_final['subsistema'].dropna().unique().tolist()
            if subsistemas:
                delete_query = text("""
                    DELETE FROM carga_ons
                    WHERE time >= :start AND time <= :end
                    AND subsistema = ANY(:subs)
                """)
                with engine.begin() as conn:
                    conn.execute(delete_query, {"start": min_time, "end": max_time, "subs": subsistemas})

        print(f"💾 Inserindo {len(df_final)} registros no Banco...")
        df_final.to_sql('carga_ons', engine, if_exists='append', index=False, method='multi', chunksize=10000)
        print("✅ Sucesso! Dados do ONS importados.")

    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    run_extraction()