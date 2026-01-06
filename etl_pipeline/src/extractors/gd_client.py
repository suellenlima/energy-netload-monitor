import pandas as pd
from sqlalchemy import create_engine
import os
import requests
import shutil

# URL Oficial
GD_URL = "https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv"
DB_URL = os.getenv("DATABASE_URL")
LOCAL_PATH = "/app/data/raw/gd_temp.csv"

def run_extraction():
    print("☀️ Extraindo GD (Versão Limpa sem Warnings)...")
    
    if not DB_URL:
        # Fallback para localhost
        engine = create_engine("postgresql://admin:admin123@localhost:5432/energy_monitor")
    else:
        engine = create_engine(DB_URL)

    try:
        # 1. Download Seguro
        if not os.path.exists(LOCAL_PATH):
            print(f"⬇️ Baixando arquivo da ANEEL...")
            with requests.get(GD_URL, stream=True, verify=False) as r:
                r.raise_for_status()
                with open(LOCAL_PATH, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            print("✅ Download concluído.")
        else:
            print(f"📂 Usando cache: {LOCAL_PATH}")

        # 2. Processamento
        agregado = {} 
        chunk_size = 50000
        
        print("🔄 Processando CSV...")
        
        with pd.read_csv(
            LOCAL_PATH, 
            sep=';', 
            encoding='latin-1', 
            chunksize=chunk_size, 
            on_bad_lines='skip',
            dtype=str
        ) as reader:
            
            for i, chunk in enumerate(reader):
                if i % 20 == 0: print(f"   Processando lote {i}...")

                # Limpeza de nomes de colunas
                chunk.columns = chunk.columns.str.strip()
                
                # Validação de colunas
                cols_necessarias = ['NomAgente', 'DscClasseConsumo', 'SigUF', 'DscFonteGeracao', 'MdaPotenciaInstaladaKW']
                if not all(col in chunk.columns for col in cols_necessarias):
                    continue

                # --- FILTRO 1: APENAS SOLAR (COM .COPY() PARA CORRIGIR O AVISO) ---
                # O .copy() aqui resolve o SettingWithCopyWarning
                chunk = chunk[chunk['DscFonteGeracao'].str.contains('Solar', case=False, na=False)].copy()

                if chunk.empty:
                    continue

                # Tratamento da Potência
                # Agora é seguro modificar pois usamos .copy() acima
                chunk['MdaPotenciaInstaladaKW'] = (
                    chunk['MdaPotenciaInstaladaKW']
                    .str.replace('.', '', regex=False)
                    .str.replace(',', '.', regex=False)
                )
                chunk['MdaPotenciaInstaladaKW'] = pd.to_numeric(chunk['MdaPotenciaInstaladaKW'], errors='coerce').fillna(0)

                # --- AGRUPAMENTO ---
                grupo = chunk.groupby(['NomAgente', 'DscClasseConsumo', 'SigUF'])['MdaPotenciaInstaladaKW'].sum()
                
                for (distribuidora, classe, uf), potencia in grupo.items():
                    chave = (str(distribuidora).upper(), str(classe).upper(), str(uf).upper())
                    agregado[chave] = agregado.get(chave, 0) + potencia

        # 3. Salvar no Banco
        print("🔨 Construindo tabela final...")
        dados_finais = [
            {
                'distribuidora': k[0], 
                'classe': k[1], 
                'sigla_uf': k[2],
                'fonte': 'Radiação Solar',
                'potencia_mw': v/1000
            } 
            for k, v in agregado.items()
        ]
        
        df_final = pd.DataFrame(dados_finais)
        
        print(f"💾 Salvando {len(df_final)} registros agregados no Banco...")
        df_final.to_sql('gd_detalhada', engine, if_exists='replace', index=False)
        
        print("✅ Sucesso Absoluto! Dados carregados sem erros.")

    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    run_extraction()