import pandas as pd
from sqlalchemy import create_engine
import os
import requests
import shutil

# URL Oficial
GD_URL = "https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv"
DB_URL = os.getenv("DATABASE_URL")
LOCAL_PATH = "/app/data/raw/gd_temp.csv" # Arquivo temporário

def run_extraction():
    print("☀️ Extraindo GD (Modo Seguro de Encoding)...")
    
    if not DB_URL:
        raise ValueError("DATABASE_URL não configurada.")
    engine = create_engine(DB_URL)

    try:
        # 1. Download Seguro (Salvar em disco antes de ler)
        # Isso evita erros de conexão e permite testar o encoding com calma
        if not os.path.exists(LOCAL_PATH):
            print(f"⬇️ Baixando arquivo gigante para {LOCAL_PATH}...")
            with requests.get(GD_URL, stream=True, verify=False) as r:
                r.raise_for_status()
                with open(LOCAL_PATH, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            print("✅ Download concluído.")
        else:
            print(f"📂 Usando arquivo em cache: {LOCAL_PATH}")

        # 2. Processamento
        # Dicionário acumulador: (Distribuidora, Classe) -> Potência
        agregado = {}
        chunk_size = 50000
        
        print("🔄 Lendo CSV com encoding 'latin-1'...")
        
        # O SEGREDO ESTÁ AQUI: encoding='latin-1' resolve o problema do 'ç'
        with pd.read_csv(
            LOCAL_PATH, 
            sep=';', 
            encoding='latin-1',  # <--- A CORREÇÃO CRÍTICA
            chunksize=chunk_size, 
            on_bad_lines='skip',
            dtype=str # Ler tudo como texto primeiro para evitar erros de conversão
        ) as reader:
            
            for i, chunk in enumerate(reader):
                if i % 10 == 0: print(f"   Processando lote {i}...")

                # Verificar colunas necessárias
                # O arquivo pode ter nomes variados, vamos tentar normalizar
                if 'NomAgente' not in chunk.columns or 'DscClasseConsumo' not in chunk.columns:
                    # Tenta limpar espaços em branco dos nomes das colunas
                    chunk.columns = chunk.columns.str.strip()
                
                if 'NomAgente' not in chunk.columns:
                    continue

                # Tratamento da Potência (Brasil usa vírgula como decimal)
                # Ex: "3,5" vira 3.5
                chunk['MdaPotenciaInstaladaKW'] = (
                    chunk['MdaPotenciaInstaladaKW']
                    .str.replace('.', '', regex=False) # Remove ponto de milhar se houver
                    .str.replace(',', '.', regex=False) # Troca vírgula por ponto
                )
                chunk['MdaPotenciaInstaladaKW'] = pd.to_numeric(chunk['MdaPotenciaInstaladaKW'], errors='coerce').fillna(0)

                # Agrupamento
                grupo = chunk.groupby(['NomAgente', 'DscClasseConsumo'])['MdaPotenciaInstaladaKW'].sum()
                
                for (distribuidora, classe), potencia in grupo.items():
                    # Normaliza nomes (Maiúsculo)
                    chave = (str(distribuidora).upper(), str(classe).upper())
                    agregado[chave] = agregado.get(chave, 0) + potencia

        # 3. Salvar no Banco
        dados_finais = [
            {'distribuidora': k[0], 'classe': k[1], 'potencia_mw': v/1000} 
            for k, v in agregado.items()
        ]
        df_final = pd.DataFrame(dados_finais)
        
        print(f"💾 Salvando resumo no Banco ({len(df_final)} registros)...")
        df_final.to_sql('gd_detalhada', engine, if_exists='replace', index=False)
        
        print("✅ Sucesso Absoluto! Classificação concluída.")

    except Exception as e:
        print(f"❌ Erro: {e}")
        # Se der erro, remova o arquivo para tentar baixar de novo na próxima
        if os.path.exists(LOCAL_PATH):
            os.remove(LOCAL_PATH)

if __name__ == "__main__":
    run_extraction()