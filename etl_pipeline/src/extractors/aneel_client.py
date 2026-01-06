import os
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import requests
import io

# URL Oficial do arquivo CSV (Baseado no Portal de Dados Abertos ANEEL)
# Se falhar no futuro, procure o link de "siga-empreendimentos-geracao.csv" no site dadosabertos.aneel.gov.br
SIGA_URL = "https://dadosabertos.aneel.gov.br/dataset/siga-sistema-de-informacoes-de-geracao-da-aneel/resource/11ec447d-698d-4ab8-977f-b424d5deee6a/download/siga-empreendimentos-geracao.csv"
DB_URL = os.getenv("DATABASE_URL")

def run_extraction():
    print("🚀 Iniciando extração de dados da ANEEL (SIGA)...")
    
    if not DB_URL:
        raise ValueError("DATABASE_URL não encontrada.")
    
    engine = create_engine(DB_URL)

    try:
        print(f"⬇️ Baixando arquivo de: {SIGA_URL}")
        # Baixa o conteúdo com requests para evitar erros de certificado ou redirecionamento
        response = requests.get(SIGA_URL, verify=False) # verify=False pois gov.br as vezes tem certificado estranho
        response.raise_for_status()
        
        # Lê o CSV da memória
        # O separador é ponto e vírgula e o encoding geralmente é ISO-8859-1 (Latin-1)
        df = pd.read_csv(io.BytesIO(response.content), sep=';', encoding='ISO-8859-1', decimal=',')
        
        print("✅ Download concluído. Processando colunas...")

        # Mapeamento das colunas do CSV oficial para o nosso banco
        # NOMES REAIS DO ARQUIVO: 'NomEmpreendimento', 'SigTipoGeracao', 'MdaPotenciaOutorgadaKw', etc.
        df_clean = df.rename(columns={
            'IdeNucleoCEG': 'ceg',
            'NomEmpreendimento': 'nome',
            'SigTipoGeracao': 'fonte',          # UHE, EOL, UFV (Solar)
            'DscOrigemCombustivel': 'combustivel', 
            'MdaPotenciaOutorgadaKw': 'potencia_kw',
            'NumCoordNEmpreendimento': 'latitude', # Atenção aqui (Norte/Sul)
            'NumCoordEEmpreendimento': 'longitude' # Leste/Oeste
        })

        # Filtrar apenas o essencial e remover linhas sem coordenadas
        colunas_finais = ['ceg', 'nome', 'fonte', 'combustivel', 'potencia_kw', 'latitude', 'longitude']
        df_clean = df_clean[colunas_finais].dropna(subset=['latitude', 'longitude', 'potencia_kw'])

        # Converter colunas numéricas (garantia)
        df_clean['potencia_kw'] = pd.to_numeric(df_clean['potencia_kw'], errors='coerce')
        df_clean['latitude'] = pd.to_numeric(df_clean['latitude'], errors='coerce')
        df_clean['longitude'] = pd.to_numeric(df_clean['longitude'], errors='coerce')

        # Criação da Geometria
        gdf = gpd.GeoDataFrame(
            df_clean,
            geometry=gpd.points_from_xy(df_clean.longitude, df_clean.latitude),
            crs="EPSG:4326"
        )

        print(f"💾 Salvando {len(gdf)} usinas no PostGIS...")
        gdf.to_postgis("usinas_siga", engine, if_exists='replace', index=False)
        
        print("✅ Sucesso! Banco populado.")

    except Exception as e:
        print(f"❌ Erro Crítico: {e}")

if __name__ == "__main__":
    run_extraction()