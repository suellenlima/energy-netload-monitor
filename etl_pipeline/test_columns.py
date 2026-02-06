import pandas as pd
from urllib.request import urlopen

ANEEL_MMGD_API_URL = "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv"

try:
    response = urlopen(ANEEL_MMGD_API_URL, timeout=30)
    df = pd.read_csv(response, encoding='latin-1', sep=';')
    
    print("✅ COLUNAS DISPONÍVEIS:")
    for i, col in enumerate(df.columns):
        print(f"  {i:2d}. {col}")
    
    print(f"\n📊 Amostra de dados:")
    print(df[['NomaGente', 'SigTipoGeracao']].head(3))
    
    # Procurar colunas de potência
    print(f"\n🔍 Procurando coluna de POTÊNCIA:")
    potencia_cols = [col for col in df.columns if 'pot' in col.lower() or 'mda' in col.lower()]
    print(f"   Colunas encontradas: {potencia_cols}")
    
    if potencia_cols:
        for col in potencia_cols[:1]:
            print(f"   Amostra de {col}: {df[col].head(3).values}")
    
except Exception as e:
    print(f"❌ Erro: {e}")
