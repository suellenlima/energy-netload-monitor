import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
from datetime import datetime, timedelta
import os

# Conecta no Banco
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    # Fallback para rodar localmente se precisar (ajuste user/pass se for o caso)
    DB_URL = "postgresql://admin:admin123@localhost:5432/energy_monitor"

engine = create_engine(DB_URL)

def gerar_dados_fake_realistas():
    print("🛠️ Gerando dados horários para demonstração...")
    
    # Vamos gerar dados para os últimos 3 dias até agora
    agora = datetime.now()
    inicio = agora - timedelta(days=3)
    
    # Cria uma lista de horas
    datas = pd.date_range(start=inicio, end=agora, freq='h')
    
    dados = []
    for data in datas:
        # Simula uma curva de carga típica (mais alta as 18h, mais baixa as 3h)
        hora = data.hour
        # Base: 35.000 MW + Variação do dia
        carga_base = 35000 
        
        # Fator Horário (Curva típica de consumo residencial)
        if 0 <= hora < 6:
            fator = 0.8  # Madrugada (baixo)
        elif 6 <= hora < 12:
            fator = 1.0  # Manhã (médio)
        elif 12 <= hora < 18:
            fator = 1.1  # Tarde (alto - ar condicionado)
        else:
            fator = 1.2  # Pico noturno (18h-21h)
            
        # Adiciona um ruído aleatório para parecer real
        ruido = np.random.uniform(-500, 500)
        
        carga_final = (carga_base * fator) + ruido
        
        dados.append({
            'time': data,
            'subsistema': 'SUDESTE/CENTRO-OESTE', # Nome oficial correto
            'carga_mw': carga_final
        })

    df = pd.DataFrame(dados)

    if not df.empty:
        delete_query = text("""
            DELETE FROM carga_ons
            WHERE time >= :inicio AND time <= :fim
            AND subsistema = :sub
        """)
        with engine.begin() as conn:
            conn.execute(delete_query, {
                "inicio": inicio,
                "fim": agora,
                "sub": "SUDESTE/CENTRO-OESTE"
            })
    
    # Salva no banco (Append)
    print(f"💾 Inserindo {len(df)} registros horários...")
    df.to_sql('carga_ons', engine, if_exists='append', index=False)
    print("✅ Sucesso! Agora o gráfico vai ter resolução horária.")

if __name__ == "__main__":
    gerar_dados_fake_realistas()