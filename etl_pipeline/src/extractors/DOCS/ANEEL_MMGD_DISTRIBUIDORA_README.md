# 🔋 ETL de MMGD por Distribuidora - ANEEL

## 📊 O que faz?

Busca dados **REAIS diários** de Geração Distribuída (MMGD) **POR DISTRIBUIDORA** da ANEEL e popula o banco de dados.

**Dados fornecidos:**
- ✅ Potência instalada TOTAL de MMGD por distribuidora (em MW)
- ✅ Potência SOLAR especificamente (Solar + Eólica + Hidro + Biomassa)
- ✅ Quantidade de empreendimentos por distribuidora
- ✅ Atualização: Diária
- ✅ Cobertura: LIGHT, ENEL, CPFL, CEMIG, AES, ENERGISA, EDP, EQUATORIAL, RGE, COPEL, CEEE e outras
- ✅ Dados históricos: 30 dias mantidos

## 🚀 Como Usar

### Opção 1: Rodar manualmente

```bash
# Entrar no container
docker compose exec etl bash

# Executar ETL para buscar dados de MMGD por distribuidora
python src/extractors/aneel_mmgd_distribuidora.py

# Ou do host (sem entrar no container)
docker compose exec etl python src/extractors/aneel_mmgd_distribuidora.py
```

### Opção 2: Usar em Python/código

```python
from etl_pipeline.src.extractors.aneel_mmgd_distribuidora import executar_etl_mmgd_distribuidora

# Buscar dados de todas as distribuidoras
executar_etl_mmgd_distribuidora()
```

### Opção 3: Scheduler (Airflow/APScheduler)

Adicione ao seu scheduler para rodar diariamente (após 12h ou 19h quando ANEEL atualiza):

```python
# Schedule para rodar diariamente
schedule.every().day.at("13:00").do(executar_etl_mmgd_distribuidora)
schedule.every().day.at("20:00").do(executar_etl_mmgd_distribuidora)
```

## 📍 Distribuidoras Cobertas

| Distribuidora | Subsistema | Estado |
|---|---|---|
| LIGHT | Sudeste/Centro-Oeste | RJ |
| ENEL | Sudeste/Centro-Oeste | SP/RJ |
| CPFL | Sudeste/Centro-Oeste | SP |
| CEMIG | Sudeste/Centro-Oeste | MG |
| ENERGISA | Nordeste/Norte | AC/AL/MA/PB/PI |
| EQUATORIAL | Nordeste/Norte | PA/AP |
| EDP | Sudeste/Centro-Oeste | SP |
| AES | Sudeste/Centro-Oeste | SP |
| RGE | Sul | RS |
| COPEL | Sul | PR |
| CEEE | Sul | RS |

*Adicione mais conforme necessário no código*

## 💾 Dados Armazenados

### Tabela: `geracao_mmgd_distribuidora`

```sql
CREATE TABLE geracao_mmgd_distribuidora (
    id BIGSERIAL PRIMARY KEY,
    distribuidora TEXT NOT NULL,              -- 'LIGHT', 'ENEL', 'CPFL', etc
    subsistema TEXT,                          -- 'Sudeste/Centro-Oeste', 'Nordeste', etc
    fonte_geracao TEXT,                       -- 'Solar', 'Eólica', 'Hidro', 'Biomassa'
    potencia_total_kw FLOAT8 NOT NULL,        -- Potência instalada (kW)
    quantidade_empreendimentos INT,           -- Número de empreendimentos
    data_medicao TIMESTAMP WITH TIME ZONE,    -- Data da medição
    data_insercao TIMESTAMP WITH TIME ZONE,   -- Quando foi inserido
    
    UNIQUE (distribuidora, fonte_geracao, data_medicao)
);
```

## 🔗 Integração com KPIs

O KPI "Geração MMGD Oficial" agora:

1. **Busca dados REAIS por distribuidora** da tabela `geracao_mmgd_distribuidora`
2. **Calcula geração atual** baseado em padrão solar (potência × fator horário)
3. **Compara com carga ONS** da tabela `carga_distribuidoras`
4. **Se não achar dados REAIS**, usa fallback com dados sintéticos

## 📈 Exemplo de Resposta

### Dados após ETL rodar:

```sql
SELECT * FROM geracao_mmgd_distribuidora 
WHERE distribuidora = 'LIGHT' 
ORDER BY data_medicao DESC;

distribuidora | subsistema | fonte_geracao | potencia_total_kw | quantidade_empreendimentos | data_medicao
LIGHT         | Sudeste/CO | Solar         | 85,500,000        | 24,580                    | 2026-02-05
LIGHT         | Sudeste/CO | Eólica        | 2,100,000         | 12                        | 2026-02-05
```

### Na API do KPI:

```json
{
    "hora": "2026-02-05T12:00:00",
    "distribuidora": "LIGHT",
    "carga_ons": 177.82,
    "estimativa_solar_mw": 81.2,
    "consumo_estimado_mw": 259.02,
    "carga_real_estimada": 259.02,
    "percentual_total": 95.0
}
```

## ⚠️ Notas Importantes

- **Primeira execução**: Pode levar alguns minutos para baixar dados da ANEEL
- **Timeout**: Se o download falhar em 30s, o ETL para com erro
- **Limpeza**: Mantém apenas últimos 30 dias de dados
- **Atualização ANEEL**: A ANEEL atualiza dados diariamente (melhor executar após 12h ou 19h)
- **Dados granulares**: Separados por tipo de fonte (Solar, Eólica, Hidro, Biomassa)

## 🔧 Troubleshooting

### "Nenhum dado retornado"

1. Verifique se consegue acessar: `https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida`
2. Verifique a URL do CSV em: https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida
3. A ANEEL pode ter mudado o URL ou formato

### "Coluna não encontrada"

- A ANEEL atualizou a estrutura dos dados
- Verifique quais colunas estão disponíveis no CSV baixado
- Atualize o mapeamento de colunas no código

### "Erro de conexão com banco"

```bash
docker compose ps  # Verificar se postgres está rodando
docker compose logs db  # Ver logs do banco
```

### "Distribuidora não encontrada"

- A distribuidora pode não ter dados de MMGD ou pode estar com outro nome
- Verifique o CSV baixado e adicione/corrija o nome no código

## 📚 Referências

- **Dataset ANEEL**: https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida
- **Download direto**: https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida/download/empreendimentos_geracao_distribuida.csv
- **Portal CKAN ANEEL**: https://dadosabertos.aneel.gov.br

## 📝 Fluxo de Dados

```
ANEEL (CSV) 
    ↓
ETL aneel_mmgd_distribuidora.py
    ↓
Transformação (agregação por distribuidora + tipo de fonte)
    ↓
Tabela geracao_mmgd_distribuidora (banco)
    ↓
Repositório obter_carga_oculta()
    ↓
KPI "Geração MMGD Oficial" (frontend)
```

## 🎯 Próximos Passos

1. ✅ Rodar a ETL: `docker compose exec etl python src/extractors/aneel_mmgd_distribuidora.py`
2. ✅ Verificar dados no banco: `docker compose exec db psql -U admin -d energy_monitor -c "SELECT * FROM geracao_mmgd_distribuidora LIMIT 5;"`
3. ✅ Acessar dashboard e ver KPI atualizado: http://localhost:8501
