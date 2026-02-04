# 📦 ETL: Sistema de Importação e Sincronização de Área de Cobertura

## 📋 Visão Geral

Sistema completo de ETL (Extract, Transform, Load) para cadastrar e manter atualizada a área de cobertura real das subestações com base em:
- 🔌 Transformadores da rede de distribuição
- 👥 Consumidores (clientes)
- 🗺️ Polígonos oficiais da concessionária
- 🔄 Sincronização automática com SCADA

---

## 🎯 Funcionalidades

### 1️⃣ Importação Inicial
- Importar transformadores de CSV
- Importar consumidores de CSV
- Importar polígonos GeoJSON
- Criar polígonos automáticos

### 2️⃣ Sincronização SCADA
- Buscar dados em tempo real
- Atualizar equipamentos existentes
- Inserir novos equipamentos
- Marcar inativos automaticamente
- Recalcular áreas automaticamente

### 3️⃣ Manutenção
- Limpar dados antigos
- Recalcular áreas de cobertura
- Gerar relatórios
- Monitoramento contínuo

---

## 📁 Arquivos Criados

### Scripts Python:

1. **`etl_area_cobertura.py`** (450 linhas)
   - Importação de transformadores CSV
   - Importação de consumidores CSV
   - Importação de polígonos GeoJSON
   - Criação automática de polígonos
   - Geração de relatórios

2. **`etl_sincronizacao_scada.py`** (500 linhas)
   - Cliente SCADA (simulado)
   - Sincronização de transformadores
   - Recálculo automático de áreas
   - Limpeza de dados antigos
   - Loop contínuo de sincronização

### Dados de Exemplo:

3. **`data/etl/transformadores_exemplo.csv`**
   - 20 transformadores de Brasília
   - Formato: codigo, subestacao_id, nome, lat, lon, potencia_kva, tipo, status

4. **`data/etl/consumidores_exemplo.csv`**
   - 15 consumidores vinculados a transformadores
   - Formato: codigo_cliente, transformador_codigo, nome, lat, lon, tipo_cliente, consumo_kwh

5. **`data/etl/poligono_se_brasilia.geojson`**
   - Polígono oficial de exemplo
   - Formato GeoJSON padrão

---

## 🚀 Como Usar

### CENÁRIO 1: Importação Inicial (Primeira Vez)

#### Passo 1: Importar Transformadores
```powershell
python etl_area_cobertura.py `
    --transformadores data/etl/transformadores_exemplo.csv `
    --subestacao-id 1
```

**Saída:**
```
📥 Importando transformadores de: data/etl/transformadores_exemplo.csv
✅ Transformadores importados: 20
❌ Erros: 0
```

#### Passo 2: Importar Consumidores
```powershell
python etl_area_cobertura.py `
    --consumidores data/etl/consumidores_exemplo.csv `
    --subestacao-id 1
```

**Saída:**
```
📥 Importando consumidores de: data/etl/consumidores_exemplo.csv
✅ Consumidores importados: 15
❌ Erros: 0
```

#### Passo 3A: Importar Polígono Oficial (SE DISPONÍVEL)
```powershell
python etl_area_cobertura.py `
    --poligono data/etl/poligono_se_brasilia.geojson `
    --subestacao-id 1
```

**OU**

#### Passo 3B: Criar Polígono Automático (SE NÃO TEM OFICIAL)
```powershell
python etl_area_cobertura.py `
    --criar-poligono `
    --subestacao-id 1
```

**Saída:**
```
🔧 Criando polígono automático para SE ID=1
✅ Polígono criado automaticamente
   Transformadores: 20
   Área: 11.25 km²
```

#### Passo 4: Gerar Relatório
```powershell
python etl_area_cobertura.py `
    --relatorio `
    --subestacao-id 1
```

**Saída:**
```
================================================================================
📊 RELATÓRIO DE COBERTURA - SUBESTAÇÃO ID=1
================================================================================

🗺️  ÁREA DE COBERTURA:
   Método: analise_topologica
   Área: 11.25 km²
   Atualização: 2026-01-31 10:30:00
   Observações: Polígono gerado automaticamente de 20 transformadores

🔌 TRANSFORMADORES:
   Total: 20
   Potência total: 5,475 kVA
   Ativos: 20
   Inativos: 0

👥 CONSUMIDORES:
   Total: 15
   Consumo total: 12,370 kWh/mês
   Residencial: 9
   Comercial: 4
   Industrial: 2
```

---

### CENÁRIO 2: Sincronização com SCADA (Atualização)

#### Sincronização Única (Manual)
```powershell
# Uma subestação
python etl_sincronizacao_scada.py --subestacao-ids 1

# Múltiplas subestações
python etl_sincronizacao_scada.py --subestacao-ids 1 2 3

# Todas as subestações
python etl_sincronizacao_scada.py --todas
```

**Saída:**
```
================================================================================
🔄 SINCRONIZAÇÃO COMPLETA - ÁREA DE COBERTURA
================================================================================
📅 Data/Hora: 2026-01-31 10:30:00
📍 Subestações: 1

🔄 Sincronizando transformadores da SE ID=1
   SCADA: 20 transformadores
   ✅ Novos: 0 | Atualizados: 20 | Inativos: 0

📐 Recalculando área de cobertura da SE ID=1
   ✅ Área recalculada: 11.25 km²
   📊 Baseado em 20 transformadores ativos

🧹 Limpando dados inativos há mais de 90 dias
   ✅ Consumidores removidos: 0
   ✅ Transformadores removidos: 0

================================================================================
📊 RESUMO DA SINCRONIZAÇÃO
================================================================================
   Transformadores novos: 0
   Transformadores atualizados: 20
   Transformadores marcados inativos: 0
   Subestações processadas: 1
```

#### Sincronização Contínua (Loop)
```powershell
# Executar a cada 60 minutos (padrão)
python etl_sincronizacao_scada.py --todas --loop

# Executar a cada 30 minutos
python etl_sincronizacao_scada.py --todas --loop --intervalo 30

# Executar a cada 2 horas (120 minutos)
python etl_sincronizacao_scada.py --todas --loop --intervalo 120
```

**Saída:**
```
🔁 Iniciando sincronização contínua (intervalo: 60 min)
📍 Monitorando 3 subestações

[Executa sincronização completa]

⏳ Próxima sincronização em 60 minutos...
[Aguarda]
[Repete...]
```

---

### CENÁRIO 3: Limpeza de Dados Antigos

```powershell
# Limpar inativos há mais de 90 dias (padrão)
python etl_sincronizacao_scada.py --limpar-antigos 90

# Limpar inativos há mais de 180 dias
python etl_sincronizacao_scada.py --limpar-antigos 180

# Limpar inativos há mais de 30 dias
python etl_sincronizacao_scada.py --limpar-antigos 30
```

---

## 📊 Formato dos Dados

### CSV de Transformadores

**Colunas obrigatórias:**
```csv
codigo,subestacao_id,latitude,longitude,potencia_kva
TR-001,1,-15.8100,-47.9100,300.0
```

**Colunas opcionais:**
```csv
nome,tipo,status,tensao_primaria_kv,tensao_secundaria_v
Transformador 1,aereo,ativo,13.8,220
```

**Tipos válidos:**
- `aereo` - Transformador aéreo (poste)
- `pedestal` - Transformador em pedestal
- `subterraneo` - Transformador subterrâneo

**Status válidos:**
- `ativo` - Em operação
- `inativo` - Desligado
- `manutencao` - Em manutenção

### CSV de Consumidores

**Colunas obrigatórias:**
```csv
codigo_cliente,transformador_codigo,latitude,longitude
CLI-001,TR-001,-15.8105,-47.9105
```

**Colunas opcionais:**
```csv
nome,tipo_cliente,consumo_medio_mensal_kwh,status
Residência 1,residencial,350,ativo
```

**Tipos de cliente:**
- `residencial` - Residência
- `comercial` - Comércio
- `industrial` - Indústria
- `rural` - Zona rural
- `publico` - Serviço público

### GeoJSON de Polígono

**Formato padrão:**
```json
{
  "type": "Feature",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[
      [-47.95, -15.77],
      [-47.86, -15.77],
      [-47.86, -15.87],
      [-47.95, -15.87],
      [-47.95, -15.77]
    ]]
  },
  "properties": {
    "nome": "Área SE X",
    "observacoes": "Polígono oficial"
  }
}
```

**Ou apenas geometria:**
```json
{
  "type": "Polygon",
  "coordinates": [[
    [-47.95, -15.77],
    ...
  ]]
}
```

---

## 🔧 Integração com SCADA Real

### Passo 1: Implementar Cliente SCADA

Editar `etl_sincronizacao_scada.py`, classe `SCADAClient`:

```python
class SCADAClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url  # Ex: "http://scada.concessionaria.com/api"
        self.api_key = api_key    # Sua API key
        
    def get_transformadores(self, subestacao_id: int):
        """Buscar transformadores do SCADA real"""
        response = self.session.get(
            f"{self.base_url}/subestacoes/{subestacao_id}/transformadores"
        )
        
        data = response.json()
        
        # Transformar para formato esperado
        return [
            {
                'codigo': t['codigo'],
                'latitude': t['latitude'],
                'longitude': t['longitude'],
                'potencia_kva': t['potencia_kva'],
                'tipo': t['tipo'],
                'status': t['status'],
                'tensao_primaria_kv': t['tensao_primaria'],
                'tensao_secundaria_v': t['tensao_secundaria']
            }
            for t in data['transformadores']
        ]
```

### Passo 2: Configurar Credenciais

Criar arquivo `.env`:
```env
SCADA_URL=http://scada.concessionaria.com.br/api
SCADA_API_KEY=sua_api_key_aqui
SCADA_TIMEOUT=30
```

Carregar no script:
```python
from dotenv import load_dotenv
import os

load_dotenv()

scada_client = SCADAClient(
    base_url=os.getenv('SCADA_URL'),
    api_key=os.getenv('SCADA_API_KEY')
)
```

### Passo 3: Mapear Endpoints

**Exemplos de endpoints típicos:**

```python
# Listar transformadores de uma SE
GET /api/subestacoes/{id}/transformadores

# Detalhes de um transformador
GET /api/transformadores/{codigo}

# Status em tempo real
GET /api/transformadores/{codigo}/status

# Listar consumidores de um transformador
GET /api/transformadores/{codigo}/consumidores

# Histórico de eventos
GET /api/transformadores/{codigo}/eventos?data_inicio=2026-01-01
```

---

## 🤖 Automatização

### Opção 1: Cron (Linux/Mac)

Editar crontab:
```bash
crontab -e
```

Adicionar:
```bash
# Sincronizar a cada hora
0 * * * * cd /caminho/energy-netload-monitor && python etl_sincronizacao_scada.py --todas

# Sincronizar a cada 6 horas
0 */6 * * * cd /caminho/energy-netload-monitor && python etl_sincronizacao_scada.py --todas

# Limpar dados antigos todo domingo às 3h
0 3 * * 0 cd /caminho/energy-netload-monitor && python etl_sincronizacao_scada.py --limpar-antigos 90
```

### Opção 2: Task Scheduler (Windows)

```powershell
# Criar tarefa agendada
$action = New-ScheduledTaskAction `
    -Execute "python" `
    -Argument "C:\Hackathon\Git\energy-netload-monitor\etl_sincronizacao_scada.py --todas"

$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Hours 1)

Register-ScheduledTask `
    -TaskName "ETL Area Cobertura" `
    -Action $action `
    -Trigger $trigger `
    -Description "Sincronização de área de cobertura"
```

### Opção 3: Docker/Kubernetes

Criar serviço Docker:
```dockerfile
FROM python:3.10

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY etl_sincronizacao_scada.py .

CMD ["python", "etl_sincronizacao_scada.py", "--todas", "--loop", "--intervalo", "60"]
```

---

## 📈 Monitoramento

### Logs

Todos os scripts geram logs detalhados:

```python
# Configurar nível de log
import logging
logging.basicConfig(level=logging.INFO)  # ou DEBUG para mais detalhes
```

**Salvar logs em arquivo:**
```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_area_cobertura.log'),
        logging.StreamHandler()
    ]
)
```

### Métricas

Criar tabela de auditoria:
```sql
CREATE TABLE etl_auditoria (
    id SERIAL PRIMARY KEY,
    tipo_etl VARCHAR(50),
    subestacao_id INTEGER,
    timestamp TIMESTAMP DEFAULT NOW(),
    registros_novos INTEGER,
    registros_atualizados INTEGER,
    registros_excluidos INTEGER,
    duracao_segundos DECIMAL,
    status VARCHAR(20),
    mensagem TEXT
);
```

Registrar execução:
```python
cursor.execute("""
    INSERT INTO etl_auditoria (
        tipo_etl, subestacao_id, registros_novos,
        registros_atualizados, duracao_segundos, status
    ) VALUES (%s, %s, %s, %s, %s, %s)
""", ('sincronizacao_scada', se_id, novos, atualizados, duracao, 'sucesso'))
```

---

## ⚠️ Tratamento de Erros

### Transformadores Duplicados

```python
# ON CONFLICT atualiza automaticamente
INSERT INTO transformadores (codigo, ...)
VALUES (%s, ...)
ON CONFLICT (codigo) DO UPDATE SET
    latitude = EXCLUDED.latitude,
    updated_at = NOW()
```

### Consumidores sem Transformador

```python
# Verificar existência antes de inserir
cursor.execute(
    "SELECT id FROM transformadores WHERE codigo = %s",
    (transformador_codigo,)
)

if not cursor.fetchone():
    logger.warning(f"Transformador {transformador_codigo} não encontrado")
    continue
```

### Erro de Conexão SCADA

```python
try:
    data = scada_client.get_transformadores(se_id)
except requests.exceptions.Timeout:
    logger.error("Timeout ao conectar ao SCADA")
    time.sleep(300)  # Aguardar 5 minutos
    continue
except requests.exceptions.ConnectionError:
    logger.error("Falha de conexão com SCADA")
    # Usar cache ou dados anteriores
```

---

## 🧪 Testes

### Testar Importação

```powershell
# 1. Importar transformadores de exemplo
python etl_area_cobertura.py `
    --transformadores data/etl/transformadores_exemplo.csv `
    --subestacao-id 1

# 2. Verificar no banco
python testar_conexao_banco.py

# 3. Gerar relatório
python etl_area_cobertura.py --relatorio --subestacao-id 1
```

### Testar Sincronização

```powershell
# 1. Executar sincronização de teste
python etl_sincronizacao_scada.py --subestacao-ids 1

# 2. Verificar logs

# 3. Comparar áreas antes/depois
python testar_area_real.py --id 1
```

---

## 📊 Fluxo Completo

```
┌─────────────────────────────────────────────────────┐
│         IMPORTAÇÃO INICIAL (Uma vez)                │
├─────────────────────────────────────────────────────┤
│  1. CSV Transformadores → etl_area_cobertura.py    │
│  2. CSV Consumidores → etl_area_cobertura.py       │
│  3. GeoJSON Polígono → etl_area_cobertura.py       │
│     (OU criar automático)                           │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│      SINCRONIZAÇÃO CONTÍNUA (Automática)            │
├─────────────────────────────────────────────────────┤
│  1. SCADA → etl_sincronizacao_scada.py (loop)      │
│  2. Atualizar transformadores                       │
│  3. Marcar inativos                                 │
│  4. Recalcular áreas                                │
│  5. Repetir a cada X minutos                        │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│           MANUTENÇÃO (Periódica)                    │
├─────────────────────────────────────────────────────┤
│  1. Limpar dados antigos (mensal)                   │
│  2. Validar integridade (semanal)                   │
│  3. Backup banco de dados (diário)                  │
└─────────────────────────────────────────────────────┘
```

---

## 🎯 Checklist de Implementação

### Fase 1: Setup Inicial (1-2 dias)
- [ ] Instalar dependências (`pip install psycopg2 requests python-dotenv`)
- [ ] Testar conexão com banco
- [ ] Preparar CSVs com dados reais
- [ ] Executar importação de transformadores
- [ ] Executar importação de consumidores
- [ ] Criar polígono automático
- [ ] Validar dados no banco

### Fase 2: Integração SCADA (3-5 dias)
- [ ] Obter credenciais da API SCADA
- [ ] Mapear endpoints do SCADA
- [ ] Implementar cliente SCADA real
- [ ] Testar busca de transformadores
- [ ] Testar sincronização manual
- [ ] Validar atualização de dados

### Fase 3: Automatização (2-3 dias)
- [ ] Configurar sincronização em loop
- [ ] Criar tarefa agendada (cron/scheduler)
- [ ] Configurar logs em arquivo
- [ ] Criar tabela de auditoria
- [ ] Implementar alertas de erro
- [ ] Documentar procedimentos

### Fase 4: Produção (ongoing)
- [ ] Monitorar logs diariamente
- [ ] Revisar relatórios semanalmente
- [ ] Ajustar intervalos conforme necessário
- [ ] Limpar dados antigos mensalmente
- [ ] Backup regular do banco

---

## 📞 Suporte

**Arquivos de referência:**
- `etl_area_cobertura.py` - Importação inicial
- `etl_sincronizacao_scada.py` - Sincronização contínua
- `data/etl/*.csv` - Dados de exemplo
- `AREA_COBERTURA_REAL.md` - Documentação técnica

**Comandos úteis:**
```powershell
# Ver ajuda
python etl_area_cobertura.py --help
python etl_sincronizacao_scada.py --help

# Testar conexão
python testar_conexao_banco.py

# Ver relatório
python etl_area_cobertura.py --relatorio --subestacao-id 1
```

---

**Versão:** 1.0  
**Data:** 31/01/2026  
**Status:** ✅ Implementado e testado  
**Autor:** Energy Netload Monitor Team
