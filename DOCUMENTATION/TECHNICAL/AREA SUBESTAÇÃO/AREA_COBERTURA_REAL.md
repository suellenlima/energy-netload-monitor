# 🗺️ Como Garantir Área de Cobertura Real da Subestação

## 📋 Visão Geral

A área de cobertura de uma subestação **NÃO é necessariamente circular**. A área real depende de:

1. **Topologia da rede elétrica** (linhas de distribuição, transformadores)
2. **Limites administrativos** (concessão, jurisdição)
3. **Barreiras geográficas** (rios, montanhas, divisas)
4. **Compartilhamento com outras subestações** (redundância)

---

## 🎯 4 Métodos de Determinação (Ordem de Confiabilidade)

### 1️⃣ Polígono Cadastrado (CONFIABILIDADE: ALTA ✅)

**Descrição:**  
Polígono oficial da área de cobertura fornecido pela concessionária de energia.

**Fonte dos Dados:**
- GeoJSON ou Shapefile da concessionária
- Cadastro oficial da ANEEL
- Sistema GIS interno da empresa

**Como Implementar:**

```sql
-- Inserir polígono de cobertura oficial
INSERT INTO subestacoes_area_cobertura (
    subestacao_id, 
    area_cobertura, 
    metodo_definicao,
    observacoes
) VALUES (
    1,  -- ID da subestação
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[-47.95,-15.80],[-47.90,-15.80],[-47.90,-15.85],[-47.95,-15.85],[-47.95,-15.80]]]}'),
    'cadastro_oficial',
    'Polígono fornecido pela CEB Distribuição em Jan/2026'
);
```

**Vantagens:**
- ✅ Máxima precisão
- ✅ Reflete limites legais/administrativos
- ✅ Considera acordos entre concessionárias

**Limitações:**
- ⚠️ Requer acesso a dados da concessionária
- ⚠️ Pode estar desatualizado se rede foi modificada

---

### 2️⃣ Rede de Transformadores (CONFIABILIDADE: ALTA ✅)

**Descrição:**  
Determina área baseada nas localizações dos transformadores de distribuição alimentados pela subestação.

**Fonte dos Dados:**
- Sistema SCADA da concessionária
- Cadastro de ativos da empresa
- Sistema de manutenção preventiva

**Como Implementar:**

```sql
-- Inserir transformadores conectados à subestação
INSERT INTO transformadores (codigo, subestacao_id, latitude, longitude, potencia_kva) VALUES
('TR-BSB-001', 1, -15.8100, -47.9100, 300.0),
('TR-BSB-002', 1, -15.8200, -47.9200, 225.0),
('TR-BSB-003', 1, -15.8300, -47.9300, 300.0);

-- Consultar transformadores
SELECT * FROM transformadores WHERE subestacao_id = 1;
```

**Executar Script SQL:**

```bash
# Conectar ao PostgreSQL e executar script de exemplo
psql -U postgres -d energy_monitor -f backend/infrastructure/database/004_area_cobertura_real.sql
```

**Vantagens:**
- ✅ Reflete topologia real da rede
- ✅ Acompanha expansões/modificações
- ✅ Permite estimar carga por região

**Limitações:**
- ⚠️ Requer integração com sistema da concessionária
- ⚠️ Pode não incluir áreas sem transformadores

---

### 3️⃣ Localizações de Consumidores (CONFIABILIDADE: MÉDIA ⚠️)

**Descrição:**  
Define área baseada nas coordenadas dos medidores/clientes atendidos pela subestação.

**Fonte dos Dados:**
- Sistema de faturamento
- Cadastro de clientes
- Sistema de medição inteligente (AMI)

**Como Implementar:**

```sql
-- Inserir consumidores
INSERT INTO consumidores (codigo_cliente, transformador_id, latitude, longitude, tipo_cliente) VALUES
('CLI-001', 1, -15.8105, -47.9105, 'residencial'),
('CLI-002', 1, -15.8110, -47.9110, 'comercial'),
('CLI-003', 1, -15.8115, -47.9115, 'residencial');

-- Consultar consumidores
SELECT * FROM consumidores c
JOIN transformadores t ON t.id = c.transformador_id
WHERE t.subestacao_id = 1;
```

**Vantagens:**
- ✅ Dados abundantes (milhares de pontos)
- ✅ Atualização frequente
- ✅ Reflete consumo real

**Limitações:**
- ⚠️ Pode ter gaps em áreas desabitadas
- ⚠️ Privacidade dos dados dos clientes
- ⚠️ Não inclui rede primária (média tensão)

---

### 4️⃣ Aproximação Circular (CONFIABILIDADE: BAIXA ❌)

**Descrição:**  
Círculo geométrico ao redor da subestação (FALLBACK - NÃO REFLETE REALIDADE).

**Como Funciona:**
- Centro: coordenadas da subestação
- Raio: estimativa (5-10 km típico)
- 37 pontos formando círculo

**Vantagens:**
- ✅ Não requer dados adicionais
- ✅ Implementação simples
- ✅ Útil para demonstrações/testes

**Limitações:**
- ❌ **NÃO REFLETE TOPOLOGIA REAL**
- ❌ Ignora limites geográficos
- ❌ Ignora compartilhamento com outras SEs
- ❌ Pode incluir áreas não atendidas
- ❌ Pode excluir áreas realmente atendidas

---

## 🛠️ Implementação Prática

### Passo 1: Configurar Banco de Dados

```bash
# 1. Configurar DATABASE_URL
export DATABASE_URL="postgresql://usuario:senha@localhost:5432/energy_monitor"

# 2. Executar script de criação de tabelas
psql $DATABASE_URL -f backend/infrastructure/database/004_area_cobertura_real.sql
```

### Passo 2: Testar Sistema

```bash
# Comparar área circular vs área real
python testar_area_real.py --id 1 --output data/areas_cobertura/comparacao_se1.json
```

**Saída Esperada (COM dados reais):**

```
================================================================================
RELATÓRIO DE COMPARAÇÃO: ÁREA APROXIMADA vs ÁREA REAL
================================================================================

📍 Subestação: SE Brasília Sul (ID: 1)
🔍 Método de Determinação: Rede de transformadores
✅ Confiabilidade: ALTA

--------------------------------------------------------------------------------
📐 ÁREA CIRCULAR APROXIMADA
--------------------------------------------------------------------------------
   Raio: 5.00 km
   Área: 78.54 km²
   Pontos no polígono: 37

--------------------------------------------------------------------------------
🔌 ÁREA REAL (REDE ELÉTRICA)
--------------------------------------------------------------------------------
   Área real: 52.30 km²
   Transformadores: 20
   Potência total: 5450 kVA
   Consumidores: 3250

   📊 DIFERENÇA:
      Absoluta: 26.24 km²
      Percentual: 33.4%
      ⚠️ Área real é 33.4% MENOR que aproximação

--------------------------------------------------------------------------------
💡 RECOMENDAÇÕES
--------------------------------------------------------------------------------
   ✅ Dados confiáveis! Use esta área para mapeamento.
   📍 A área reflete a topologia real da rede elétrica.
```

### Passo 3: Executar Pipeline com Área Real

```bash
# Pipeline de mapeamento usando área real dos transformadores
python pipeline_mapeamento_area.py --id 1 --usar-area-real --tile-size 500 --max-tiles 50
```

---

## 📊 Comparação dos Métodos

| Método | Confiabilidade | Precisão | Complexidade | Dados Necessários |
|--------|----------------|----------|--------------|-------------------|
| **1. Polígono Cadastrado** | ⭐⭐⭐⭐⭐ | 95-100% | Baixa | GeoJSON da concessionária |
| **2. Rede de Transformadores** | ⭐⭐⭐⭐⭐ | 85-95% | Média | Cadastro de transformadores |
| **3. Localizações de Consumidores** | ⭐⭐⭐ | 70-85% | Média | Cadastro de clientes |
| **4. Aproximação Circular** | ⭐ | 30-50% | Baixa | Nenhum (apenas centro) |

---

## 🎯 Qual Método Usar?

### Para Produção:
1. **Ideal:** Polígono cadastrado oficial da concessionária
2. **Alternativa:** Rede de transformadores conectados
3. **Complemento:** Dados de consumidores para validação

### Para Demonstração/Protótipo:
- Aproximação circular é aceitável
- Indicar claramente que é aproximação
- Mostrar aviso sobre limitações

### Para Pesquisa/Análise:
- Combinar múltiplos métodos
- Comparar resultados
- Validar com dados reais quando disponível

---

## 🚀 Próximos Passos

### 1. Obter Dados Reais
- [ ] Solicitar shapefile de cobertura da concessionária
- [ ] Exportar cadastro de transformadores do sistema SCADA
- [ ] Obter lista de clientes por subestação (se autorizado)

### 2. Popular Banco de Dados
```bash
# Executar script SQL fornecido
psql $DATABASE_URL -f backend/infrastructure/database/004_area_cobertura_real.sql
```

### 3. Validar Resultados
```bash
# Testar com dados reais
python testar_area_real.py --id 1
```

### 4. Executar Pipeline Completo
```bash
# Mapear área real com grid inteligente
python pipeline_mapeamento_area.py --id 1 --usar-area-real --max-tiles 100
```

---

## ⚠️ IMPORTANTE

### NÃO Usar Aproximação Circular Para:
- ❌ Planejamento operacional
- ❌ Estudos de expansão
- ❌ Análise de carga por região
- ❌ Relatórios oficiais para ANEEL
- ❌ Cálculos de perdas técnicas

### Usar Apenas Dados Reais Para:
- ✅ Decisões operacionais
- ✅ Investimentos em infraestrutura
- ✅ Estudos regulatórios
- ✅ Planejamento estratégico
- ✅ Análises críticas

---

## 📞 Contato

Para dúvidas sobre obtenção de dados reais da rede elétrica:
- Departamento de GIS da concessionária
- Engenharia de Distribuição
- Planejamento de Sistemas
- Cadastro de Ativos

---

## 📚 Referências

- [ANEEL - Procedimentos de Distribuição (PRODIST)](https://www.aneel.gov.br/prodist)
- [ONS - Procedimentos de Rede](http://www.ons.org.br/paginas/sobre-o-ons/procedimentos-de-rede)
- [PostGIS Documentation](https://postgis.net/documentation/)
- [Norma ABNT NBR 5440 - Transformadores](https://www.abnt.org.br/)

---

**Última Atualização:** 30/01/2026  
**Versão:** 1.0  
**Autor:** Energy Netload Monitor Team
