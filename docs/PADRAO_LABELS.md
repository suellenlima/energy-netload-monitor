# Padrão de Labels e Unidades - Energy Netload Monitor

**Versão**: 1.0
**Data**: Fevereiro 2025
**Status**: Oficial

---

## 1. Objetivo

Estabelecer nomenclatura **consistente e clara** para todos os gráficos, tabelas, métricas e documentação do sistema.

---

## 2. Unidades Físicas Padronizadas

### 2.1 Potência

| Unidade | Símbolo | Uso | Exemplo |
|---------|---------|-----|---------|
| Megawatt | MW | Potência de sistemas, carga ONS, geração MMGD | "Carga Líquida: 150 MW" |
| Kilowatt | kW | Potência de instalações individuais | "Potência instalada: 5.5 kW" |

**Importante**:
- ✅ **Sempre especificar**: "Potência (MW)" ou "Carga (MW)"
- ❌ **Nunca usar**: Apenas "Potência" ou "Carga" sem unidade
- 📌 **Contexto**: MW geralmente representa **potência média horária**

### 2.2 Energia

| Unidade | Símbolo | Uso | Exemplo |
|---------|---------|-----|---------|
| Megawatt-hora | MWh | Energia acumulada (diária, mensal) | "Energia gerada: 240 MWh/dia" |
| Kilowatt-hora | kWh | Consumo de UC individuais | "Consumo mensal: 150 kWh/mês" |

**Importante**:
- ✅ **Energia**: Acumulado ao longo do tempo (MWh/dia, kWh/mês)
- ✅ **Potência**: Instantânea ou média em um período (MW, kW)
- ❌ **Não confundir**: "MW/h" não existe! Use "MW" (potência) ou "MWh" (energia)

### 2.3 Irradiância Solar

| Unidade | Símbolo | Uso | Exemplo |
|---------|---------|-----|---------|
| Watts por metro quadrado | W/m² | Intensidade da radiação solar | "Irradiância: 850 W/m²" |

**Referência**: 1000 W/m² = STC (Standard Test Conditions) - condições padrão de teste

### 2.4 Temperatura

| Unidade | Símbolo | Uso | Exemplo |
|---------|---------|-----|---------|
| Graus Celsius | °C | Temperatura ambiente | "Temperatura: 28°C" |

### 2.5 Distância

| Unidade | Símbolo | Uso | Exemplo |
|---------|---------|-----|---------|
| Quilômetro | km | Raios de clustering, distâncias entre instalações | "Raio de detecção: 10 km" |

### 2.6 Tensão Elétrica

| Unidade | Símbolo | Uso | Exemplo |
|---------|---------|-----|---------|
| Kilovolt | kV | Tensão nominal de subestações | "Tensão: 138 kV" |

**Classificação**:
- **AT (Alta Tensão)**: 230 kV, 500 kV, 765 kV
- **MT (Média Tensão)**: 69 kV, 138 kV
- **BT (Baixa Tensão)**: 220V, 380V, 13.8 kV

### 2.7 Fator de Carga (Adimensional)

| Unidade | Símbolo | Uso | Exemplo |
|---------|---------|-----|---------|
| Por unidade | p.u. | Fatores normalizados de perfis de carga | "Fator de carga: 1.45 p.u." |
| Percentual | % | Fator de capacidade, eficiência | "Fator de capacidade: 18%" |

**Importante**:
- ✅ **p.u. (per unit)**: Valor normalizado, geralmente com média = 1.0
- ✅ **Perfis de carga**: Usar p.u. para facilitar comparação
- ✅ **Para obter MW/kW**: Multiplicar p.u. pela potência média

---

## 3. Padrão de Labels para Eixos de Gráficos

### 3.1 Eixo X (Tempo)

| Tipo de Dado | Label Padrão | Exemplo |
|--------------|--------------|---------|
| Hora do dia (0-23) | "Hora do Dia" | Gráficos de perfis de carga |
| Data e hora | "Data/Hora" | Séries temporais de carga ONS |
| Apenas data | "Data" | Acumulados diários |

**Formato de sufixo**:
- ✅ `ticksuffix="h"` para horas: "12h", "18h"
- ✅ `tickformat="%d/%m"` para datas: "15/01"
- ✅ `tickformat="%d/%m %Hh"` para timestamp: "15/01 14h"

### 3.2 Eixo Y (Valores)

| Tipo de Dado | Label Padrão | ticksuffix | Exemplo |
|--------------|--------------|------------|---------|
| Potência (MW) | "Potência Média (MW)" | " MW" | Carga ONS, consumo real |
| Potência (kW) | "Potência (kW)" | " kW" | Instalações individuais |
| Energia (MWh) | "Energia (MWh)" | " MWh" | Acumulados |
| Fator de carga | "Fator de Carga (p.u.)" | "" | Perfis típicos |
| Irradiância | "Irradiância (W/m²)" | " W/m²" | Dados solares |
| Temperatura | "Temperatura (°C)" | "°C" | Clima |
| Percentual | "Percentual (%)" | "%" | Distribuições |

**Importante**:
- ✅ **Sempre incluir unidade entre parênteses** no label
- ✅ **Usar ticksuffix** para adicionar unidade nos valores do eixo
- ✅ **Especificar "Potência Média"** quando for média horária

---

## 4. Padrão de Títulos de Gráficos

### Formato Geral:
```
[Tipo de Análise]: [Variável Principal] vs [Variável Secundária]
```

### Exemplos:

| Contexto | Título Correto | Título Incorreto |
|----------|---------------|------------------|
| Carga comparativa | "Comparativo: Carga Líquida vs Consumo Real" | "Carga" |
| Perfis típicos | "Curvas de Carga Típicas por Classe de Consumo" | "Perfis" |
| Análise local | "Carga Sintética: Subestação SE Centro" | "Carga da Subestação" |
| Temporal | "Histórico de Carga Líquida - SUDESTE" | "Histórico" |

**Boas práticas**:
- ✅ Ser descritivo e auto-explicativo
- ✅ Incluir contexto (subsistema, período, etc) quando relevante
- ✅ Usar ":" para separar categoria de descrição
- ❌ Evitar títulos genéricos ("Gráfico", "Análise")

---

## 5. Padrão de Nomes de Métricas (Cards)

### Formato Geral:
```
[Ícone] [Nome da Métrica]
Valor com unidade
```

### Exemplos:

```python
st.metric(
    "⚡ Carga Líquida (ONS)",
    f"{valor:,.0f} MW",
    help="Carga medida pelo ONS nos pontos de entrega (transmissão)"
)

st.metric(
    "🏭 Geração MMGD (Agora)",
    f"{valor:,.0f} MW",
    help="Geração distribuída (painéis solares, mini-usinas)"
)

st.metric(
    "☀️ Irradiância Solar",
    f"{valor:,.0f} W/m²",
    help="Intensidade da radiação solar atual"
)
```

**Boas práticas**:
- ✅ Usar ícones relevantes (⚡🏭☀️🔌🌡️📍)
- ✅ Incluir contexto entre parênteses: "(ONS)", "(Agora)", "(Estimado)"
- ✅ Sempre usar `help=` para tooltip explicativo
- ✅ Formatar números: `{valor:,.0f}` adiciona separadores de milhar
- ✅ Unidade logo após o valor, com espaço: "150 MW", "850 W/m²"

---

## 6. Padrão de Nomes de Colunas em Tabelas

### Formato:
```
[Nome da Coluna] ([Unidade])
```

### Exemplos:

| Tipo de Dado | Nome da Coluna | Código |
|--------------|----------------|--------|
| Potência | "Potência (MW)" | `df.columns = ["Tipo", "Potência (MW)"]` |
| Tensão | "Tensão (kV)" | `df.columns = ["SE", "Tensão (kV)"]` |
| Distância | "Raio (km)" | `df.columns = ["Cluster", "Raio (km)"]` |
| Quantidade | "Quantidade" | Sem unidade (é contagem) |
| Nome/Texto | "Nome", "Tipo" | Sem unidade (é categórico) |

**Importante**:
- ✅ Unidade entre parênteses logo após o nome
- ✅ Campos categóricos (nome, tipo, classe) não têm unidade
- ✅ Contagens (quantidade, total de UCs) não têm unidade

---

## 7. Padrão de Legendas (Traces em Gráficos)

### Formato:
```
[Nome da Série] [(Contexto)]
```

### Exemplos:

```python
fig.add_trace(go.Scatter(
    name="Carga Líquida (ONS)",  # ✅ Clara e contextualizada
    ...
))

fig.add_trace(go.Scatter(
    name="Geração MMGD",  # ✅ Concisa
    ...
))

fig.add_trace(go.Scatter(
    name="Consumo Real (Estimado)",  # ✅ Indica que é estimativa
    ...
))
```

**Boas práticas**:
- ✅ Máximo 3-4 palavras
- ✅ Adicionar contexto quando há ambiguidade: "(ONS)", "(Estimado)"
- ✅ Usar ordem lógica: do menor para o maior, da base para o topo
- ❌ Evitar abreviações obscuras

---

## 8. Padrão de Tooltips (Hover)

### Formato Recomendado:

```python
hovertemplate=(
    "<b>Nome da Série</b><br>"
    "Hora: %{x}<br>"
    "Valor: %{y:,.0f} MW<br>"
    "<extra></extra>"  # Remove box secundário
)
```

### Exemplos por Tipo:

#### Temporal:
```python
hovertemplate=(
    "<b>Carga Líquida (ONS)</b><br>"
    "Data/Hora: %{x}<br>"
    "Carga: %{y:,.0f} MW<br>"
    "<extra></extra>"
)
```

#### Perfil de Carga:
```python
hovertemplate=(
    f"<b>{classe.title()}</b><br>"
    "Hora: %{x}h<br>"
    "Fator: %{y:.2f} p.u.<br>"
    "<extra></extra>"
)
```

#### Geográfico:
```python
hovertemplate=(
    "<b>%{customdata[0]}</b><br>"  # Nome da SE
    "Potência: %{customdata[1]:,.0f} MW<br>"
    "Latitude: %{lat:.4f}<br>"
    "Longitude: %{lon:.4f}<br>"
    "<extra></extra>"
)
```

**Boas práticas**:
- ✅ Nome em negrito: `<b>Nome</b>`
- ✅ Incluir unidades em cada linha
- ✅ Formatar números: `:,.0f` (inteiro com separador), `:.2f` (2 decimais)
- ✅ Usar `<extra></extra>` para remover box secundário
- ✅ Quebrar linhas com `<br>`

---

## 9. Conversões de Unidades

### Tabela de Conversão Rápida:

| De | Para | Fórmula | Exemplo |
|----|------|---------|---------|
| kW | MW | kW / 1000 | 5000 kW = 5 MW |
| MW | kW | MW × 1000 | 2.5 MW = 2500 kW |
| kWh/mês | kW médio | kWh / 720 | 150 kWh/mês ≈ 0.208 kW |
| kW médio | kWh/mês | kW × 720 | 0.208 kW ≈ 150 kWh/mês |
| W/m² | Fator cap. | W/m² / 1000 | 850 W/m² = 0.85 p.u. |
| Fator cap. | W/m² | p.u. × 1000 | 0.85 p.u. = 850 W/m² |

**Importante**:
- 📌 **720 horas/mês** = 30 dias × 24 horas
- 📌 **1000 W/m²** = STC (Standard Test Conditions)
- 📌 **Potência média** = Energia / Tempo

---

## 10. Cores Padronizadas por Conceito

### Mapa de Cores:

| Conceito | Cor | Código Hex | Uso |
|----------|-----|------------|-----|
| Carga Líquida (ONS) | 🔵 Azul Escuro | #1e3a8a | Linha principal, área preenchida |
| Geração MMGD | 🟡 Amarelo | #facc15 | Área preenchida (carga oculta) |
| Consumo Real | 🟢 Verde | #16a34a | Linha superior, borda |
| Residencial | 🔵 Azul | #1f77b4 | Perfis de carga |
| Comercial | 🟠 Laranja | #ff7f0e | Perfis de carga |
| Industrial | 🟢 Verde | #2ca02c | Perfis de carga |
| Rural | 🔴 Vermelho | #d62728 | Perfis de carga |
| Poder Público | 🟣 Roxo | #9467bd | Perfis de carga |
| Alerta/Fraude | 🔴 Vermelho | #dc2626 | Alertas, projeções |

**Consistência**:
- ✅ Usar sempre as mesmas cores para os mesmos conceitos
- ✅ Manter contraste suficiente em modo escuro
- ✅ Evitar mais de 5 cores no mesmo gráfico

---

## 11. Checklist de Conformidade

### Ao criar um novo gráfico, verificar:

- [ ] **Eixo X**: Label claro com unidade (se aplicável)
- [ ] **Eixo Y**: Label com unidade entre parênteses
- [ ] **Título**: Descritivo e auto-explicativo
- [ ] **Legendas**: Concisas (3-4 palavras), com contexto se necessário
- [ ] **Tooltips**: Formatados com unidades explícitas
- [ ] **Métricas**: Ícones relevantes, help text, unidades após o valor
- [ ] **Cores**: Seguem padrão estabelecido
- [ ] **Tabelas**: Colunas com unidades entre parênteses
- [ ] **Números**: Formatados com separadores (`,`) quando > 1000

### Ao documentar código:

- [ ] **Docstrings**: Args e Returns com unidades
- [ ] **Comentários**: Unidades em fórmulas e conversões
- [ ] **Schemas**: Field(..., description="...") com unidades
- [ ] **SQL**: COMMENT ON COLUMN com unidades

---

## 12. Exemplos de Código Correto

### Gráfico Completo:

```python
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df["hora"],
    y=df["carga_mw"],
    mode="lines",
    name="Carga Líquida (ONS)",
    line=dict(color="#1e3a8a", width=3),
    hovertemplate=(
        "<b>Carga Líquida (ONS)</b><br>"
        "Data/Hora: %{x}<br>"
        "Carga: %{y:,.0f} MW<br>"
        "<extra></extra>"
    )
))

fig.update_layout(
    title="Comparativo: Carga Líquida vs Consumo Real",
    xaxis_title="Data/Hora",
    yaxis_title="Potência Média (MW)",
    template="plotly_dark",
    yaxis=dict(
        tickformat=",.0f",
        ticksuffix=" MW"
    )
)

st.plotly_chart(fig, use_container_width=True)
```

### Métrica Completa:

```python
st.metric(
    label="⚡ Carga Líquida (ONS)",
    value=f"{carga_mw:,.0f} MW",
    delta=f"{delta:+.1f} MW",
    help="Carga medida pelo ONS nos pontos de entrega do sistema de transmissão"
)
```

### Schema Pydantic:

```python
class CargaResponse(BaseModel):
    """Resposta com dados de carga."""

    carga_mw: float = Field(
        ...,
        description="Carga líquida medida pelo ONS (MW)",
        ge=0
    )
    irradiancia_wm2: float = Field(
        ...,
        description="Irradiância solar (W/m²)",
        ge=0,
        le=1500
    )
```

### SQL com Comentários:

```sql
CREATE TABLE carga_ons (
    time TIMESTAMPTZ NOT NULL,
    carga_mw DOUBLE PRECISION
);

COMMENT ON COLUMN carga_ons.carga_mw IS 'Carga líquida medida pelo ONS (MW) - potência média horária';
```

---

## 13. Glossário de Termos

| Termo | Significado | Unidade Típica |
|-------|-------------|----------------|
| **Carga Líquida** | Medida pelo ONS nos pontos de entrega | MW |
| **MMGD** | Micro e Minigeração Distribuída | MW ou kW |
| **Consumo Real** | Carga Líquida + MMGD | MW |
| **Carga Oculta** | Energia MMGD não vista pelo ONS | MW |
| **Fator de Capacidade** | Geração real / Capacidade instalada | p.u. ou % |
| **Fator de Carga** | Demanda média / Demanda pico | p.u. ou % |
| **Irradiância** | Intensidade da radiação solar | W/m² |
| **STC** | Standard Test Conditions (1000 W/m²) | W/m² |
| **p.u.** | Per unit - valor normalizado | Adimensional |
| **AT/MT/BT** | Alta/Média/Baixa Tensão | kV |

---

## 14. Referências

- **ANEEL**: Nomenclatura de classes de consumo
- **ONS**: Definições de carga líquida e subsistemas
- **EPE**: Perfis típicos de carga
- **ABNT NBR**: Normas de unidades de medida
- **IEEE Std 1547**: Interconexão de geração distribuída

---

**Documento mantido por**: Equipe Energy Netload Monitor
**Última atualização**: 2025-02-02
**Versão**: 1.0
