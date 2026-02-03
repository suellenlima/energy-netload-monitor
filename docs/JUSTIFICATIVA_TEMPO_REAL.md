# Justificativa Técnica: Análise em "Tempo Real"

**Versão**: 1.0
**Data**: Janeiro 2024
**Autores**: Equipe Energy Netload Monitor

---

## 1. Contexto e Desafio

Durante a apresentação do projeto no hackathon, a banca avaliadora questionou a **viabilidade de análises em tempo real** considerando que:

> "Os registros da BDGD são de energias mensais, não há medições disponíveis com taxa de amostragem suficiente para ações em tempo real como defendido no pitch."

Esta é uma observação pertinente e merece uma resposta técnica estruturada.

---

## 2. Limitações de Dados Públicos no Brasil

### 2.1 Realidade da Infraestrutura de Medição

| Tipo de Medição | Disponibilidade Pública | Frequência | Cobertura |
|-----------------|-------------------------|------------|-----------|
| **Smart Meters (AMI)** | ❌ Não disponível | Horária/15min | <5% das UCs |
| **BDGD** | ✅ Disponível (ANEEL) | **Mensal** | ~95% das UCs |
| **SIGA** | ✅ Disponível (ANEEL) | Anual | Usinas registradas |
| **Carga ONS** | ✅ Disponível (API) | Horária | Sistema de transmissão |
| **Clima/Irradiância** | ✅ Disponível (APIs) | 15 minutos | Várias estações |

**Conclusão**: Não existem dados de medição horária por UC disponíveis publicamente no Brasil (jan/2024).

### 2.2 Por que smart meters não estão disponíveis?

1. **Implantação gradual**: Distribuidoras estão instalando AMI, mas dados são proprietários
2. **Privacidade**: LGPD restringe publicação de dados de consumo individual
3. **Custo**: Infraestrutura de telecomunicação cara para ~90 milhões de UCs
4. **Regulação**: ANEEL não obriga publicação de dados horários por UC

---

## 3. Nossa Abordagem: Estimativa Informada

### 3.1 O que NÃO estamos fazendo

❌ Medição real em tempo real via smart meters
❌ Leitura direta de consumo individual
❌ Telemetria de subestações

### 3.2 O que ESTAMOS fazendo

✅ **Estimativa Sintética em Tempo Real** usando:

```
┌─────────────────────────────────────────────────────────────┐
│                  FONTES DE DADOS                            │
├─────────────────────────────────────────────────────────────┤
│ 1. Carga ONS (horária)           │ Última medição real     │
│ 2. Irradiância atual (15 min)    │ API Open-Meteo          │
│ 3. Perfis de carga típicos       │ EPE/ANEEL (literatura)  │
│ 4. Mix de consumidores (BDGD)    │ Mensal → horário        │
│ 5. Potência MMGD instalada       │ SIGA/GD ANEEL           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              MODELO DE ESTIMATIVA                           │
├─────────────────────────────────────────────────────────────┤
│ Consumo_hora(h) = Σ(Qtd_UCs_classe × Perfil_classe(h))     │
│                                                             │
│ Geração_MMGD(h) = P_instalada × (Irrad/1000) × η           │
│                                                             │
│ Carga_Real(h) = Carga_ONS(h) + Geração_MMGD(h)             │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 Fundamentação Científica

Esta abordagem é **padrão de mercado** e utilizada por:

| Entidade | Uso de Perfis Sintéticos |
|----------|--------------------------|
| **ONS** | Planejamento da operação diária ([PEN 2023](https://www.ons.org.br/paginas/sobre-o-ons/procedimentos-de-rede/vigentes)) |
| **EPE** | Projeções de demanda ([PDE 2031](https://www.epe.gov.br/pt/publicacoes-dados-abertos/publicacoes/plano-decenal-de-expansao-de-energia-pde)) |
| **IEEE** | Modelagem de sistemas elétricos (IEEE 1547.4) |
| **Distribuidoras** | Planejamento de rede (quando AMI não disponível) |

**Referências Acadêmicas**:
- Paatero, J. V., & Lund, P. D. (2006). "A model for generating household electricity load profiles"
- Grandjean, A., et al. (2012). "A review and an analysis of the residential electric load curve models"
- Muratori, M. (2018). "Impact of uncoordinated plug-in electric vehicle charging on residential power demand"

---

## 4. Precisão e Validação

### 4.1 Validação do Modelo

Nossa abordagem permite **validação cruzada**:

```python
# Comparar estimativa sintética com medição real agregada
Carga_Estimada_Local = Soma_Perfis_Sintéticos
Carga_Medida_ONS = Leitura_Real_Subestação

Erro_Percentual = |Estimada - Medida| / Medida × 100%
```

**Precisão esperada** (baseada em literatura):
- Nível agregado (subestação): **±10-15%** (aceitável para planejamento)
- Nível individual (UC): **±30-50%** (não usamos para decisões críticas)

### 4.2 Casos de Uso Apropriados

| Aplicação | Viável com Estimativas? | Justificativa |
|-----------|------------------------|---------------|
| **Planejamento de expansão** | ✅ Sim | Horizonte de anos, erros se compensam |
| **Detecção de anomalias** | ✅ Sim | Padrões persistentes são detectáveis |
| **Análise de impacto MMGD** | ✅ Sim | Geração solar é estimável por irradiância |
| **Identificação de fraude** | ⚠️ Parcial | Requer validação com medição real |
| **Comando direto de cargas** | ❌ Não | Requer medição real e comunicação |

---

## 5. Evolução Futura: Preparados para AMI

Nossa arquitetura é **compatível com smart meters**:

```
┌─────────────────────────────────────────────────────────────┐
│                  ARQUITETURA MODULAR                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  [Perfis Sintéticos] ←→ [Interface] ←→ [Dados Reais AMI]   │
│                                                             │
│  Quando AMI disponível:                                     │
│  1. Substituir perfis por leituras reais                    │
│  2. Usar sintéticos como fallback                           │
│  3. Calibrar perfis com dados reais                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Código preparado**:
```python
def get_consumo_hora(uc_id: str, hora: int) -> float:
    """Busca consumo - real se disponível, sintético caso contrário"""
    # Tentar AMI primeiro
    consumo_real = fetch_ami_reading(uc_id, hora)
    if consumo_real:
        return consumo_real

    # Fallback para perfil sintético
    return apply_synthetic_profile(uc_id, hora)
```

---

## 6. Resposta aos Questionamentos da Banca

### 6.1 "BDGD tem dados mensais, não horários"

**✅ Correto!** E reconhecemos isso. Nossa inovação é:

> Transformar dados mensais em curvas horárias usando perfis típicos validados pela literatura técnica (EPE/ANEEL), criando estimativas locais que **antes não existiam**.

**Contribuição**:
- Nível regional: Usávamos carga agregada ONS (imprecisa localmente)
- Nível local (nossa solução): Carga estimada por subestação baseada em mix real de consumidores

### 6.2 "Não há taxa de amostragem para tempo real"

**✅ Correto!** Smart meters não são públicos. Mas:

1. **"Tempo real" = Estimativa atualizada** (não medição direta)
2. **Atualização horária** é viável com dados disponíveis:
   - Carga ONS: atualizada a cada hora
   - Irradiância: API com refresh de 15 minutos
   - Perfis: aplicados à hora atual do sistema

3. **Decisões operacionais** não requerem precisão de milissegundos:
   - Planejamento de dia seguinte: horizonte de 24h
   - Despacho de geração: ONS já tem seus sistemas
   - Nossa contribuição: **visibilidade local** que não existia

### 6.3 "Ações em tempo real como defendido no pitch"

**Esclarecimento**: Redefinimos "ações em tempo real" para:

| O que dissemos inicialmente | O que realmente fazemos |
|-----------------------------|-------------------------|
| "Detectar fraudes em tempo real" | "Identificar padrões suspeitos com atualização horária" |
| "Controlar cargas em tempo real" | "Recomendar ações com base no estado atual estimado" |
| "Medição instantânea" | "Estimativa informada com dados de múltiplas fontes" |

**Honestidade técnica**: Nosso sistema é uma **ferramenta de análise e planejamento**, não um SCADA de tempo real crítico.

---

## 7. Valor da Nossa Solução

### 7.1 O que oferecemos que não existia antes?

1. **Visão local granular**: Subestação por subestação, não apenas regional
2. **Mix de consumidores real**: BDGD + clustering espacial
3. **Separação conceitual**: Carga líquida (ONS) vs Consumo real (+MMGD)
4. **Perfis categorizados**: Residencial ≠ Comercial ≠ Industrial (clara diferença horária)
5. **Metodologia defensável**: Baseada em padrões IEEE e práticas da EPE

### 7.2 Casos de uso práticos

✅ **Planejamento de expansão de rede**: Onde instalar novos transformadores?
✅ **Análise de impacto de GD**: Como MMGD afeta a curva de carga local?
✅ **Estudos regulatórios**: Subsídio para políticas públicas (ANEEL)
✅ **Educação e pesquisa**: Visualização de conceitos de sistemas elétricos

---

## 8. Conclusão

### Reconhecemos:
- BDGD tem dados mensais, não horários
- Smart meters públicos não existem no Brasil (jan/2024)
- Não fazemos medição real em tempo real

### Defendemos:
- Nossa abordagem de **estimativa sintética** é tecnicamente sólida
- É **padrão de mercado** (ONS, EPE, IEEE)
- Oferece **valor prático** para planejamento e análise
- Está **preparada para evolução** quando AMI se tornar disponível

### Nossa contribuição:

> "Transformamos dados públicos fragmentados (ONS, BDGD, SIGA, clima) em uma visão integrada e localmente relevante do sistema elétrico, usando metodologia cientificamente validada."

Isso **não existia antes** de forma acessível e gratuita.

---

## 9. Materiais de Apoio

### Para a Apresentação:

**Slide 1**: "Desafio da Banca"
- Quote do feedback sobre dados mensais

**Slide 2**: "Realidade Brasileira"
- Tabela de disponibilidade de dados

**Slide 3**: "Nossa Abordagem"
- Diagrama de estimativa sintética

**Slide 4**: "Validação Científica"
- Referências EPE, ONS, IEEE

**Slide 5**: "Casos de Uso"
- O que funciona, o que não funciona

**Slide 6**: "Preparados para o Futuro"
- Arquitetura modular para AMI

---

## 10. Referências

1. ONS - Procedimentos de Rede: https://www.ons.org.br/paginas/sobre-o-ons/procedimentos-de-rede/vigentes
2. EPE - Plano Decenal de Expansão: https://www.epe.gov.br/pt/publicacoes-dados-abertos/publicacoes/plano-decenal-de-expansao-de-energia-pde
3. ANEEL - PRODIST Módulo 7 (Qualidade da Energia Elétrica)
4. IEEE 1547.4 - Guide for Design, Operation, and Integration of DER
5. Paatero & Lund (2006) - "A model for generating household electricity load profiles"
6. ANEEL - Dados Abertos BDGD: https://dadosabertos.aneel.gov.br/dataset/bdgd

---

**Documento preparado para defesa técnica perante a banca avaliadora do hackathon.**
