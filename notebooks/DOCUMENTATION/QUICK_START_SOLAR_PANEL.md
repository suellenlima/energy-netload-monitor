## 🚀 COMO USAR O DATASET DE PAINÉIS SOLARES

### ⚡ Quick Start

1. **Copie suas imagens de painéis solares para:**
   ```
   notebooks/data/solar_panel/
   └── (todas suas imagens aqui)
   ```
   
   Ou (alternativa):
   ```
   data/solar_panel/
   └── (todas suas imagens aqui)
   ```

2. **Teste o carregador:**
   ```bash
   python test_solar_panel_loader.py
   ```

3. **Execute o notebook 07:**
   ```
   07_advanced_detection_techniques.ipynb
   ```

---

### 📋 Passo a Passo Completo

#### 1️⃣ Preparar os dados

```bash
# Opção 1: Criar pasta em notebooks/data/
mkdir -p notebooks/data/solar_panel

# Opção 2: Criar pasta em data/
mkdir -p data/solar_panel

# Copie suas imagens (JPG, PNG, TIFF, BMP)
# cp seus_arquivos/*.jpg notebooks/data/solar_panel/
# OU
# cp seus_arquivos/*.jpg data/solar_panel/
```

#### 2️⃣ Verificar (opcional)
```bash
# Teste se está tudo certo
python test_solar_panel_loader.py
```

Saída esperada:
```
✅ 150 imagens encontradas!
✅ TESTE BEM-SUCEDIDO! Dataset pronto para usar
```

#### 3️⃣ Treinar o modelo

Execute `07_advanced_detection_techniques.ipynb` do início ao fim.

O notebook irá automaticamente:
- ✅ Carregar dados base do notebook 06
- ✅ Procurar pelo dataset solar_panel
- ✅ Adicionar ao treinamento (80% treino, 20% teste)
- ✅ Aplicar todas as técnicas avançadas
- ✅ Salvar modelo otimizado

#### 4️⃣ Comparar resultados

Execute `08_comparison_benchmark.ipynb` para:
- Comparar modelo base vs otimizado
- Ver ganhos em recall, precision, F1-Score
- Obter recomendação final

---

### 📊 Estrutura de Pastas

```
energy-netload-monitor/
├── notebooks/
│   ├── 06_transfer_learning_real.ipynb           (base)
│   ├── 07_advanced_detection_techniques.ipynb    (otimizado)
│   └── 08_comparison_benchmark.ipynb             (comparação)
├── data/
│   ├── solar_panel/                              ← NOVA PASTA
│   │   ├── painel_1.jpg
│   │   ├── painel_2.jpg
│   │   ├── painel_3.png
│   │   └── ... (suas imagens)
│   ├── pv/
│   ├── extracted_uc_merced/
│   └── lacuna-solar-survey-zindi/
├── modelos/
│   ├── modelo_detector_paineis_reais.keras       (notebook 06)
│   ├── modelo_paineis_otimizado.keras            (notebook 07)
│   └── RELATORIO_BENCHMARK_FINAL.txt             (notebook 08)
└── SOLAR_PANEL_DATASET_README.md                 (documentação)
```

---

### 🎯 Resultado Esperado

**Sem dataset solar_panel:**
```
Modelo Otimizado (06): F1-Score 82%
```

**Com dataset solar_panel:**
```
Modelo Otimizado (07): F1-Score 88-92%
                      ↑ +6-10 pontos percentuais!
```

---

### ❓ Dúvidas?

**P: Posso usar qualquer tipo de imagem?**
- ✅ Sim! Qualquer imagem de painel solar funciona
- ✅ Diferentes ângulos
- ✅ Diferentes tipos de telhado
- ✅ Diferentes condições de luz

**P: Quantas imagens preciso?**
- 🎯 50+ para ter impacto
- 🎯  100-200 é bom
- 🎯  300+ é excelente

**P: Qual é a resolução ideal?**
- ✅ Qualquer resolução funciona! O notebook redimensiona automaticamente

**P: Posso organizar em subpastas?**
```
data/solar_panel/
├── tipo_brick/
│   ├── img_1.jpg
│   └── img_2.jpg
├── tipo_concrete/
│   └── img_1.png
└── tipo_metal/
    └── img_1.tif
```
- ✅ SIM! O carregador busca recursivamente em todas as subpastas

**P: O dataset é obrigatório?**
- ❌ Não. Se não existir, o notebook 07 usa apenas dados do 06

---

### 🔧 Troubleshooting

**Erro: "Nenhuma imagem foi carregada"**
```
Solução:
1. Verifique se a pasta ./data/solar_panel/ existe
2. Copie suas imagens lá
3. Verifique formato: jpg, png, tif, bmp
```

**Erro: "Pasta não encontrada"**
```
Verifique se está em um destes locais:
• ./data/solar_panel/          (padrão)
• ./data/paineis_solares/
• ./data/solar/
• ./data/images/solar_panel/
• ./data/solar_panels/
```

**Erro: "Imagem corrompida"**
```
Solução:
1. Tente abrir a imagem no visualizador
2. Se não abrir, é arquivo corrompido
3. Remova-a da pasta solar_panel/
```

---

### 📈 Monitorar Progresso

O notebook 07 mostra em tempo real:

```
✅ Adicionando dataset de painéis solares ao treino...
   Dataset Original: 500 → Novo: 700 imagens
   Adicionadas: 160 imagens ao treino
   Adicionadas: 40 imagens ao teste

   📊 Novo balanceamento:
      Urbano (y=1): 595 (85%)
      Natural (y=0): 105 (15%)
```

---

### 🎉 Sucesso!

Quando terminar a execução, você terá:

```
✅ modelos/modelo_paineis_otimizado.keras
   → Modelo treinado com seus dados!

✅ modelos/resumo_otimizacoes.txt
   → Relatório técnico completo

✅ modelos/resultados_benchmark.csv
   → Métricas comparativas
```

---

**Pronto!** 🚀 Agora seu modelo detectará painéis solares muito melhor!

Dúvidas? Veja `SOLAR_PANEL_DATASET_README.md`
