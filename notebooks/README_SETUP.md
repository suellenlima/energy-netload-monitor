# Notebooks - Machine Learning para Detecção de Painéis Solares

## ⚙️ Setup - Docker (Comando Único!)

### ⚠️ IMPORTANTE: Execute na pasta `notebooks/`

Abra PowerShell na pasta onde está o `requirements.txt`:

```powershell
cd C:\Hackathon\Git\energy-netload-monitor\notebooks
```

### Passo 1: COPIAR requirements para o container

```powershell
docker cp requirements.txt energy_ai_lab:/home/jovyan/
```

### Passo 2: Setup automático (cria venv + instala + registra kernel)

```powershell
docker exec energy_ai_lab bash -c "cd /home/jovyan && python -m venv yolo_venv && source yolo_venv/bin/activate && pip install --upgrade pip -q && pip install -r requirements.txt -q && python -m ipykernel install --user --name venv-yolo --display-name 'Python 3.11 (venv notebooks)'"
```

### Passo 3: Reiniciar

```powershell
docker restart energy_ai_lab
```

**Pronto!** Agora é só usar.

---

## 🚀 Usar no Jupyter

1. Abra http://localhost:8888
2. Clique no kernel (canto superior direito)
3. Selecione **"Python 3.11 (venv notebooks)"**
4. Execute as células

**Esperado:**
```
Python: /home/jovyan/yolo_venv/bin/python
Versão: 3.11.x
Ambiente: venv
```

---

## 🔄 Se Precisar Reinstalar Pacotes

```powershell
# Certifique-se que requirements.txt foi copiado
docker cp requirements.txt energy_ai_lab:/home/jovyan/

# Reinstalar no container
docker exec energy_ai_lab bash -c "source /home/jovyan/yolo_venv/bin/activate && pip install -r requirements.txt --force-reinstall -q"

# Reiniciar
docker restart energy_ai_lab
```

---

## ⚠️ Troubleshooting

**Erro: `Could not open requirements file`**
- Solução: Certifique-se de executar `docker cp requirements.txt energy_ai_lab:/home/jovyan/` ANTES

**Erro: `ModuleNotFoundError: No module named 'cv2'`**
- Solução: Clique no kernel (canto superior direito) e selecione **"Python 3.11 (venv notebooks)"**, depois reinicie o kernel

**Problema: Kernel não aparece no seletor**
- Solução: Recarregue a página (Ctrl+F5)

---

## 📚 Notebooks Disponíveis

### 01 - Exploração ONS
Análise exploratória dos dados de operação do sistema elétrico.

### 02 - Validação SIGA
Validação de dados do sistema de informações geográficas.

### 03 - Simulador & Treinamento Modelo Telhados
Simulação e treinamento de modelos para detecção em telhados.

### 04 - Exemplo de Treinamento
Exemplo prático de treinamento do modelo.

### 05-06 - Treinamento Modelo Telhados
Treinamento completo do modelo de detecção com transfer learning (MobileNetV2).

### 07 - Detecção de Painéis em Subestações
Algoritmo de Detecção Heurística (OpenCV). Painéis solares têm uma assinatura visual muito específica:

- São retangulares
- Têm cor azul-escura/preta (diferente de telhados de barro ou vegetação)
- Têm bordas bem definidas

Notebook que baixa uma imagem de satélite, encontra essas áreas "azuis e retangulares" e calcula a área total. Se a área for maior que o registrado na ANEEL, temos um "Gato" (Carga Oculta Não Registrada).

---

## 🐍 Dependências Principais

| Pacote | Versão | Motivo |
|--------|--------|--------|
| numpy | >=1.26.0,<2.0.0 | Compatibilidade com TensorFlow 2.13+ |
| scipy | >=1.14.0 | Requer NumPy 1.26.4+ |
| scikit-learn | >=1.5.0 | Compatível com scipy 1.14+ |
| tensorflow | >=2.13.0 | Deep Learning para Transfer Learning |
| torch, torchvision | >=2.0.0 | PyTorch para YOLO e modelos avançados |
| opencv-python | >=4.8.0 | Processamento de imagens e detecção |
| ultralytics | >=8.0.0 | YOLOv8 para detecção de objetos |
| pillow | >=10.0.0 | Manipulação de imagens |
| albumentations | >=1.4.0 | Data augmentation para treinamento |
| jupyter, ipykernel, ipython | >=7.0.0 | Ambiente de notebooks interativos |
| plotly, seaborn, matplotlib | Latest | Visualização e gráficos |
| geopandas, rasterio | Latest | Processamento de dados geoespaciais |
