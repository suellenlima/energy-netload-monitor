# Notebooks - Machine Learning para Detecção de Painéis Solares

## ⚙️ Setup Inicial

### Instalação de Dependências

Antes de executar qualquer notebook, instale as dependências necessárias:

```bash
pip install -r requirements.txt
```

### Resolver Conflitos de Versão

Se encontrar erros relacionados a versões de `numpy`, `scipy` ou `sklearn`:

```bash
# Downgrade NumPy para compatibilidade
pip install "numpy>=1.26.4,<2"

# Atualizar SciPy para versão compatível
pip install "scipy>=1.14.0"

# Reinstalar scikit-learn
pip install --upgrade scikit-learn
```

### Resetar Kernel do Jupyter

Se os erros persistirem após instalar pacotes:

1. No VS Code, abra a paleta de comandos (`Ctrl+Shift+P`)
2. Procure por "Jupyter: Restart Kernel"
3. Execute novamente a célula que gerou erro

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
| numpy | >=1.26.4,<2 | Compatibilidade com TensorFlow 2.13+ |
| scipy | >=1.14.0 | Requer NumPy 1.26.4+ |
| scikit-learn | >=1.5.0 | Compatível com scipy 1.14+ |
| tensorflow | >=2.13.0 | Deep Learning para Transfer Learning |
| opencv-python | >=4.8.0 | Processamento de imagens e detecção |
| albumentations | >=1.4.0 | Data augmentation para treinamento |

---

## 💡 Dicas

- Sempre instale `requirements.txt` antes de executar novos notebooks
- Se um kernel não funciona, reinicie-o através do VS Code
- Para desenvolvimento local, use um ambiente virtual Python isolado
