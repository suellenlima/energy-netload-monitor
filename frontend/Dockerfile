FROM python:3.10-slim
WORKDIR /app

# Copiar e instalar dependências ANTES de copiar o código
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código por último para aproveitar cache
COPY . .

EXPOSE 8501