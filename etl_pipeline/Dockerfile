FROM python:3.10-slim

WORKDIR /app

# Configurar PYTHONPATH para incluir src
ENV PYTHONPATH=/app/src:$PYTHONPATH

# Dependências para processamento de dados pesados e geo
RUN apt-get update && apt-get install -y \
    libgdal-dev --no-install-recommends && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependências ANTES de copiar o código
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código por último para aproveitar cache
COPY . .