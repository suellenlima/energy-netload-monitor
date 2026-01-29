FROM python:3.10-slim

WORKDIR /app

# Instalar bibliotecas do sistema necessárias para GeoPandas/PostGIS
RUN apt-get update && apt-get install -y \
    libgdal-dev g++ --no-install-recommends && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependências ANTES de copiar o código
# Isso permite reutilizar a layer em rebuilds se requirements.txt não mudar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código do projeto por último
COPY . .

EXPOSE 8000