FROM python:3.11-slim

WORKDIR /app

# Instalar bibliotecas do sistema necessárias para GeoPandas/PostGIS e OpenCV
RUN apt-get update && apt-get install -y \
    libgdal-dev g++ \
    libgl1 \
    libglib2.0-0 \
    --no-install-recommends && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependências ANTES de copiar o código
# Isso permite reutilizar a layer em rebuilds se requirements.txt não mudar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código do projeto por último
COPY . .

EXPOSE 8000

CMD ["sh", "-c", "uvicorn backend.src.main:app --host 0.0.0.0 --port ${PORT:-8000}"]