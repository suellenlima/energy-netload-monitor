FROM python:3.10-slim

WORKDIR /app

# Instalar bibliotecas do sistema necessárias para GeoPandas/PostGIS
RUN apt-get update && apt-get install -y \
    libgdal-dev g++ --no-install-recommends && \
    apt-get clean -y

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000