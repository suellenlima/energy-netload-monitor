FROM python:3.10-slim

WORKDIR /app

# Dependências para processamento de dados pesados e geo
RUN apt-get update && apt-get install -y \
    libgdal-dev --no-install-recommends && \
    apt-get clean -y

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .