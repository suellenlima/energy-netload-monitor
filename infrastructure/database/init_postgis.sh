#!/bin/bash
set -e

# Conecta no banco e ativa as extensões
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    -- Opcional: Topologia para análises avançadas
    CREATE EXTENSION IF NOT EXISTS postgis_topology;
EOSQL