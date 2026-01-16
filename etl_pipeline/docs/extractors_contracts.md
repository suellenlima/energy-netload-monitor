# Extractors Contracts

This document maps each extractor with its inputs, outputs, tables, and schemas.

## Interface (extract/transform/load)
- extract(): fetch raw payload (bytes or local path).
- transform(raw): normalize to a canonical dataframe.
- load(df): persist normalized data; return row count.

## aneel_client.py (ANEEL SIGA)
Input:
- Source: SIGA_URL (CSV).
- Format: sep=';'; encoding='ISO-8859-1'; decimal=','.

Output:
- Table: usinas_siga (PostGIS).
- Schema:
  - ceg: text (nullable).
  - nome: text (nullable).
  - fonte: text (nullable).
  - combustivel: text (nullable).
  - potencia_kw: double precision (not null).
  - latitude: double precision (not null).
  - longitude: double precision (not null).
  - geometry: geometry(Point,4326) (not null).

Notes:
- Rows without latitude/longitude/potencia_kw are dropped.
- Geometry is derived from latitude/longitude in EPSG:4326.

## gd_client.py (ANEEL GD)
Input:
- Source: GD_URL (CSV).
- Format: sep=';'; encoding='latin-1'.
- Cache: ${ETL_RAW_DIR:-/app/data/raw}/gd_temp.csv.

Output:
- Table: gd_detalhada.
- Schema:
  - distribuidora: text (not null).
  - classe: text (not null).
  - sigla_uf: text (not null).
  - fonte: text (not null). Value: "Radiacao Solar".
  - potencia_mw: double precision (not null).

Notes:
- Filters rows where DscFonteGeracao contains "Solar".
- Aggregates by distribuidora/classe/uf.

## inpe_weather_client.py (Open-Meteo archive)
Input:
- Source: https://archive-api.open-meteo.com/v1/archive.
- Params: hourly shortwave_radiation, temperature_2m.
- Timezone: America/Sao_Paulo.
- Window: DIAS_ATRAS, shifted by OFFSET_ANOS.

Output:
- Table: clima_real.
- Schema:
  - time: timestamptz (not null).
  - subsistema: varchar(20) (not null).
  - irradiancia_wm2: double precision (nullable).
  - temperatura_c: double precision (nullable).

Notes:
- Unique constraint on (time, subsistema).
- Deletes the time window before reinsert.

## ons_client.py (ONS carga)
Input:
- Source: https://dados.ons.org.br/api/3/action/package_show?id=carga-energia.
- Finds CSV resource for current or previous year.
- Format: sep=';'; decimal=','.

Output:
- Table: carga_ons.
- Schema:
  - time: timestamp (not null).
  - subsistema: text (not null).
  - carga_mw: double precision (not null).

Notes:
- Column name for carga is detected dynamically.
- Deletes matching time window before insert.
