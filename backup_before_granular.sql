pg_dump: warning: there are circular foreign-key constraints on this table:
pg_dump: detail: hypertable
pg_dump: hint: You might not be able to restore the dump without using --disable-triggers or temporarily dropping the constraints.
pg_dump: hint: Consider using a full dump instead of a --data-only dump to avoid this problem.
pg_dump: warning: there are circular foreign-key constraints on this table:
pg_dump: detail: chunk
pg_dump: hint: You might not be able to restore the dump without using --disable-triggers or temporarily dropping the constraints.
pg_dump: hint: Consider using a full dump instead of a --data-only dump to avoid this problem.
pg_dump: warning: there are circular foreign-key constraints on this table:
pg_dump: detail: continuous_agg
pg_dump: hint: You might not be able to restore the dump without using --disable-triggers or temporarily dropping the constraints.
pg_dump: hint: Consider using a full dump instead of a --data-only dump to avoid this problem.
--
-- PostgreSQL database dump
--

\restrict nRDa5KC3QW49pIR5abdfx3rkoI83TGzMIn1Axt4XIA2xUYZEv4JWMFEwlg0IQbw

-- Dumped from database version 15.15 (Ubuntu 15.15-1.pgdg22.04+1)
-- Dumped by pg_dump version 15.15 (Ubuntu 15.15-1.pgdg22.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: timescaledb; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION timescaledb IS 'Enables scalable inserts and complex queries for time-series data (Community Edition)';


--
-- Name: timescaledb_toolkit; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb_toolkit; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION timescaledb_toolkit IS 'Library of analytical hyperfunctions, time-series pipelining, and other SQL utilities';


--
-- Name: topology; Type: SCHEMA; Schema: -; Owner: admin
--

CREATE SCHEMA topology;


ALTER SCHEMA topology OWNER TO admin;

--
-- Name: SCHEMA topology; Type: COMMENT; Schema: -; Owner: admin
--

COMMENT ON SCHEMA topology IS 'PostGIS Topology schema';


--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';


--
-- Name: postgis_topology; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis_topology WITH SCHEMA topology;


--
-- Name: EXTENSION postgis_topology; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis_topology IS 'PostGIS topology spatial types and functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: auditoria_visual; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.auditoria_visual (
    id bigint NOT NULL,
    data_inspecao timestamp with time zone,
    latitude double precision,
    longitude double precision,
    distribuidora text,
    classe_estimada_ia text,
    diferenca_fraude_kw double precision,
    potencia_oficial_kw double precision,
    status text
);


ALTER TABLE public.auditoria_visual OWNER TO admin;

--
-- Name: auditoria_visual_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

CREATE SEQUENCE public.auditoria_visual_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.auditoria_visual_id_seq OWNER TO admin;

--
-- Name: auditoria_visual_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin
--

ALTER SEQUENCE public.auditoria_visual_id_seq OWNED BY public.auditoria_visual.id;


--
-- Name: carga_ons; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.carga_ons (
    "time" timestamp with time zone NOT NULL,
    subsistema text,
    carga_mw double precision
);


ALTER TABLE public.carga_ons OWNER TO admin;

--
-- Name: clima_real; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.clima_real (
    "time" timestamp with time zone NOT NULL,
    subsistema character varying(20),
    irradiancia_wm2 double precision,
    temperatura_c double precision
);


ALTER TABLE public.clima_real OWNER TO admin;

--
-- Name: gd_detalhada; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.gd_detalhada (
    distribuidora text,
    classe text,
    sigla_uf text,
    fonte text,
    potencia_mw double precision
);


ALTER TABLE public.gd_detalhada OWNER TO admin;

--
-- Name: subestacoes_detectadas; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.subestacoes_detectadas (
    id integer NOT NULL,
    cluster_id integer,
    nome text,
    latitude double precision,
    longitude double precision,
    distribuidora text,
    subsistema text,
    quantidade_gd integer,
    potencia_total_mw double precision,
    raio_deteccao_km double precision,
    data_deteccao timestamp with time zone DEFAULT now(),
    geom public.geometry(Point,4326)
);


ALTER TABLE public.subestacoes_detectadas OWNER TO admin;

--
-- Name: subestacoes_detectadas_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

CREATE SEQUENCE public.subestacoes_detectadas_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.subestacoes_detectadas_id_seq OWNER TO admin;

--
-- Name: subestacoes_detectadas_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin
--

ALTER SEQUENCE public.subestacoes_detectadas_id_seq OWNED BY public.subestacoes_detectadas.id;


--
-- Name: subestacoes_ons; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.subestacoes_ons (
    id integer NOT NULL,
    nome text,
    sigla_se text,
    tensao_kv double precision,
    subsistema text,
    distribuidora text,
    latitude double precision,
    longitude double precision,
    fonte_dados text,
    geom public.geometry(Point,4326)
);


ALTER TABLE public.subestacoes_ons OWNER TO admin;

--
-- Name: subestacoes_ons_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

CREATE SEQUENCE public.subestacoes_ons_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.subestacoes_ons_id_seq OWNER TO admin;

--
-- Name: subestacoes_ons_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin
--

ALTER SEQUENCE public.subestacoes_ons_id_seq OWNED BY public.subestacoes_ons.id;


--
-- Name: usinas_siga; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.usinas_siga (
    ceg text,
    nome text,
    fonte text,
    combustivel text,
    potencia_kw double precision,
    latitude double precision,
    longitude double precision,
    geom public.geometry(Point,4326)
);


ALTER TABLE public.usinas_siga OWNER TO admin;

--
-- Name: auditoria_visual id; Type: DEFAULT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.auditoria_visual ALTER COLUMN id SET DEFAULT nextval('public.auditoria_visual_id_seq'::regclass);


--
-- Name: subestacoes_detectadas id; Type: DEFAULT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.subestacoes_detectadas ALTER COLUMN id SET DEFAULT nextval('public.subestacoes_detectadas_id_seq'::regclass);


--
-- Name: subestacoes_ons id; Type: DEFAULT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.subestacoes_ons ALTER COLUMN id SET DEFAULT nextval('public.subestacoes_ons_id_seq'::regclass);


--
-- Data for Name: hypertable; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.hypertable (id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size, compression_state, compressed_hypertable_id, status) FROM stdin;
1	public	carga_ons	_timescaledb_internal	_hyper_1	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
2	public	clima_real	_timescaledb_internal	_hyper_2	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
\.


--
-- Data for Name: chunk; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.chunk (id, hypertable_id, schema_name, table_name, compressed_chunk_id, dropped, status, osm_chunk, creation_time) FROM stdin;
\.


--
-- Data for Name: chunk_column_stats; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.chunk_column_stats (id, hypertable_id, chunk_id, column_name, range_start, range_end, valid) FROM stdin;
\.


--
-- Data for Name: dimension; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.dimension (id, hypertable_id, column_name, column_type, aligned, num_slices, partitioning_func_schema, partitioning_func, interval_length, compress_interval_length, integer_now_func_schema, integer_now_func) FROM stdin;
1	1	time	timestamp with time zone	t	\N	\N	\N	604800000000	\N	\N	\N
2	2	time	timestamp with time zone	t	\N	\N	\N	604800000000	\N	\N	\N
\.


--
-- Data for Name: dimension_slice; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.dimension_slice (id, dimension_id, range_start, range_end) FROM stdin;
\.


--
-- Data for Name: chunk_constraint; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.chunk_constraint (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name) FROM stdin;
\.


--
-- Data for Name: compression_chunk_size; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.compression_chunk_size (chunk_id, compressed_chunk_id, uncompressed_heap_size, uncompressed_toast_size, uncompressed_index_size, compressed_heap_size, compressed_toast_size, compressed_index_size, numrows_pre_compression, numrows_post_compression, numrows_frozen_immediately) FROM stdin;
\.


--
-- Data for Name: compression_settings; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.compression_settings (relid, compress_relid, segmentby, orderby, orderby_desc, orderby_nullsfirst, index) FROM stdin;
\.


--
-- Data for Name: continuous_agg; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.continuous_agg (mat_hypertable_id, raw_hypertable_id, parent_mat_hypertable_id, user_view_schema, user_view_name, partial_view_schema, partial_view_name, direct_view_schema, direct_view_name, materialized_only, finalized) FROM stdin;
\.


--
-- Data for Name: continuous_agg_migrate_plan; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.continuous_agg_migrate_plan (mat_hypertable_id, start_ts, end_ts, user_view_definition) FROM stdin;
\.


--
-- Data for Name: continuous_agg_migrate_plan_step; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.continuous_agg_migrate_plan_step (mat_hypertable_id, step_id, status, start_ts, end_ts, type, config) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_bucket_function; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.continuous_aggs_bucket_function (mat_hypertable_id, bucket_func, bucket_width, bucket_origin, bucket_offset, bucket_timezone, bucket_fixed_width) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_hypertable_invalidation_log; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (hypertable_id, lowest_modified_value, greatest_modified_value) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_invalidation_threshold; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.continuous_aggs_invalidation_threshold (hypertable_id, watermark) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_materialization_invalidation_log; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value, greatest_modified_value) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_materialization_ranges; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.continuous_aggs_materialization_ranges (materialization_id, lowest_modified_value, greatest_modified_value) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_watermark; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.continuous_aggs_watermark (mat_hypertable_id, watermark) FROM stdin;
\.


--
-- Data for Name: metadata; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.metadata (key, value, include_in_telemetry) FROM stdin;
install_timestamp	2026-01-26 01:29:05.003037+00	t
timescaledb_version	2.24.0	f
\.


--
-- Data for Name: tablespace; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: admin
--

COPY _timescaledb_catalog.tablespace (id, hypertable_id, tablespace_name) FROM stdin;
\.


--
-- Data for Name: bgw_job; Type: TABLE DATA; Schema: _timescaledb_config; Owner: admin
--

COPY _timescaledb_config.bgw_job (id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, fixed_schedule, initial_start, hypertable_id, config, check_schema, check_name, timezone) FROM stdin;
\.


--
-- Data for Name: auditoria_visual; Type: TABLE DATA; Schema: public; Owner: admin
--

COPY public.auditoria_visual (id, data_inspecao, latitude, longitude, distribuidora, classe_estimada_ia, diferenca_fraude_kw, potencia_oficial_kw, status) FROM stdin;
\.


--
-- Data for Name: carga_ons; Type: TABLE DATA; Schema: public; Owner: admin
--

COPY public.carga_ons ("time", subsistema, carga_mw) FROM stdin;
\.


--
-- Data for Name: clima_real; Type: TABLE DATA; Schema: public; Owner: admin
--

COPY public.clima_real ("time", subsistema, irradiancia_wm2, temperatura_c) FROM stdin;
\.


--
-- Data for Name: gd_detalhada; Type: TABLE DATA; Schema: public; Owner: admin
--

COPY public.gd_detalhada (distribuidora, classe, sigla_uf, fonte, potencia_mw) FROM stdin;
\.


--
-- Data for Name: spatial_ref_sys; Type: TABLE DATA; Schema: public; Owner: admin
--

COPY public.spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) FROM stdin;
\.


--
-- Data for Name: subestacoes_detectadas; Type: TABLE DATA; Schema: public; Owner: admin
--

COPY public.subestacoes_detectadas (id, cluster_id, nome, latitude, longitude, distribuidora, subsistema, quantidade_gd, potencia_total_mw, raio_deteccao_km, data_deteccao, geom) FROM stdin;
\.


--
-- Data for Name: subestacoes_ons; Type: TABLE DATA; Schema: public; Owner: admin
--

COPY public.subestacoes_ons (id, nome, sigla_se, tensao_kv, subsistema, distribuidora, latitude, longitude, fonte_dados, geom) FROM stdin;
\.


--
-- Data for Name: usinas_siga; Type: TABLE DATA; Schema: public; Owner: admin
--

COPY public.usinas_siga (ceg, nome, fonte, combustivel, potencia_kw, latitude, longitude, geom) FROM stdin;
\.


--
-- Data for Name: topology; Type: TABLE DATA; Schema: topology; Owner: admin
--

COPY topology.topology (id, name, srid, "precision", hasz, useslargeids) FROM stdin;
\.


--
-- Data for Name: layer; Type: TABLE DATA; Schema: topology; Owner: admin
--

COPY topology.layer (topology_id, layer_id, schema_name, table_name, feature_column, feature_type, level, child_id) FROM stdin;
\.


--
-- Name: chunk_column_stats_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: admin
--

SELECT pg_catalog.setval('_timescaledb_catalog.chunk_column_stats_id_seq', 1, false);


--
-- Name: chunk_constraint_name; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: admin
--

SELECT pg_catalog.setval('_timescaledb_catalog.chunk_constraint_name', 1, false);


--
-- Name: chunk_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: admin
--

SELECT pg_catalog.setval('_timescaledb_catalog.chunk_id_seq', 1, false);


--
-- Name: continuous_agg_migrate_plan_step_step_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: admin
--

SELECT pg_catalog.setval('_timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq', 1, false);


--
-- Name: dimension_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: admin
--

SELECT pg_catalog.setval('_timescaledb_catalog.dimension_id_seq', 2, true);


--
-- Name: dimension_slice_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: admin
--

SELECT pg_catalog.setval('_timescaledb_catalog.dimension_slice_id_seq', 1, false);


--
-- Name: hypertable_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: admin
--

SELECT pg_catalog.setval('_timescaledb_catalog.hypertable_id_seq', 2, true);


--
-- Name: bgw_job_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_config; Owner: admin
--

SELECT pg_catalog.setval('_timescaledb_config.bgw_job_id_seq', 1000, false);


--
-- Name: auditoria_visual_id_seq; Type: SEQUENCE SET; Schema: public; Owner: admin
--

SELECT pg_catalog.setval('public.auditoria_visual_id_seq', 1, false);


--
-- Name: subestacoes_detectadas_id_seq; Type: SEQUENCE SET; Schema: public; Owner: admin
--

SELECT pg_catalog.setval('public.subestacoes_detectadas_id_seq', 1, false);


--
-- Name: subestacoes_ons_id_seq; Type: SEQUENCE SET; Schema: public; Owner: admin
--

SELECT pg_catalog.setval('public.subestacoes_ons_id_seq', 1, false);


--
-- Name: topology_id_seq; Type: SEQUENCE SET; Schema: topology; Owner: admin
--

SELECT pg_catalog.setval('topology.topology_id_seq', 1, false);


--
-- Name: auditoria_visual auditoria_visual_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.auditoria_visual
    ADD CONSTRAINT auditoria_visual_pkey PRIMARY KEY (id);


--
-- Name: clima_real clima_real_unique; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.clima_real
    ADD CONSTRAINT clima_real_unique UNIQUE ("time", subsistema);


--
-- Name: subestacoes_detectadas subestacoes_detectadas_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.subestacoes_detectadas
    ADD CONSTRAINT subestacoes_detectadas_pkey PRIMARY KEY (id);


--
-- Name: subestacoes_ons subestacoes_ons_nome_key; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.subestacoes_ons
    ADD CONSTRAINT subestacoes_ons_nome_key UNIQUE (nome);


--
-- Name: subestacoes_ons subestacoes_ons_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.subestacoes_ons
    ADD CONSTRAINT subestacoes_ons_pkey PRIMARY KEY (id);


--
-- Name: clima_real_time_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX clima_real_time_idx ON public.clima_real USING btree ("time" DESC);


--
-- Name: idx_auditoria_visual_distribuidora; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_auditoria_visual_distribuidora ON public.auditoria_visual USING btree (distribuidora);


--
-- Name: idx_carga_ons_subsistema; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_carga_ons_subsistema ON public.carga_ons USING btree (subsistema);


--
-- Name: idx_carga_ons_time; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_carga_ons_time ON public.carga_ons USING btree ("time");


--
-- Name: idx_gd_detalhada_distribuidora; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_gd_detalhada_distribuidora ON public.gd_detalhada USING btree (distribuidora);


--
-- Name: idx_subestacoes_detectadas_cluster; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_subestacoes_detectadas_cluster ON public.subestacoes_detectadas USING btree (cluster_id);


--
-- Name: idx_subestacoes_detectadas_distribuidora; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_subestacoes_detectadas_distribuidora ON public.subestacoes_detectadas USING btree (distribuidora);


--
-- Name: idx_subestacoes_ons_distribuidora; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_subestacoes_ons_distribuidora ON public.subestacoes_ons USING btree (distribuidora);


--
-- Name: idx_subestacoes_ons_subsistema; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_subestacoes_ons_subsistema ON public.subestacoes_ons USING btree (subsistema);


--
-- PostgreSQL database dump complete
--

\unrestrict nRDa5KC3QW49pIR5abdfx3rkoI83TGzMIn1Axt4XIA2xUYZEv4JWMFEwlg0IQbw

