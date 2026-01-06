Run Docker

docker-compose up --build


Abra http://localhost:5050.

Login: admin@energy.com / Senha: admin.


Execute

docker-compose exec db psql -U admin -d energy_monitor

docker-compose exec etl python src/extractors/aneel_client.py
docker-compose exec etl python src/extractors/ons_client.py
docker-compose exec etl python src/extractors/gd_client.py
docker-compose exec etl python src/extractors/inpe_weather_client.py




energy-netload-monitor/
├── .github/                 # CI/CD (GitHub Actions para deploy/testes)
├── .gitignore               # Ignorar venv, dados locais, credenciais
├── docker-compose.yml       # Orquestra todos os containers (DB, API, Airflow)
├── README.md                # Documentação do projeto
├── .env.example             # Exemplo de variáveis de ambiente (DB_HOST, API_KEY)
│
├── 📂 infrastructure/       # Configurações de Infraestrutura (IaC)
│   ├── 📂 database/         # Scripts de inicialização do DB
│   │   ├── init_postgis.sh  # Script para ativar extensão PostGIS
│   │   └── schema.sql       # DDL das tabelas (Carga, Usinas, Satélite)
│   └── 📂 airflow/          # Configurações do Apache Airflow (se usado)
│
├── 📂 data/                 # Armazenamento local temporário (Ignorado no Git)
│   ├── 📂 raw/              # CSVs baixados do ONS/ANEEL
│   ├── 📂 processed/        # Dados limpos prontos para o banco
│   └── 📂 images/           # Tiles de satélite do INPE
│
├── 📂 notebooks/            # Área de "Playground" para Cientistas de Dados
│   ├── 01_exploracao_ons.ipynb
│   ├── 02_validacao_siga.ipynb
│   └── 03_treino_modelo_telhados.ipynb
│
├── 📂 backend/              # Lógica da Aplicação e API (FastAPI)
│   ├── Dockerfile
│   ├── requirements.txt     # Dependências Python
│   └── src/
│       ├── main.py          # Entrypoint da API
│       ├── 📂 api/          # Rotas/Endpoints (ex: /get-hidden-load)
│       ├── 📂 core/         # Configurações globais (Settings)
│       ├── 📂 domain/       # Modelos de Dados (Pydantic/SQLAlchemy)
│       └── 📂 services/     # Lógica de Negócio (O "Cérebro")
│           ├── geospatial.py # Cruzamento de coordenadas (PostGIS logic)
│           └── load_calc.py  # Cálculo de Carga Líquida vs Real
│
├── 📂 etl_pipeline/         # Engenharia de Dados (Workers)
│   ├── Dockerfile
│   └── src/
│       ├── 📂 extractors/   # Scripts que baixam dados
│       │   ├── aneel_client.py
│       │   ├── ons_client.py
│       │   └── inpe_client.py
│       ├── 📂 transformers/ # Limpeza e normalização (Pandas/GeoPandas)
│       └── 📂 loaders/      # Inserção no PostgreSQL
│
└── 📂 frontend/             # Interface do Usuário (React/Streamlit)
    ├── Dockerfile
    ├── package.json
    ├── public/
    └── src/
        ├── 📂 components/   # Mapa, Gráficos, Botões
        └── 📂 services/     # Conexão com o backend (API Client)


1. Separação de "Prototipagem" e "Produção"
Problema: É comum misturar Jupyter Notebooks com código de produção.

Solução: A pasta notebooks/ é onde você testa hipóteses e visualiza os dados do INPE/ONS pela primeira vez. Quando o código funciona, você o refatora e move para etl_pipeline/ ou backend/src/services.

2. Isolamento do Pipeline de Dados (etl_pipeline/)
Os scripts que baixam dados da ANEEL e ONS não devem estar dentro da API. Eles são processos demorados (background jobs).

Ao separá-los, você pode escalar o ETL independentemente da API. Se precisar processar 10 anos de histórico do ONS, isso não vai derrubar o site que o usuário está acessando.

3. Centralização da Infraestrutura (infrastructure/)
Aqui ficam os scripts SQL que criam as tabelas e ativam o PostGIS. Isso é crucial para que, se você apagar tudo e rodar docker-compose up, o ambiente se reconstrua sozinho e pronto para uso.

4. Dados Locais (data/)
Regra de Ouro: Nunca suba dados (CSVs ou Imagens) para o GitHub.

Esta pasta serve como um "cache" local para seus scripts Python. O .gitignore deve garantir que ela não seja versionada, evitando repositórios pesados.


Boas Práticas Embutidas

Modularização: Se a ANEEL mudar o link do arquivo CSV, você só precisa alterar etl_pipeline/src/extractors/aneel_client.py. O resto do sistema nem percebe.

Containerização: Note que backend, frontend e etl_pipeline têm seus próprios Dockerfiles. Isso evita o inferno de dependências (ex: o GeoPandas do ETL não conflita com o Pandas da API).

Domain-Driven Design (DDD) Lite: Na pasta backend/src/services, temos geospatial.py. Ali deve conter apenas a lógica pura de como cruzar mapas, sem se preocupar se o dado veio de uma API ou de um CSV.




Carga Oculta Total = Estimativa ANEEL (Oficial) + Fraudes Detectadas (IA)

