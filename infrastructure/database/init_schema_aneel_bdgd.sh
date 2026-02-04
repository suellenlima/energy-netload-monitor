#!/bin/bash
# ============================================================================
# Script de Inicialização - ANEEL BDGD Schema
# ============================================================================
# Uso:
#   bash init_schema_aneel_bdgd.sh
#   bash init_schema_aneel_bdgd.sh --host localhost --user postgres --password 
#   bash init_schema_aneel_bdgd.sh --database energia_db
# ============================================================================

set -e

# Configurações padrão
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD:-}
DB_NAME=${DB_NAME:-energia_db}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_FILE="$SCRIPT_DIR/schema_aneel_bdgd.sql"

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Funções auxiliares
print_header() {
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# Parse argumentos
while [[ $# -gt 0 ]]; do
    case $1 in
        --host) DB_HOST="$2"; shift 2 ;;
        --port) DB_PORT="$2"; shift 2 ;;
        --user) DB_USER="$2"; shift 2 ;;
        --password) DB_PASSWORD="$2"; shift 2 ;;
        --database) DB_NAME="$2"; shift 2 ;;
        --help) 
            echo "Uso: $0 [--host HOST] [--port PORT] [--user USER] [--password PASS] [--database DB]"
            exit 0
            ;;
        *) print_error "Argumento desconhecido: $1"; exit 1 ;;
    esac
done

# Exibir configurações
print_header "INICIALIZAR SCHEMA ANEEL BDGD"
echo "Banco de Dados: $DB_HOST:$DB_PORT/$DB_NAME"
echo "Usuário: $DB_USER"
echo "Schema File: $SCHEMA_FILE"
echo ""

# Verificar se arquivo existe
if [ ! -f "$SCHEMA_FILE" ]; then
    print_error "Arquivo não encontrado: $SCHEMA_FILE"
    exit 1
fi

print_success "Arquivo schema encontrado"

# Preparar variáveis de ambiente para psql
export PGHOST="$DB_HOST"
export PGPORT="$DB_PORT"
export PGUSER="$DB_USER"
export PGPASSWORD="$DB_PASSWORD"
export PGDATABASE="$DB_NAME"

# Testar conexão
print_info "Testando conexão com banco de dados..."
if psql -c "SELECT 1" >/dev/null 2>&1; then
    print_success "Conexão estabelecida"
else
    print_error "Falha ao conectar ao banco de dados"
    print_info "Verifique suas credenciais e tente novamente"
    exit 1
fi

# Criar banco se não existir
print_info "Verificando/criando banco de dados..."
psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1 || \
    psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -c "CREATE DATABASE $DB_NAME;" 2>/dev/null || true
print_success "Banco de dados pronto"

# Aplicar schema
print_info "Aplicando schema ANEEL BDGD..."
echo ""

if psql -f "$SCHEMA_FILE" 2>&1; then
    print_success "Schema aplicado com sucesso!"
else
    print_error "Erro ao aplicar schema"
    exit 1
fi

echo ""

# Verificar tabelas criadas
print_info "Verificando tabelas criadas..."
TABLES=$(psql -tc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';")
echo "  Total de tabelas: $TABLES"

# Listar tabelas principais
print_info "Tabelas principais:"
psql -c "
    SELECT 
        tablename,
        (SELECT count(*) FROM information_schema.columns WHERE table_name = tablename) as colunas
    FROM pg_catalog.pg_tables 
    WHERE schemaname = 'public' 
      AND tablename LIKE '%aneel%'
    ORDER BY tablename;
" 2>/dev/null || true

echo ""

# Verificar extensões
print_info "Verificando extensões PostGIS..."
if psql -c "CREATE EXTENSION IF NOT EXISTS postgis;" >/dev/null 2>&1; then
    print_success "PostGIS disponível"
else
    print_warning "PostGIS não disponível (requer instalação)"
fi

echo ""

# Exemplo de uso
print_header "PRÓXIMOS PASSOS"
echo ""
echo "1. Inserir dados:"
echo "   psql -f dados_transformadores.sql"
echo ""
echo "2. Calcular áreas (via SQL):"
echo "   SELECT * FROM sp_calcular_area_transformadores('BT', 'IENERGIA_87');"
echo ""
echo "3. Validar dados:"
echo "   SELECT * FROM sp_validar_integridade();"
echo ""
echo "4. Consultar resultados:"
echo "   SELECT * FROM v_aneel_cobertura_resumo;"
echo ""

print_success "Inicialização completa!"
