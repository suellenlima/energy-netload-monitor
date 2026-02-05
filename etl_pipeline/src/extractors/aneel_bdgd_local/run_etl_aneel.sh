#!/bin/bash
# ============================================================================
# Script runner para ETL ANEEL BDGD
# Facilita a execução dos scripts de extração e validação
# ============================================================================

cd "$(dirname "$0")"

echo ""
echo "============================================================================"
echo " ETL ANEEL BDGD - Menu Principal"
echo "============================================================================"
echo ""

if [ $# -eq 0 ]; then
    echo "Opções disponíveis:"
    echo ""
    echo "  1. Extrair dados de todas as distribuidoras"
    echo "  2. Extrair dados de uma distribuidora específica"
    echo "  3. Gerar relatórios de cobertura"
    echo "  4. Exportar dados em CSV"
    echo "  5. Listar camadas disponíveis"
    echo "  6. Limpar duplicatas"
    echo "  7. Modo completo (tudo)"
    echo "  8. Modo debug"
    echo ""
    echo "Exemplos:"
    echo "  ./run_etl_aneel.sh 1"
    echo "  ./run_etl_aneel.sh 2 'IENERGIA_87_2021-02-28'"
    echo "  ./run_etl_aneel.sh 3"
    echo "  ./run_etl_aneel.sh 7"
    echo ""
    exit 0
fi

# ============================================================================
# Executar opções
# ============================================================================

case "$1" in
    1)
        echo ""
        echo "[1/3] Extraindo dados..."
        python etl_aneel_bdgd_local.py
        ;;
    2)
        if [ -z "$2" ]; then
            echo "Erro: Especifique o nome da distribuidora"
            echo "Exemplo: ./run_etl_aneel.sh 2 'IENERGIA_87_2021-02-28'"
            exit 1
        fi
        echo ""
        echo "[1/3] Extraindo dados de $2..."
        python etl_aneel_bdgd_local.py --distribuidora "$2"
        ;;
    3)
        echo ""
        echo "[1/1] Gerando relatórios..."
        python aneel_bdgd_validator.py --report
        ;;
    4)
        echo ""
        echo "[1/1] Exportando dados..."
        python aneel_bdgd_validator.py --export
        ;;
    5)
        echo ""
        echo "[1/1] Listando camadas..."
        python aneel_bdgd_validator.py --list-layers
        ;;
    6)
        echo ""
        echo "[1/1] Limpando duplicatas..."
        python aneel_bdgd_validator.py --clean
        ;;
    7)
        echo ""
        echo "[1/3] Extraindo dados..."
        python etl_aneel_bdgd_local.py
        echo ""
        echo "[2/3] Gerando relatórios e exportando..."
        python aneel_bdgd_validator.py --all
        echo ""
        echo "[3/3] Limpando duplicatas..."
        python aneel_bdgd_validator.py --clean
        ;;
    8)
        echo ""
        echo "[1/3] Extraindo dados em modo DEBUG..."
        python etl_aneel_bdgd_local.py --debug
        ;;
    *)
        echo "Opção desconhecida: $1"
        exit 1
        ;;
esac

echo ""
echo "============================================================================"
echo " Operação concluída"
echo "============================================================================"
echo ""
