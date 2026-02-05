@echo off
REM ============================================================================
REM Script runner para ETL ANEEL BDGD
REM Facilita a execução dos scripts de extração e validação
REM ============================================================================

setlocal enabledelayedexpansion

echo.
echo ============================================================================
echo  ETL ANEEL BDGD - Menu Principal
echo ============================================================================
echo.

if "%1"=="" (
    echo Opcoes disponveis:
    echo.
    echo   1. Extrair dados de todas as distribuidoras
    echo   2. Extrair dados de uma distribuidora especfica
    echo   3. Gerar relatorios de cobertura
    echo   4. Exportar dados em CSV
    echo   5. Listar camadas disponveis
    echo   6. Limpar duplicatas
    echo   7. Modo completo (tudo)
    echo   8. Modo debug
    echo.
    echo Exemplos:
    echo   run_etl.bat 1
    echo   run_etl.bat 2 "IENERGIA_87_2021-02-28"
    echo   run_etl.bat 3
    echo   run_etl.bat 7
    echo.
    exit /b
)

REM ============================================================================
REM Executar opcoes
REM ============================================================================

if "%1"=="1" (
    echo.
    echo [1/3] Extraindo dados...
    python etl_aneel_bdgd_local.py
    goto end
)

if "%1"=="2" (
    if "%2"=="" (
        echo Erro: Especifique o nome da distribuidora
        echo Exemplo: run_etl_aneel.bat 2 "IENERGIA_87_2021-02-28"
        exit /b 1
    )
    echo.
    echo [1/3] Extraindo dados de %2...
    python etl_aneel_bdgd_local.py --distribuidora "%2"
    goto end
)

if "%1"=="3" (
    echo.
    echo [1/1] Gerando relatorios...
    python aneel_bdgd_validator.py --report
    goto end
)

if "%1"=="4" (
    echo.
    echo [1/1] Exportando dados...
    python aneel_bdgd_validator.py --export
    goto end
)

if "%1"=="5" (
    echo.
    echo [1/1] Listando camadas...
    python aneel_bdgd_validator.py --list-layers
    goto end
)

if "%1"=="6" (
    echo.
    echo [1/1] Limpando duplicatas...
    python aneel_bdgd_validator.py --clean
    goto end
)

if "%1"=="7" (
    echo.
    echo [1/3] Extraindo dados...
    python etl_aneel_bdgd_local.py
    echo.
    echo [2/3] Gerando relatorios e exportando...
    python aneel_bdgd_validator.py --all
    echo.
    echo [3/3] Limpando duplicatas...
    python aneel_bdgd_validator.py --clean
    goto end
)

if "%1"=="8" (
    echo.
    echo [1/3] Extraindo dados em modo DEBUG...
    python etl_aneel_bdgd_local.py --debug
    goto end
)

echo Opcao desconhecida: %1
exit /b 1

:end
echo.
echo ============================================================================
echo  Operacao concluida
echo ============================================================================
echo.
