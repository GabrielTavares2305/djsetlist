@echo off
title DJ Set Analyzer
color 0B

echo.
echo  ============================================
echo    DJ SET ANALYZER
echo    Shazam + YouTube + SoundCloud
echo  ============================================
echo.

:: Verifica Python
python --version >nul 2>&1
IF ERRORLEVEL 1 (
    echo  [ERRO] Python nao encontrado.
    pause
    exit /b 1
)
echo  [OK] Python encontrado

:: Verifica ffmpeg
ffmpeg -version >nul 2>&1
IF ERRORLEVEL 1 (
    echo  [ERRO] ffmpeg nao encontrado.
    echo  Rode: winget install ffmpeg
    pause
    exit /b 1
)
echo  [OK] ffmpeg encontrado

:: Ativa o ambiente virtual
echo.
echo  Ativando ambiente virtual...
IF NOT EXIST "%~dp0venv\Scripts\activate.bat" (
    echo  [ERRO] Ambiente virtual nao encontrado.
    pause
    exit /b 1
)
call "%~dp0venv\Scripts\activate.bat"
echo  [OK] Ambiente virtual ativo

:: Inicia o servidor backend
echo.
echo  Iniciando servidor backend...
start /B python "%~dp0backend\server.py"

:: Aguarda o servidor responder
echo  Aguardando servidor iniciar...
:WAIT_LOOP
timeout /t 2 /nobreak >nul
curl -s http://localhost:5055/api/health >nul 2>&1
IF ERRORLEVEL 1 goto WAIT_LOOP
echo  [OK] Servidor rodando em http://localhost:5055

:: Abre a interface no navegador
echo  Abrindo interface grafica...
start "" "%~dp0frontend\index.html"

echo.
echo  ============================================
echo    DJ Set Analyzer esta rodando!
echo    Feche esta janela para encerrar.
echo  ============================================
echo.

pause