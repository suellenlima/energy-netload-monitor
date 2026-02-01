#!/usr/bin/env python3
"""
Wrapper para iniciar FastAPI backend
"""
import sys
import os

# Força Python path correto
sys.path.insert(0, r'c:\Hackathon\Git\energy-netload-monitor\backend')
os.chdir(r'c:\Hackathon\Git\energy-netload-monitor\backend')

# Verifica imports STAC
try:
    from pystac_client import Client
    import planetary_computer
    print("✅ pystac-client e planetary-computer importados com sucesso")
except ImportError as e:
    print(f"❌ Erro ao importar: {e}")
    sys.exit(1)

# Inicia uvicorn
sys.argv = ['uvicorn', 'src.main:app', '--host', '0.0.0.0', '--port', '8000']
from uvicorn import main
main()
