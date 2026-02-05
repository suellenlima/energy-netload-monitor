"""
Script para acessar Planetary Computer e obter dados Sentinel-2 reais

Dependências:
  pip install pystac-client planetary-computer requests
  
Uso:
  python scripts/fetch_sentinel_data.py <latitude> <longitude> <raio_km>
  
Exemplos:
  python scripts/fetch_sentinel_data.py -3.1190 -60.0217 5    # Manaus
  python scripts/fetch_sentinel_data.py -23.5505 -46.6333 5   # São Paulo
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import requests
from pystac_client import Client
import planetary_computer

def setup_paths():
    """Configura paths do projeto"""
    root_dir = Path(__file__).parent.parent
    backend_dir = root_dir / "backend"
    sys.path.insert(0, str(backend_dir))
    return root_dir, backend_dir


def consultar_planetary_computer(
    latitude: float, 
    longitude: float, 
    raio_km: float = 5,
    data_inicio: str = None,
    data_fim: str = None,
    cobertura_nuvem_max: int = 30
) -> List[Dict]:
    """
    Consulta Planetary Computer STAC para Sentinel-2
    
    Args:
        latitude: Latitude do centro
        longitude: Longitude do centro
        raio_km: Raio de busca em km
        data_inicio: Data no formato YYYY-MM-DD (default: 90 dias atrás)
        data_fim: Data no formato YYYY-MM-DD (default: hoje)
        cobertura_nuvem_max: Cobertura máxima de nuvens (0-100)
    
    Returns:
        Lista de dicts com informações das imagens
    """
    
    print("\n" + "="*70)
    print("CONSULTANDO PLANETARY COMPUTER - SENTINEL-2")
    print("="*70)
    
    # Datas padrão
    if data_fim is None:
        data_fim = datetime.utcnow().date().isoformat()
    if data_inicio is None:
        data_inicio = (datetime.utcnow() - timedelta(days=90)).date().isoformat()
    
    print(f"\n📍 Localização: ({latitude}, {longitude})")
    print(f"📏 Raio: {raio_km} km")
    print(f"📅 Período: {data_inicio} a {data_fim}")
    print(f"☁️  Cobertura máxima: {cobertura_nuvem_max}%")
    
    try:
        # Conectar ao STAC do Planetary Computer
        print("\n🔗 Conectando ao Planetary Computer...")
        client = Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
        
        # Definir bbox (buffer de raio_km)
        buffer_graus = raio_km / 111  # 1 grau ≈ 111 km
        bbox = [
            longitude - buffer_graus,
            latitude - buffer_graus,
            longitude + buffer_graus,
            latitude + buffer_graus,
        ]
        
        # Buscar Sentinel-2
        print("🔍 Buscando Sentinel-2...")
        search = client.search(
            collections=["sentinel-2-l2a"],
            bbox=bbox,
            datetime=f"{data_inicio}T00:00:00Z/{data_fim}T23:59:59Z",
            query={"eo:cloud_cover": {"lt": cobertura_nuvem_max}},
            max_items=10,
        )
        
        # Processar resultados
        imagens = []
        count = 0
        
        for item in search.get_items():
            count += 1
            print(f"\n  ✓ Imagem {count}: {item.id}")
            
            # Extrair informações
            props = item.properties
            assets = item.assets
            
            # URL do asset visual (RGB combinado)
            visual_url = None
            if "visual" in assets:
                visual_url = assets["visual"].href
            elif "TCI_10m" in assets:
                visual_url = assets["TCI_10m"].href
            
            # Aplicar assinatura do Planetary Computer se necessário
            if visual_url and visual_url.startswith("https://"):
                visual_url = planetary_computer.sign_url(visual_url)
            
            imagem_info = {
                "id": item.id,
                "data": item.datetime.isoformat() if item.datetime else props.get("datetime"),
                "sensor": "Sentinel-2",
                "cobertura_nuvem": props.get("eo:cloud_cover", 0),
                "bbox": item.bbox,
                "url_visual": visual_url,
                "url_item": item.get_self_href(),
                "assets": list(assets.keys()),
            }
            
            imagens.append(imagem_info)
            
            print(f"    Data: {imagem_info['data']}")
            print(f"    Nuvens: {imagem_info['cobertura_nuvem']:.1f}%")
            if visual_url:
                print(f"    URL: {visual_url[:80]}...")
        
        if not imagens:
            print("\n⚠️  Nenhuma imagem encontrada com os critérios especificados")
        else:
            print(f"\n✅ {count} imagem(ns) encontrada(s)!")
        
        return imagens
        
    except Exception as e:
        print(f"\n❌ Erro ao consultar: {e}")
        import traceback
        traceback.print_exc()
        return []


def salvar_resultados(imagens: List[Dict], output_file: Optional[str] = None):
    """Salva resultados em JSON"""
    if not output_file:
        output_file = Path(__file__).parent.parent / "data" / "sentinel2_results.json"
    
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(imagens, f, indent=2)
    
    print(f"\n💾 Resultados salvos em: {output_file}")
    return output_file


def main():
    """Main"""
    
    # Exemplos padrão (Manaus)
    if len(sys.argv) == 1:
        print("\n📋 Uso: python scripts/fetch_sentinel_data.py <latitude> <longitude> [raio_km]")
        print("\nExemplos:")
        print("  python scripts/fetch_sentinel_data.py -3.1190 -60.0217 5    # Manaus")
        print("  python scripts/fetch_sentinel_data.py -23.5505 -46.6333 5   # São Paulo")
        print("  python scripts/fetch_sentinel_data.py -15.7936 -47.8822 5   # Brasília")
        
        latitude, longitude, raio = -3.1190, -60.0217, 5
        print(f"\n🚀 Executando exemplo: Manaus ({latitude}, {longitude})")
    else:
        latitude = float(sys.argv[1])
        longitude = float(sys.argv[2])
        raio = float(sys.argv[3]) if len(sys.argv) > 3 else 5
    
    # Consultar
    imagens = consultar_planetary_computer(latitude, longitude, raio)
    
    # Salvar
    if imagens:
        salvar_resultados(imagens)
        
        print("\n" + "="*70)
        print("PRÓXIMOS PASSOS")
        print("="*70)
        print("\n1. Registrar imagens no banco:")
        print("   POST http://localhost:8000/satelite/subestacao/1/consultar-e-registrar")
        print("\n2. Segmentar telhados:")
        print("   POST http://localhost:8000/telhados/segmentar-subestacao")
        print("   JSON: {")
        print('     "id_subestacao": 1,')
        print('     "url_imagem": "<url_visual_da_imagem>",')
        print('     "confianca_minima": 0.5,')
        print('     "extrair_rois": true')
        print("   }")
    else:
        print("\n❌ Nenhuma imagem para processar")
    
    return len(imagens) > 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
