"""
Template para Integração com APIs de Distribuidoras

Este arquivo serve como TEMPLATE para conectar com APIs privadas de distribuidoras.
Substitua as URLs, credenciais e mapeamentos conforme sua distribuidora específica.

Distribuidoras brasileiras com APIs conhecidas:
- CEB (Brasília)
- CPFL (São Paulo interior)
- Light (Rio de Janeiro)
- Enel (São Paulo, Rio, Ceará)
- EDP (São Paulo, Espírito Santo)

Autor: Energy Netload Monitor
Data: 2026-01-31
"""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


# ============================================================================
# TEMPLATE 1: CEB - Companhia Energética de Brasília
# ============================================================================

class CEBClient:
    """
    Cliente para API da CEB (Companhia Energética de Brasília).
    
    ATENÇÃO: Este é um TEMPLATE. Você precisa:
    1. Obter credenciais com a CEB (API key, usuário/senha)
    2. Verificar a documentação oficial da API
    3. Ajustar endpoints e mapeamentos conforme documentação
    4. Configurar VPN se necessário
    """
    
    def __init__(self, base_url: str, api_key: str, username: str = None, password: str = None):
        """
        Inicializa cliente CEB.
        
        Args:
            base_url: URL base da API (ex: https://api.ceb.com.br/v1)
            api_key: Chave de API fornecida pela CEB
            username: Usuário (se autenticação for Basic)
            password: Senha (se autenticação for Basic)
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.username = username
        self.password = password
        
        # Configurar sessão com retry automático
        self.session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Configurar headers
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Se usar Basic Auth
        if username and password:
            self.session.auth = (username, password)
    
    def listar_subestacoes(self) -> List[Dict]:
        """
        Lista todas as subestações da CEB.
        
        AJUSTAR conforme API real:
        - Endpoint correto
        - Paginação (se necessário)
        - Mapeamento de campos
        """
        endpoint = f"{self.base_url}/subestacoes"
        
        try:
            response = self.session.get(endpoint, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # AJUSTAR mapeamento conforme estrutura real
            subestacoes = []
            for item in data.get('subestacoes', []):
                subestacoes.append({
                    'codigo': item['codigo'],
                    'nome': item['nome'],
                    'latitude': item['coordenadas']['latitude'],
                    'longitude': item['coordenadas']['longitude'],
                    'tensao_kv': item['tensao_nominal'],
                    'tipo': item.get('tipo', 'distribuicao'),
                    'bairro': item.get('bairro'),
                    'cidade': item.get('cidade', 'Brasília'),
                    'estado': 'DF'
                })
            
            logger.info(f"✅ {len(subestacoes)} subestações da CEB")
            return subestacoes
            
        except Exception as e:
            logger.error(f"❌ Erro ao listar subestações CEB: {e}")
            return []
    
    def listar_transformadores(self, subestacao_codigo: str) -> List[Dict]:
        """
        Lista transformadores de uma subestação.
        
        AJUSTAR conforme API real.
        """
        endpoint = f"{self.base_url}/subestacoes/{subestacao_codigo}/transformadores"
        
        try:
            response = self.session.get(endpoint, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # AJUSTAR mapeamento
            transformadores = []
            for item in data.get('transformadores', []):
                transformadores.append({
                    'codigo': item['codigo'],
                    'nome': item.get('identificacao', f"TR-{item['codigo']}"),
                    'latitude': item['localizacao']['latitude'],
                    'longitude': item['localizacao']['longitude'],
                    'potencia_kva': item['potencia_nominal'],
                    'tipo': item.get('tipo', 'aereo'),
                    'tensao_primaria_kv': item.get('tensao_primaria', 13.8),
                    'tensao_secundaria_v': item.get('tensao_secundaria', 220),
                    'status': item.get('status', 'ativo'),
                    'data_instalacao': item.get('data_instalacao')
                })
            
            logger.info(f"✅ {len(transformadores)} transformadores da SE {subestacao_codigo}")
            return transformadores
            
        except Exception as e:
            logger.error(f"❌ Erro ao listar transformadores: {e}")
            return []
    
    def listar_consumidores(self, transformador_codigo: str) -> List[Dict]:
        """
        Lista consumidores conectados a um transformador.
        
        AJUSTAR conforme API real.
        ATENÇÃO: Dados sensíveis! Respeitar LGPD.
        """
        endpoint = f"{self.base_url}/transformadores/{transformador_codigo}/consumidores"
        
        try:
            response = self.session.get(endpoint, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # AJUSTAR mapeamento
            consumidores = []
            for item in data.get('consumidores', []):
                consumidores.append({
                    'codigo_cliente': item['numero_instalacao'],
                    # NÃO armazenar nome/CPF (LGPD)
                    'latitude': item['coordenadas']['latitude'],
                    'longitude': item['coordenadas']['longitude'],
                    'tipo_cliente': item['classe'],
                    'grupo_tarifario': item.get('grupo', 'B1'),
                    'consumo_medio_kwh': item.get('consumo_medio_mensal'),
                    'status': item.get('status', 'ativo')
                })
            
            logger.info(f"✅ {len(consumidores)} consumidores do TR {transformador_codigo}")
            return consumidores
            
        except Exception as e:
            logger.error(f"❌ Erro ao listar consumidores: {e}")
            return []


# ============================================================================
# TEMPLATE 2: CPFL - Companhia Paulista de Força e Luz
# ============================================================================

class CPFLClient:
    """
    Cliente para API da CPFL.
    
    ATENÇÃO: Template - ajustar conforme documentação real.
    """
    
    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self.base_url = base_url.rstrip('/')
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.session = requests.Session()
    
    def autenticar(self):
        """
        Obtém access token OAuth2.
        
        AJUSTAR conforme fluxo real (OAuth2, API Key, etc.)
        """
        endpoint = f"{self.base_url}/oauth/token"
        
        try:
            response = self.session.post(
                endpoint,
                data={
                    'grant_type': 'client_credentials',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            self.access_token = data['access_token']
            self.session.headers.update({
                'Authorization': f'Bearer {self.access_token}'
            })
            
            logger.info("✅ Autenticado na API CPFL")
            
        except Exception as e:
            logger.error(f"❌ Erro ao autenticar CPFL: {e}")
            raise
    
    def buscar_rede_distribuicao(self, municipio: str) -> List[Dict]:
        """
        Busca rede de distribuição de um município.
        
        AJUSTAR conforme API real.
        """
        if not self.access_token:
            self.autenticar()
        
        endpoint = f"{self.base_url}/rede/municipio/{municipio}"
        
        try:
            response = self.session.get(endpoint, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            # AJUSTAR mapeamento
            return data.get('elementos', [])
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar rede CPFL: {e}")
            return []


# ============================================================================
# TEMPLATE 3: SCADA Genérico (IEC 61850, DNP3, Modbus)
# ============================================================================

class SCADAGenericClient:
    """
    Cliente genérico para sistemas SCADA.
    
    Protocolos comuns:
    - IEC 61850 (padrão internacional)
    - DNP3 (legacy)
    - Modbus TCP
    - OPC UA
    
    ATENÇÃO: Requer biblioteca específica do protocolo.
    """
    
    def __init__(self, host: str, port: int, protocol: str = 'iec61850'):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.client = None
    
    def conectar(self):
        """
        Conecta ao SCADA conforme protocolo.
        
        EXEMPLOS (requer bibliotecas):
        - IEC 61850: pip install libiec61850
        - DNP3: pip install pydnp3
        - Modbus: pip install pymodbus
        - OPC UA: pip install opcua
        """
        if self.protocol == 'iec61850':
            # from iec61850 import IedConnection
            # self.client = IedConnection.create()
            # self.client.connect(self.host, self.port)
            pass
        
        elif self.protocol == 'modbus':
            # from pymodbus.client import ModbusTcpClient
            # self.client = ModbusTcpClient(self.host, port=self.port)
            pass
        
        logger.info(f"✅ Conectado ao SCADA ({self.protocol})")
    
    def ler_medidas(self, device_id: str) -> Dict:
        """
        Lê medidas de um dispositivo.
        
        AJUSTAR conforme protocolo específico.
        """
        # Exemplo genérico
        return {
            'tensao_fase_a': 13800.0,
            'tensao_fase_b': 13850.0,
            'tensao_fase_c': 13820.0,
            'corrente_fase_a': 145.2,
            'corrente_fase_b': 142.8,
            'corrente_fase_c': 147.1,
            'potencia_ativa_kw': 3240.5,
            'potencia_reativa_kvar': 458.3,
            'timestamp': datetime.now()
        }


# ============================================================================
# FUNÇÃO AUXILIAR: Carregar Credenciais
# ============================================================================

def carregar_credenciais_distribuidora(distribuidora: str) -> Dict:
    """
    Carrega credenciais de arquivo .env ou variáveis de ambiente.
    
    Exemplo de .env:
    
    # CEB
    CEB_API_URL=https://api.ceb.com.br/v1
    CEB_API_KEY=sua_api_key_aqui
    CEB_USERNAME=seu_usuario
    CEB_PASSWORD=sua_senha
    
    # CPFL
    CPFL_API_URL=https://api.cpfl.com.br/v2
    CPFL_CLIENT_ID=seu_client_id
    CPFL_CLIENT_SECRET=seu_client_secret
    
    # SCADA
    SCADA_HOST=192.168.1.100
    SCADA_PORT=102
    SCADA_PROTOCOL=iec61850
    """
    
    distribuidora = distribuidora.upper()
    
    if distribuidora == 'CEB':
        return {
            'base_url': os.getenv('CEB_API_URL'),
            'api_key': os.getenv('CEB_API_KEY'),
            'username': os.getenv('CEB_USERNAME'),
            'password': os.getenv('CEB_PASSWORD')
        }
    
    elif distribuidora == 'CPFL':
        return {
            'base_url': os.getenv('CPFL_API_URL'),
            'client_id': os.getenv('CPFL_CLIENT_ID'),
            'client_secret': os.getenv('CPFL_CLIENT_SECRET')
        }
    
    elif distribuidora == 'SCADA':
        return {
            'host': os.getenv('SCADA_HOST'),
            'port': int(os.getenv('SCADA_PORT', 102)),
            'protocol': os.getenv('SCADA_PROTOCOL', 'iec61850')
        }
    
    else:
        raise ValueError(f"Distribuidora '{distribuidora}' não configurada")


# ============================================================================
# EXEMPLO DE USO
# ============================================================================

def exemplo_uso_ceb():
    """Exemplo de como usar o cliente CEB"""
    
    # 1. Carregar credenciais
    creds = carregar_credenciais_distribuidora('CEB')
    
    # 2. Criar cliente
    client = CEBClient(
        base_url=creds['base_url'],
        api_key=creds['api_key'],
        username=creds['username'],
        password=creds['password']
    )
    
    # 3. Listar subestações
    subestacoes = client.listar_subestacoes()
    print(f"Total de subestações: {len(subestacoes)}")
    
    # 4. Para cada subestação, listar transformadores
    for se in subestacoes[:5]:  # Primeiras 5
        print(f"\n📍 {se['nome']}")
        transformadores = client.listar_transformadores(se['codigo'])
        print(f"   Transformadores: {len(transformadores)}")
        
        # 5. Para cada transformador, listar consumidores
        for tr in transformadores[:2]:  # Primeiros 2
            consumidores = client.listar_consumidores(tr['codigo'])
            print(f"   └─ {tr['nome']}: {len(consumidores)} consumidores")


# ============================================================================
# INTEGRAÇÃO COM ETL PRINCIPAL
# ============================================================================

def integrar_com_etl():
    """
    Integra dados da distribuidora com ETL principal.
    
    Adicione ao etl_area_cobertura_real.py:
    """
    
    from etl_pipeline.src.extractors.area_cobertura_real import DatabaseConnection
    
    # 1. Buscar dados da distribuidora
    creds = carregar_credenciais_distribuidora('CEB')
    client = CEBClient(**creds)
    
    subestacoes = client.listar_subestacoes()
    
    # 2. Carregar no banco
    with DatabaseConnection() as cursor:
        for se in subestacoes:
            cursor.execute("""
                INSERT INTO subestacoes_detectadas (
                    nome, latitude, longitude, localizacao,
                    tensao_nominal_kv, distribuidora, fonte_dados
                ) VALUES (
                    %s, %s, %s, 
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                    %s, %s, %s
                )
                ON CONFLICT (nome, latitude, longitude) DO UPDATE SET
                    tensao_nominal_kv = EXCLUDED.tensao_nominal_kv,
                    fonte_dados = EXCLUDED.fonte_dados
            """, (
                se['nome'],
                se['latitude'],
                se['longitude'],
                se['longitude'],
                se['latitude'],
                se['tensao_kv'],
                'CEB',
                'CEB'
            ))
            
            # Buscar ID da subestação
            cursor.execute("""
                SELECT id FROM subestacoes_detectadas
                WHERE nome = %s AND latitude = %s AND longitude = %s
            """, (se['nome'], se['latitude'], se['longitude']))
            
            se_id = cursor.fetchone()[0]
            
            # Listar transformadores
            transformadores = client.listar_transformadores(se['codigo'])
            
            for tr in transformadores:
                cursor.execute("""
                    INSERT INTO transformadores (
                        codigo, subestacao_id, nome, latitude, longitude,
                        localizacao, potencia_kva, tipo, status,
                        tensao_primaria_kv, tensao_secundaria_v, fonte_dados
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                        %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (codigo) DO UPDATE SET
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        status = EXCLUDED.status,
                        updated_at = NOW()
                """, (
                    tr['codigo'],
                    se_id,
                    tr['nome'],
                    tr['latitude'],
                    tr['longitude'],
                    tr['longitude'],
                    tr['latitude'],
                    tr['potencia_kva'],
                    tr['tipo'],
                    tr['status'],
                    tr['tensao_primaria_kv'],
                    tr['tensao_secundaria_v'],
                    'CEB'
                ))
    
    logger.info(f"✅ {len(subestacoes)} subestações CEB carregadas")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 80)
    print("TEMPLATE: Integração com APIs de Distribuidoras")
    print("=" * 80)
    print()
    print("⚠️  ATENÇÃO: Este é um TEMPLATE!")
    print()
    print("Para usar com sua distribuidora:")
    print("1. Obtenha credenciais (API key, usuário/senha)")
    print("2. Consulte documentação oficial da API")
    print("3. Ajuste URLs, endpoints e mapeamentos")
    print("4. Configure arquivo .env com credenciais")
    print("5. Execute integrar_com_etl()")
    print()
    print("Distribuidoras com APIs conhecidas:")
    print("- CEB (Brasília)")
    print("- CPFL (São Paulo)")
    print("- Light (Rio de Janeiro)")
    print("- Enel (SP, RJ, CE)")
    print("- EDP (SP, ES)")
    print()
    print("Contato: Fale com o setor de TI da sua distribuidora local")
    print("=" * 80)
