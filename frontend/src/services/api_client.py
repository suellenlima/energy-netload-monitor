"""Cliente HTTP para comunicação com a API do backend."""

import logging
from dataclasses import dataclass
from typing import Any, Optional

import requests

from utils.errors import parse_error_response

logger = logging.getLogger(__name__)


@dataclass
class ApiResult:
    """Resultado de uma chamada à API."""

    data: Optional[Any]
    error: Optional[str]
    status_code: Optional[int]


class ApiClient:
    """Cliente HTTP para comunicação com o backend."""

    def __init__(self, base_url: str, timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _handle_response(self, resp: requests.Response, method: str, url: str) -> ApiResult:
        """
        Processa a resposta HTTP e retorna ApiResult.

        Args:
            resp: Resposta HTTP
            method: Método HTTP usado (GET, POST, etc)
            url: URL da requisição

        Returns:
            ApiResult com dados ou erro
        """
        # Erro do servidor (5xx)
        if resp.status_code >= 500:
            detail = parse_error_response(resp, "Erro interno do backend.")
            logger.error(f"[{method}] {url} - Erro 5xx: {resp.status_code} - {detail}")
            return ApiResult(data=None, error=detail, status_code=resp.status_code)

        # Erro do cliente (4xx)
        if resp.status_code >= 400:
            detail = parse_error_response(resp, f"Erro na requisição: {resp.status_code}")
            logger.warning(f"[{method}] {url} - Erro 4xx: {resp.status_code} - {detail}")
            return ApiResult(data=None, error=detail, status_code=resp.status_code)

        # Resposta sem conteúdo (204 No Content)
        if resp.status_code == 204:
            logger.debug(f"[{method}] {url} - 204 No Content")
            return ApiResult(data=None, error=None, status_code=resp.status_code)

        # Sucesso (2xx)
        if resp.status_code >= 200 and resp.status_code < 300:
            try:
                data = resp.json()
                logger.debug(f"[{method}] {url} - Sucesso: {resp.status_code}")
                return ApiResult(data=data, error=None, status_code=resp.status_code)
            except ValueError:
                logger.error(f"[{method}] {url} - Resposta JSON inválida")
                return ApiResult(data=None, error="Resposta inválida do backend.", status_code=resp.status_code)

        # Outros status codes
        logger.warning(f"[{method}] {url} - Status inesperado: {resp.status_code}")
        return ApiResult(data=None, error=None, status_code=resp.status_code)

    def get(self, path: str, params: Optional[dict] = None) -> ApiResult:
        """
        Realiza requisição GET.

        Args:
            path: Caminho do endpoint (ex: /subestacoes/ons)
            params: Query parameters opcionais

        Returns:
            ApiResult com dados ou erro
        """
        url = f"{self.base_url}{path}"
        logger.debug(f"[GET] {url} params={params}")

        try:
            resp = requests.get(url, params=params, timeout=self.timeout)
        except requests.RequestException as exc:
            logger.error(f"[GET] {url} - Falha de conexão: {exc}")
            return ApiResult(data=None, error=f"Falha ao conectar ao backend: {exc}", status_code=None)

        return self._handle_response(resp, "GET", url)

    def post(
        self,
        path: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
    ) -> ApiResult:
        """
        Realiza requisição POST.

        Args:
            path: Caminho do endpoint (ex: /subestacoes/detectadas/atualizar)
            params: Query parameters opcionais
            json_data: Corpo da requisição em JSON (opcional)

        Returns:
            ApiResult com dados ou erro
        """
        url = f"{self.base_url}{path}"
        logger.debug(f"[POST] {url} params={params} json={json_data}")

        try:
            resp = requests.post(url, params=params, json=json_data, timeout=self.timeout)
        except requests.RequestException as exc:
            logger.error(f"[POST] {url} - Falha de conexão: {exc}")
            return ApiResult(data=None, error=f"Falha ao conectar ao backend: {exc}", status_code=None)

        return self._handle_response(resp, "POST", url)
