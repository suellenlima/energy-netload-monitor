from dataclasses import dataclass
from typing import Any, Optional

import requests

from utils.errors import parse_error_response


@dataclass
class ApiResult:
    data: Optional[Any]
    error: Optional[str]
    status_code: Optional[int]


class ApiClient:
    def __init__(self, base_url: str, timeout: int = 2):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get(self, path: str, params: Optional[dict] = None) -> ApiResult:
        url = f"{self.base_url}{path}"
        try:
            resp = requests.get(url, params=params, timeout=self.timeout)
        except requests.RequestException as exc:
            return ApiResult(data=None, error=f"Falha ao conectar ao backend: {exc}", status_code=None)

        if resp.status_code >= 500:
            detail = parse_error_response(resp, "Erro interno do backend.")
            return ApiResult(data=None, error=detail, status_code=resp.status_code)

        if resp.status_code != 200:
            return ApiResult(data=None, error=None, status_code=resp.status_code)

        try:
            data = resp.json()
        except ValueError:
            return ApiResult(data=None, error="Resposta inválida do backend.", status_code=resp.status_code)

        return ApiResult(data=data, error=None, status_code=resp.status_code)