import logging
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import HttpSettings


def create_session(settings: HttpSettings, logger: Optional[logging.Logger] = None) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=settings.retries,
        backoff_factor=settings.backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST", "PUT", "DELETE", "PATCH"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    if logger is not None:
        logger.setLevel(settings.log_level.upper())
    return session


def request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    settings: Optional[HttpSettings] = None,
    logger: Optional[logging.Logger] = None,
    **kwargs,
) -> requests.Response:
    if settings is not None and "timeout" not in kwargs:
        kwargs["timeout"] = settings.timeout_s
    if logger:
        logger.info("Requisicao HTTP %s %s", method.upper(), url)
    response = session.request(method, url, **kwargs)
    if logger:
        logger.info("Resposta HTTP %s %s -> %s", method.upper(), url, response.status_code)
    return response