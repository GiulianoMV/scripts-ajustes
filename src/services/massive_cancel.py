import yaml
import time
import asyncio
import aiohttp
import requests
import polars as pl
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict, Any, List
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

from src.logger import Logger


class MassiveCancel:
    def __init__(
        self,
        input_path:Optional[str]=None,
        output_path:Optional[str]=None,
        settings:Optional[Dict[str, Any]]=None
    ):
        """
        Inicializa a classe de cancelamento massivo de contratos

        Args:
            input_path: Caminho para o arquivo de entrada
            output_path: Caminho para o arquivo de saída
            settings: Configurações da API
        """
        # Configurações
        self.settings = settings or {}

        # Logger
        self.log = Logger(__name__)._init_logger()

        # Caminhos
        self.input_path = Path(input_path) if input_path else Path(
            self.settings.get("paths").get("input_path", "entry/massive_cancel.csv")
        )
        self.output_path = Path(output_path) if output_path else Path(
            self.settings.get("paths").get("output_path", "output/massive_cancel.xlsx")
        )

        # Configuração de sessão para requests síncronas
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)

        # Resultados
        self.contratos = List[List[Any]]=[]

        # Validação de informações iniciadas
        self._validate_parameters()
        self._ensure_output_directory()

    def _validate_parameters(self) -> None:
        """Valida parâmetros obrigatórios."""
        if not self.input_path or not self.input_path.exists:
            raise ValueError(f"input_path deve conter um caminho válido: {self.input_path}")
        
        required_settings = ["url_get_contrato", "url_put_contrato",
                             "url_get_contrato_equip", "url_put_contrato_equip",
                             "url_get_neg", "url_put_neg",
                             "url_get_protocol", "url_put_protocol"]
        # for setting in required_settings:
        #     if not self.settings.get(setting):
        #         raise ValueError(f"{setting} não definido nas configurações.")

    def _ensure_output_path(self) -> None:
        """Garante que o caminho de saída exista."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def _make_request(self, url:str, method:str="GET", payload:Optional[Dict[str, Any]]=None, **kwargs,) -> Optional[Dict[str, Any]]:
        """
        Faz uma requisição HTTP com tratamento de erros.

        Args:
            url: URL da requisição
            method: Método HTTP (GET, PUT, etc.)
            payload: Informações enviadas na requisição (Opcional)
            **kwargs: Argumentos adicionais para requests

        Returns:
            Resposta JSON ou None em caso de erro.
        """
        try:
            match method.upper():
                case "GET":
                    response = self.session.get(url=url, headers={"Content-type": "application/json"}, timeout=10)
                case "PUT":
                    response = self.session.put(url=url, headers={"Content-type": "application/json"}, json=payload, timeout=10)
                case _:
                    raise ValueError(f"Método {method} não suportado.")

            response.raise_for_status()
            if response.status_code==204:   # No content
                return {}
            return response.json()
        except requests.exceptions.RequestException:
            self.log.warning(f"[!] Erro na requisição.", exc_info=True)
            return None

    def _process_contract(self, row:tuple) -> List[List[Any]]:
        """
        Processa um único contrato e retorna seu status já cancelado.

        Args:
            row: Tupla contendo informações de EC, contrato e serial.
        
        Returns:
            Lista com status dos contratos alterados.
        """
        # Primeiro conjunto de requests: coleta de dados do contrato
        response_get_contrato = self._make_request(url=f"{self.settings.get("requests_url").get("url_get_contrato")}{str(getattr(row, "EC"))}", method="GET")
        if not response_get_contrato:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    "Não localizado.", "---", "---", "---"]
        response_get_contrato_equip = self._make_request(url=f"{self.settings.get("requests_url").get("url_get_contrato_equip")}{str(getattr(row, "CONTRATO"))}", method="GET")
        if not response_get_contrato_equip:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    "Localizado", "Não localizado", "---", "---"]
        response_get_neg = self._make_request(url=f"{self.settings.get("requests_url").get("url_get_neg")}{str(getattr(row, "CONTRATO"))}", method="GET")
        if not response_get_neg:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    "Localizado", "Localizado", "Não localizado", "---"]
        response_get_protocol = self._make_request(url=f"{self.settings.get("requests_url").get("url_get_protocol")}{str(getattr(row, "CONTRATO"))}", method="GET")
        if not response_get_protocol:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    "Localizado", "Localizado", "Localizado", "Não localizado"]

        # Filtro para coleta do contrato correto
        for contrato in response_get_contrato:
            