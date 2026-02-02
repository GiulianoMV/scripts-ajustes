import yaml
import aiohttp
import requests
import polars as pl
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
import time
from tqdm import tqdm

from src.logger import Logger


class IsentaContratos:
    def __init__(
        self,
        input_path: Optional[str]=None,
        output_path: Optional[str]=None,
        settings: Optional[Dict[str, Any]]=None
    ):
        """
        Inicializa a classe de isenção de contratos.
        
        Args:
            input_path: Caminho para o arquivo de entrada
            output_path: Caminho para o arquivo de saída
            settings: Configurações da API (URLs, etc.)
        """
        # Configurações
        self.settings = settings or {}

        # Logger
        self.log = Logger(name=__name__)._init_logger()

        #Caminhos
        self.input_path = Path(input_path) if input_path else Path(
            self.settings.get(("input_path", "entry/isenta_contratos.csv"))
        )
        self.output_path = Path(output_path) if output_path else Path(
            self.settings.get("output_path", "output/isenta_contratos.xlsx")
        )

        # Configurações de sessão para requests síncronos
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500,502,503,504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)

        # Resultados
        self.contratos: List[List[Any]] = []

        # Validação de informações iniciais
        self._validate_parameters()
        self._ensure_output_directory()

    def _validate_parameters(self) -> None:
        """Valida parâmetros obrigatórios."""
        if not self.input_path or not self.input_path.exists:
            raise ValueError(f"input_path deve conter um caminho válido: {self.input_path}")
        
        required_settings = ["url_get", "url_put", "valor"]
        for setting in required_settings:
            if not self.settings.get(setting):
                raise ValueError(f"{setting} não definido nas configurações.")
            
    def _ensure_output_directory(self) -> None:
        """Garante que o caminho de saída exista."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def _make_request(self, url:str, method:str="GET", payload:Optional[Dict[str, Any]]=None, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Faz uma requisição HTTP com tratamento de erros.
        
        Args:
            url: URL da requisição
            method: Método HTTP (GET, PUT, etc.)
            **kwargs: Argumentos adicionais para requests

        Returns:
            Resposta JSON ou None em caso de erro
        """
        try:
            if method.upper() == "GET":
                response = self.session.get(url=url, headers={"Content-type": "application/json"}, timeout=10, **kwargs)
            elif method.upper() == "PUT":
                response = self.session.put(url=url, headers={"Content_type": "application/json"}, json=payload, timeout=10, **kwargs)
            else:
                raise ValueError(f"Método {method} não suportado.")
            
            response.raise_for_status()
            if response.status_code == 204: # No content
                return {}
            return response.json()
        except requests.exceptions.RequestException:
            self.log.warning("[!] Erro na requisição.", exc_info=True)
            return None
        
    def _process_contract(self, contract:str) -> list[list[Any]]:
        """
        Processa um único contrato e retorna todas as suas negociações.
        
        Args:
            contract: Contrato a ser processado.
        
        Returns:
            Lista de negociações com negociacao já alterada.
        """
        negociacoes_contrato = []

        # Primeira API: coleta de negociações
        url_get = f"{self.settings.get("url_get")}{contract}"

        response_contract = self._make_request(url_get, method="GET")
        if not response_contract or not isinstance(response_contract, list) or len(response_contract) == 0:
            negociacoes_contrato.append([
                contract, "Não localizado negociações.", "Não alterado.", "Não alterado."
            ])
        
        for negociacao in response_contract:
            negociacao["vlNegociacao"] = self.settings.get("valor")
            response_negociacao = self._make_request(url=self.settings.get("url_put"), method="PUT", payload=negociacao)
            if not response_negociacao:
                negociacoes_contrato.append([
                    contract, negociacao.get("cdContratoNegociacao", None), negociacao.get("vlNegociacao", None), "Não alterado."
                ])
            negociacoes_contrato.append([
                contract, negociacao.get("cdContratoNegociacao", None), negociacao.get("vlNegociacao", None), "Alterado."
            ])

        return negociacoes_contrato