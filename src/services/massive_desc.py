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


class MassiveDesc:
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
            self.settings.get("paths").get("input_path", "entry/massive_desc.csv")
        )
        self.output_path = Path(output_path) if output_path else Path(
            self.settings.get("paths").get("output_path", "output/massive_desc.xlsx")
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
        
    def _process_contract(self, contract:str) -> List[List[Any]]:
        """
        Processa um único contrato e sobe sua solicitação de cancelamento.

        Args:
            contract: Contrato a ser descredenciado.
        
        Results:
            Contrato com seu respectivo status.
        """
        # Variáveis importantes
        payload = {
            "usuarioSolicitacao": 'API',
            "idCanal": "API",
            "dsSistema": "API",
            "observacao": "OS DE AJUSTE",
            "churnInvoluntario": False
        }
        # Primeiro request: Solicitação de cancelamento.
        response = self._make_request(url=f"{self.settings.get("requests_url").get("url_desc")}{contract}", method="PUT", payload=payload)
        if not response or len(response) <= 0:
            return [contract, "Erro ao solicitar cancelamento."]
        else:
            response [contract, "Solicitado cancelamento."]
    
    def process_sync(self, max_workers:int=10, use_polars:bool=True) -> None:
        """
        Processa os dados de forma síncrona com threads

        Args:
            max_workers: Número máximo de threads
            user_polars: Check para o uso da lib Polars no lugar da lib Pandas
        """
        self.log.info(f"[+] Iniciando processamento síncrono com {max_workers} threads.")
        inicio = time.time()

        if use_polars:
            try:
                df = pl.read_csv(self.input_path, separator=";")
                contratos = df["CONTRATOS"].to_list()
            except Exception as e:
                self.log.warning(f"[!] Erro ao ler arquivo com poalrs: {e}. Tentando com pandas...")
                df = pd.read_csv(self.input_path, sep=";")
                contratos = df["CONTRATOS"].unique().tolist()
        else:
            df = pd.read_csv(self.input_path, sep=";")
            contratos = df["CONTRATOS"].unique().tolist()

        self.log.info(f"[+] Lidos {len(contratos)} contratos únicos.")

        # Processo com ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(max_workers, len(contratos))) as executor:
            futures = [executor.submit(self._process_contract, contrato) for contrato in contratos]

            for future in tqdm(futures, total=len(contratos), desc="Solicitando cancelamento..."):
                try:
                    result = future.result(timeout=30)
                    self.contratos.extend(result)
                except Exception:
                    self.log.error("[!!] Erro ao processar contrato", exc_info=True)

        tempo_total = time.time() - inicio
        self.log.info(f"[+] Processamento concluído em {tempo_total:.2f} segundos.")

    def save_result(self, format:str="excel") -> None:
        """
        Salva os resultados coletados.
        
        Args:
            format: Formato de saída
        """

        if not self.contratos:
            self.log.warning("[!] Nenhum dado para salvar.")
            return
        
        colunas = [
            "CONTRATO", "CD_NEGOCIACAO", "VL_NEGOCIACAO", "STATUS_ALTERACAO"
        ]

        if len(self.contratos[0]) == len(colunas):
            df_result = pl.DataFrame(self.contratos, schema=colunas)
        else:
            df_result = pl.DataFrame(self.contratos)

        match format.lower():
            case "excel":
                df_result.write_excel(self.output_path)
                self.log.info(f"[+] Resultados salvos em Excel: {self.output_path}")
            case "csv":
                csv_path = self.output_path.with_suffix(".csv")
                df_result.write_csv(csv_path)
                self.log.info(f"[+] Resultados salvos em CSV: {csv_path}")
            case "parquet":
                parquet_path = self.output_path.with_suffix(".parquet")
                df_result.write_parquet(parquet_path)
                self.log.info(f"[+] Resultados salvos em Parquet: {parquet_path}")
            case _:
                self.log.error("[!!] Tipo de arquivo não informado ou não mapeado.", exc_info=True)
                return

    def run(self, method:str="sync", **kwargs) -> None:
        """
        Executa o fluxo completo de (re)precificação
        
        Args:
            method: Método de processamento (sync, async)
            **kawrgs: Argumentos adicionais para o método de processamento
        """
        self.log.info("[+] Iniciando alteração de valores negociados.")

        try:
            match method.lower():
                case "async":
                    asyncio.run(self.process_async(**kwargs))
                case "sync":
                    self.process_sync(**kwargs)
                case _:
                    self.log.error("[!!] Método não informado.")
                    return

            self.save_result()
            self.log.info("[+] Alteração concluída com sucesso.")
        except:
            self.log.error(f"[!!] Erro durante a alteração.", exc_info=True)
            raise