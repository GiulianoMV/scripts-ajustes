import yaml
import aiohttp
import asyncio
import requests
import polars as pl
import pandas as pd
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
            payload: Informações enviadas na requisição (Opcional)
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

        # Primeiro request: coleta de negociações
        url_get = f"{self.settings.get("url_get")}{contract}"

        response_contract = self._make_request(url_get, method="GET")
        if not response_contract or not isinstance(response_contract, list) or len(response_contract) == 0:
            negociacoes_contrato.append([
                contract, "Não localizado negociações.", "Não alterado.", "Não alterado."
            ])

        for negociacao in response_contract:
            # Segundo request: atualização de negociação com novo valor
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

    def process_sync(self, max_workers:int=10, use_polars:bool=True) -> None:
        """
        Processa os dados de forma síncrona com threads
        
        Args:
            max_workers: Número máximo de threads
            use_polars: Check para o uso da lib Polars no lugar da lib Pandas
        """
        self.log.info(f"[+] Iniciando processamento síncrono com {max_workers} threads.")
        inicio = time.time()

        if use_polars:
            try:
                df = pl.read_csv(self.input_path, separator=";")
                contratos = df["CONTRATO"].to_list()
            except Exception as e:
                self.log.warning(f"[!] Erro ao ler arquivo com polars: {e}. Tentando com pandas...")
                df = pd.read_csv(self.input_path, sep=";")
                contratos = df["CONTRATO"].unique().tolist()
        else:
            df = pd.read_csv(self.input_path, sep=";")
            contratos = df["CONTRATO"].unique().tolist()

        self.log.info(f"[+] Lidos {len(contratos)} contratos únicos.")

        # Processa com ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(max_workers, len(contratos))) as executor:
            futures = [executor.submit(self._process_contract, contrato) for contrato in contratos]

            for future in tqdm(futures, total=len(contratos), desc="Processando contratos."):
                try:
                    contratos_negociacao = future.result(timeout=30)
                    self.contratos.extend(contratos_negociacao)
                except Exception:
                    self.log.error("[!!] Erro ao processar contrato.", exc_info=True)

        tempo_total = time.time() - inicio
        self.log.info(f"[+] Processamento concluído em {tempo_total:.2f} segundos.")
        self.log.info(f"[+] Total de negociações alteradas: {len(self.contratos)}")

    async def _process_contract_async(self, session:aiohttp.ClientSession, contract:str) -> List[List[Any]]:
        """
        Processa um contrato de forma assíncrona.
        
        Args:
            session: Sessão aiohttp
            contract: Contrato a ser processado
        
        Results:
            Lista de negociações com negociacao já alterada.
        """
        # Implementação assíncrona similar à sincrona
        # (necessário implementar usando aiohttp)
        # Por brevidade, mantemos a versão síncrona aqui
        return self._process_contract(contract)
    
    async def process_async(self, batch_size:int=50) -> None:
        """
        Processa os dados de forma assíncrona.
        
        Args:
            batch_size: Tamanho do lote de processamento assíncrono
        """
        self.log.info(f"[+] Iniciando processamento assíncrono com batch de {batch_size}.")

        try:
            df = pl.read_csv(self.input_path, separator=";")
            contratos = df["CONTRATO"].to_list()
        except:
            df = pd.read_csv(self.input_path, sep=";")
            contratos = df["CONTRATO"].unique().tolist()
        
        # Implemenação assíncrona completa requer reescrita das funções de request
        # Para manter a simplicidade, usamos a versão síncrona por enquanto
        self.log.warning(f"[!] Processamento assíncrono não implementado, usando síncrono.")
        self.process_sync()

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