import yaml
import json
import base64
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
import time
from tqdm import tqdm

from src.logger import Logger


class ColetaContratos:
    def __init__(
        self,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa a classe de coleta de contratos.
        
        Args:
            input_path: Caminho para o arquivo de entrada
            output_path: Caminho para o arquivo de saída
            settings: Configurações da API (client_id, client_secret, url_token)
        """
        # Configurações
        self.settings = settings or {}

        # Logger
        self.log = Logger(name=__name__)._init_logger()

        # Caminhos
        self.input_path = Path(input_path) if input_path else Path(
            self.settings.get("input_path", "entry/coleta_contratos.csv")
        )
        self.output_path = Path(output_path) if output_path else Path(
            self.settings.get("output_path", "output/coleta_contratos.xlsx")
        )

        # Configuração de sessão para requests síncronos
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

        # Cache para tokens e dados
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

        # Resultados
        self.contratos: List[List[Any]] = []

        self._validate_parameters()
        self._ensure_output_directory()

    def _validate_parameters(self) -> None:
        """Valida os parâmetros obrigatórios."""
        if not self.input_path or not self.input_path.exists():
            raise ValueError(f"input_path deve conter um caminho válido: {self.input_path}")

        required_settings = ["client_id", "client_secret", "url_token"]
        for setting in required_settings:
            if not self.settings.get(setting):
                raise ValueError(f"{setting} não definido nas configurações.")

    def _ensure_output_directory(self) -> None:
        """Garante que o diretório de saída existe."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def _get_access_token(self) -> Optional[str]:
        """
        Obtém token de acesso da API com caching.

        Returns:
            Token de acesso ou None em caso de erro
        """
        # Verifica se o token ainda é válido
        if self._access_token and self._token_expiry:
            if datetime.now() < self._token_expiry:
                return self._access_token

        credentials = f"{self.settings['client_id']}:{self.settings['client_secret']}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {"grant_type": "client_credentials"}

        try:
            response = self.session.post(
                url=self.settings["url_token"],
                headers=headers,
                data=data,
                timeout=10
            )
            response.raise_for_status()

            token_data = response.json()
            self._access_token = token_data.get("access_token")

            # Define expiração do token (assume 1 hora se não especificado)
            expires_in = token_data.get("expires_in", 3600)
            self._token_expiry = datetime.now().timestamp() + expires_in

            self.log.info("[+] Token de acesso obtido com sucesso.")
            return self._access_token

        except requests.exceptions.RequestException:
            self.log.error(f"[!!] Erro ao obter token de acesso.", exc_info=True)
            return None

    def _get_headers(self) -> Dict[str, str]:
        """
        Retorna headers para requisições da API.

        Returns:
            Dicionário com headers
        """
        token = self._get_access_token()
        if not token:
            raise ValueError("Não foi possível obter token de acesso")

        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "client_id": self.settings["client_id"]
        }

    def _make_request(self, url: str, method: str = "GET", **kwargs) -> Optional[Dict[str, Any]]:
        """
        Faz uma requisição HTTP com tratamento de erros.

        Args:
            url: URL da requisição
            method: Método HTTP (GET, POST, etc.)
            **kwargs: Argumentos adicionais para requests

        Returns:
            Resposta JSON ou None em caso de erro
        """
        try:
            if method.upper() == "GET":
                response = self.session.get(url, headers=self._get_headers(), timeout=10, **kwargs)
            elif method.upper() == "POST":
                response = self.session.post(url, headers=self._get_headers(), timeout=10, **kwargs)
            else:
                raise ValueError(f"Método {method} não suportado")

            response.raise_for_status()

            if response.status_code == 204:  # No Content
                return {}

            return response.json()

        except requests.exceptions.RequestException:
            self.log.warning(f"[!] Erro na requisição {method} para {url}.")
            return None
        except json.JSONDecodeError:
            self.log.warning(f"[!] Erro ao decodificar JSON da resposta de {url}.")
            return None

    def _process_cnpj(self, cnpj: str) -> List[List[Any]]:
        """
        Processa um único CNPJ e retorna seus contratos.

        Args:
            cnpj: CNPJ a ser processado

        Returns:
            Lista de contratos para o CNPJ
        """
        cnpj_formatado = str(cnpj).zfill(14)
        contratos_cnpj = []

        # Primeira API: Buscar estabelecimento pelo CNPJ
        url_cnpj = (
            "https://api-ext.tribanco.com.br/trp/corpclienteapi/lista-estabelecimento?"
            f"cnpj={cnpj_formatado}&codigoEc=&nomeEstabelecimento=&qtdeLinhas=15&page=1"
        )

        response_cnpj = self._make_request(url_cnpj)
        if not response_cnpj or not isinstance(response_cnpj, list) or len(response_cnpj) == 0:
            contratos_cnpj.append([
                "Não localizado", cnpj_formatado, "Não localizado", "Não localizado",
                "Não localizado", "Não localizado", "Não localizado", "Não localizado",
                "Não localizado", "Não localizado", "Não localizado", "Não localizado"
            ])
            return contratos_cnpj

        # Extrai EC e razão social da primeira resposta
        primeiro_estabelecimento = response_cnpj[0]
        ec = str(primeiro_estabelecimento.get("codigoEc", ""))
        razao_social = primeiro_estabelecimento.get("razaoSocial", "")

        # Segunda API: Buscar contratos pelo EC
        url_ec = (
            f"https://api-ext.tribanco.com.br/trp/corpgestaoequipamento/contratos/"
            f"historico/equipamentos/{ec}?periodoInicial=&periodoFim=&cdStatus=&cdTipoEquipamento="
        )

        response_ec = self._make_request(url_ec)
        if not response_ec:
            contratos_cnpj.append([
                ec, cnpj_formatado, razao_social, "Não localizado", "Não localizado",
                "Não localizado", "Não localizado", "Não localizado", "Não localizado",
                "Não localizado", "Não localizado", "Não localizado"
            ])
            return contratos_cnpj

        # Processa cada contrato encontrado
        for solucao in response_ec:
            contrato = solucao.get('cdContrato', '')
            status = solucao.get('dsStatus', '')
            serial = solucao.get('nrSerie', '')
            patrimonio = solucao.get('dsPatrimonio', '')
            modelo = solucao.get('dsEquipamento', '')
            negociacao = solucao.get('vlNegociacao', '')
            vigencia = solucao.get('vigencia', '')
            inicio = solucao.get('dtInicio', '')
            fim = solucao.get('dtFim', '')

            contratos_cnpj.append([
                ec, cnpj_formatado, razao_social, contrato, status,
                serial, patrimonio, modelo, negociacao, vigencia,
                inicio, fim
            ])

        return contratos_cnpj

    def process_sync(self, max_workers: int = 10, use_polars: bool = True) -> None:
        """
        Processa os dados de forma síncrona com threads.
        
        Args:
            max_workers: Número máximo de threads
            use_polars: Se True, usa Polars para leitura; se False, usa Pandas
        """
        self.log.info(f"[+] Iniciando processamento síncrono com {max_workers} workers.")
        inicio = time.time()

        # Lê o arquivo de entrada
        if use_polars:
            try:
                df = pl.read_csv(self.input_path, separator=";")
                cnpjs = df["CNPJ"].unique().to_list()
                self.log.info(f"[+] Lidos {len(cnpjs)} CNPJs únicos com Polars.")
            except Exception as e:
                self.log.error(f"[!!] Erro ao ler com Polars: {e}, tentando Pandas...")
                df = pd.read_csv(self.input_path, sep=";")
                cnpjs = df["CNPJ"].unique().tolist()
        else:
            df = pd.read_csv(self.input_path, sep=";")
            cnpjs = df["CNPJ"].unique().tolist()

        self.log.info(f"[+] Total de CNPJs únicos para processar: {len(cnpjs)}")

        # Processa com ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(max_workers, len(cnpjs))) as executor:
            # Cria uma lista de tarefas
            futures = [executor.submit(self._process_cnpj, cnpj) for cnpj in cnpjs]

            # Processa resultados conforme são concluídos
            for future in tqdm(futures, total=len(cnpjs), desc="Processando CNPJs"):
                try:
                    contratos_cnpj = future.result(timeout=30)
                    self.contratos.extend(contratos_cnpj)
                except Exception as e:
                    self.log.error(f"Erro ao processar CNPJ: {e}")

        tempo_total = time.time() - inicio
        self.log.info(f"[+] Processamento concluído em {tempo_total:.2f} segundos")
        self.log.info(f"T[+] otal de contratos coletados: {len(self.contratos)}")

    async def _process_cnpj_async(self, session: aiohttp.ClientSession, cnpj: str) -> List[List[Any]]:
        """
        Processa um CNPJ de forma assíncrona.

        Args:
            session: Sessão aiohttp
            cnpj: CNPJ a ser processado

        Returns:
            Lista de contratos para o CNPJ
        """
        # Implementação assíncrona similar à síncrona
        # (seria necessário reimplementar usando aiohttp)
        # Por brevidade, mantemos a versão síncrona aqui
        return self._process_cnpj(cnpj)

    async def process_async(self, batch_size: int = 50) -> None:
        """
        Processa os dados de forma assíncrona.

        Args:
            batch_size: Tamanho do lote para processamento assíncrono
        """
        self.log.info(f"[+] Iniciando processamento assíncrono com batch de {batch_size}")

        # Lê CNPJs (mesma lógica do process_sync)
        try:
            df = pl.read_csv(self.input_path, separator=";")
            cnpjs = df["CNPJ"].unique().to_list()
        except:
            df = pd.read_csv(self.input_path, sep=";")
            cnpjs = df["CNPJ"].unique().tolist()

        # Implementação assíncrona completa requer reescrita das funções de request
        # Para manter a simplicidade, usamos a versão síncrona por enquanto
        self.log.warning("[!] Processamento assíncrono não implementado, usando síncrono")
        self.process_sync()

    def save_results(self, format: str = "excel") -> None:
        """
        Salva os resultados coletados.

        Args:
            format: Formato de saída (excel, csv, parquet)
        """
        if not self.contratos:
            self.log.warning("[!] Nenhum contrato para salvar")
            return

        colunas = [
            'EC', 'CNPJ', 'CLIENTE', 'CONTRATO', 'STATUS', 'SERIAL',
            'PATRIMONIO', 'MODELO', 'NEGOCIACAO', 'VIGENCIA', 'INICIO', 'FIM'
        ]

        # Cria DataFrame
        if len(self.contratos[0]) == len(colunas):
            df_resultado = pl.DataFrame(self.contratos, schema=colunas)
        else:
            # Se o número de colunas não bater, cria sem schema
            df_resultado = pl.DataFrame(self.contratos)

        # Salva no formato especificado
        if format.lower() == "excel":
            df_resultado.write_excel(self.output_path)
            self.log.info(f"[+] Resultados salvos em Excel: {self.output_path}")
        elif format.lower() == "csv":
            csv_path = self.output_path.with_suffix(".csv")
            df_resultado.write_csv(csv_path)
            self.log.info(f"[+] Resultados salvos em CSV: {csv_path}")
        elif format.lower() == "parquet":
            parquet_path = self.output_path.with_suffix(".parquet")
            df_resultado.write_parquet(parquet_path)
            self.log.info(f"[+] Resultados salvos em Parquet: {parquet_path}")
        else:
            raise ValueError(f"Formato não suportado: {format}")

    def run(self, method: str = "sync", **kwargs) -> None:
        """
        Executa o fluxo completo de coleta.

        Args:
            method: Método de processamento (sync, async)
            **kwargs: Argumentos adicionais para o método de processamento
        """
        self.log.info("[+] Iniciando coleta de contratos")

        try:
            if method.lower() == "async":
                asyncio.run(self.process_async(**kwargs))
            else:
                self.process_sync(**kwargs)

            self.save_results()
            self.log.info("[+] Coleta concluída com sucesso")

        except Exception as e:
            self.log.error(f"[!!] Erro durante a coleta: {e}", exc_info=True)
            raise