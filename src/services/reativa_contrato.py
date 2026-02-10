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


class ReativaContrato:
    def __init__(
        self,
        input_path:Optional[str]=None,
        output_path:Optional[str]=None,
        settings:Optional[Dict[str, Any]]=None
    ):
        """
        Inicializa a classe de reativação de contratos

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
            self.settings.get("input_path", "entry/reativa_contrato.csv")
        )
        self.output_path = Path(output_path) if output_path else Path(
            self.settings.get("output_path", "output/reativa_contrato.xlsx")
        )

        # Configuração de sessão para requests síncronos
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

        required_settings = ["url_get_contrato", "url_get_equip",
                             "url_put_contrato", "url_put_equp"]
        # for setting in required_settings:
        #     if not self.settings.get(setting):
        #         raise ValueError(f"{setting} não definido nas configurações.")

    def _ensure_output_directory(self) -> None:
        """Garante que o caminho de saída exsita"""
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

    def _process_contract(self, row:tuple) -> list[list[Any]]:
        """
        Processa um único contrato e retorna seu status já reativado.

        Args:
            row: Tupla contendo informações de EC, contrato e serial.
        
        Returns:
            Lista com status dos contratos alterados.
        """
        # Primeiro par de requests: coleta de contrato e equipamentos
        response_get_contrato = self._make_request(url=f"{self.settings.get("url_get_contrato")}{str(getattr(row, "EC"))}", method="GET")
        response_get_equip = self._make_request(url=f"{self.settings.get("url_get_equip")}{str(getattr(row, "CONTRATO"))}", method="GET")

        if not response_get_contrato or not response_get_equip:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    str(getattr(row, "SERIAL")),
                    "n", "---", "---"]

        check_found_contrato = False
        for contrato in response_get_contrato:
            if str(getattr(row, "CONTRATO")) == str(contrato.get("cdContrato", "")):
                new_contrato = {
                    "cdContrato": int(getattr(row, "CONTRATO")),
                    "cdCliente": int(getattr(row, "EC")),
                    "dtSolicitacao": contrato.get("dtSolicitacao", ""),
                    "dtInicio": contrato.get("dtInicio", ""),
                    "dsSistemaSolicitacao": contrato.get("dsSistemaSolicitacao", ""),
                    "dsUsuarioSolicitacao": contrato.get("dsUsuarioSolicitacao", ""),
                    "cdStatus": 2,
                    "dsDetalheChamado": contrato.get("dsDetalheChamado") if "dsDetalheChamado" in contrato and contrato["dsDetalheChamado"] else None,
                    "cdTipoEquipamento": contrato.get("cdTipoEquipamento", ""),
                    "cdContratante": contrato.get("cdContratante", ""),
                    "cdSolicitacaoSistemaExterno": contrato.get("cdSolicitacaoSistemaExterno") if "cdSolicitacaoSistemaExterno" in contrato and contrato["cdSolicitacaoSistemaExterno"] else None
                }
                check_found_contrato = True

        if not check_found_contrato:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    str(getattr(row, "SERIAL")),
                    "y", "nok", "---"]

        check_found_equip: False
        for solucao in response_get_equip:
            if str(getattr(row, "SERIAL")) == str(solucao.get("nrSerie", "")):
                new_equip = {
                    "cdContratoEquip": solucao.get("cdContratoEquip"),
                    "cdContrato": solucao.get("cdContrato", int(getattr(row, "CONTRATO"))),
                    "cdModelo": solucao.get("cdModelo", ""),
                    "modelo": solucao.get("modelo", ""),
                    "nrSerie": solucao.get("nrSerie", ""),
                    "nrPatrimonio": solucao.get("nrPatrimonio", ""),
                    "dataInicio": solucao.get("dataInicio", ""),
                    "ativo": True,
                }
                check_found_equip = True

        if not check_found_equip:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    str(getattr(row, "SERIAL")),
                    "y", "---", "nok"]

        # Segundo par de requests: atualização de contrato e equipamentos
        response_put_contrato = self._make_request(url=self.settings.get("url_put_contrato"), method="PUT", payload=new_contrato)
        if not response_put_contrato:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    str(getattr(row, "SERIAL")),
                    "y", "nok", "nok"]
        
        response_put_equip = self._make_request(url=self.settings.get("url_put_equip"), method="PUT", payload=new_equip)
        if not response_put_equip:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    str(getattr(row, "SERIAL" )),
                    "y", "---", "nok"]
        
        return [str(getattr(row, "EC")),
                str(getattr(row, "CONTRATO")),
                str(getattr(row, "SERIAL")),
                "y", "ok", "ok"]
    
    def process_sync(self, max_workers:int=10) -> None:
        """
        Processa os dados de forma síncrona com threads

        Args:
            max_workers: Número máximo de threads
        """
        self.log.info(f"[+] Iniciando processamento síncrono com {max_workers} threads.")
        inicio = time.time()

        df = pd.read_csv(self.input_path, sep=";")
        contratos = df["CONTRATO"].unique().tolist()
        self.log.info(f"[+] Lidos {len(contratos)} contratos únicos.")

        # Processa com ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(max_workers, len(contratos))) as executor:
            futures = [executor.submit(self._process_contract, df.itertuples(index=False))]

            for future in tqdm(futures, total=len(contratos), desc="Reativando contratos"):
                try:
                    result = future.result(timeout=30)
                    self.contratos.extend(result)
                except Exception:
                    self.log.error("[!!] Erro ao reativar contrato.", exc_info=True)

        tempo_total = time.time() - inicio
        self.log.info(f"[+] Reativação finalizada em {tempo_total:.2f} segundos.")
        self.log.info(f"[+] Total de contratos processados: {len(self.contratos)}")

    def _process_contract_async(self, session:aiohttp.ClientSession, row:tuple) -> List[List[Any]]:
        """
        Reativa um contrato de forma assíncrona

        Args:
            session: Sessão aiohttp
            contract: Contrato a ser reativado
        """
        # Implementação assíncrona similar à síncrona
        # (necessário implementar usando aiohttp)
        # Por brevidade, mantemos a versão síncrona aqui
        return self._process_contract(row=row)
    
    async def process_async(self, batch_size:int=50) -> None:
        """
        Processa os dados de forma assíncrona

        Args:
            batch_size: Tamanho do lote de processamento assíncrono
        """
        self.log.info(f"[+] Iniciando processamento assíncrono com batch de {batch_size}")

        try:
            df = pl.read_csv(self.input_path, separator=";")
            contratos = df["CONTRATO"].to_list()
        except:
            df = pd.read_csv(self.input_path, sep=";")
            contrato = df["CONTRATO"].unique().tolist()

        # Implementação assíncrona completa requer reescrita das funções de request
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
            "EC", "CONTRATO", "SERIAL",
            "LOCALIZADO", "STATUS_CONTRATO", "STATUS_EQUIPAMENTO"
        ]
        if len(self.contratos[0])==len(colunas):
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
                self.log.info(f"[+] Resultados salvos em PARQUET: {parquet_path}")
            case _:
                self.log.error("[!!] Tipo de arquivo não informado ou não mapeado.", exc_info=True)
                return

    def run(self, method:str="sync", **kwargs) -> None:
        """
        Executa o fluxo completo de reativação

        Args:
            method: Método de processamento (sync, async)
            **kwargs: Argumentos adicionais para o método de processamento
        """
        self.log.info("[+] Iniciando reativação de contratos.")

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
            self.log.info("[+] Alteração concluído com sucesso.")
        except:
            self.log.error(f"[!!] Erro durante a alteração.", exc_info=True)
            raise