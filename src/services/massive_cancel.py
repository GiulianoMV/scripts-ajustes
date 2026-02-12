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

        # Variáveis importantes
        self.data = int(time.time()*1000)

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

        # Filtro para coleta do contrato correto + alteração para cancelado
        for contrato in response_get_contrato:
            if str(getattr(row, "CONTRATO")) == str(contrato.get("cdContrato")):
                new_contrato = {
                    "cdContrato": int(getattr(row, "CONTRATO")),
                    "cdCliente": int(getattr(row, "EC")),
                    "dtSolicitacao": contrato.get("dtSolicitacao", ""),
                    "dtInicio": contrato.get("dtInicio", ""),
                    "dtFim": self.data,
                    "dtSolicitacaoCancelamento": self.data,
                    "dsSistemaCancelamento": "Sistema",
                    "dsUsuárioCancelamento": "Sistema",
                    "dsSistemaSolicitacao": contrato.get("dsSistemaSolicitacao", ""),
                    "dsUsuarioSolicitacao": contrato.get("dsUsuarioSolicitacao", ""),
                    "cdStatus": 5,
                    "dsDetalheChamado": contrato.get("dsDetalheChamado") if "dsDetalheChamado" in contrato and contrato["dsDetalheChamado"] else None,
                    "cdTipoEquipamento": contrato.get("cdTipoEquipamento", ""),
                    "cdContratante": contrato.get("cdContratante", ""),
                    "cdSolicitacaoSistemaExterno": contrato.get("cdSolicitacaoSistemaExterno") if "cdSolicitacaoSistemaExterno" in contrato and contrato["cdSolicitacaoSistemaExterno"] else None,
                    "cdExternoSistemaCancelamento": 15
                }
                break

        # Localiza equipamento ativo no contrato e altera para cancelado
        for solucao in response_get_contrato_equip:
            if solucao.get("ativo") and not solucao.get("dataFim"):
                new_equip = {
                    "cdContratoEquip": solucao.get("cdContratoEquip"),
                    "cdContrato": solucao.get("cdContrato", int(getattr(row, "CONTRATO"))),
                    "cdModelo": solucao.get("cdModelo", ""),
                    "modelo": solucao.get("modelo", ""),
                    "nrSerie": solucao.get("nrSerie", ""),
                    "nrPatrimonio": solucao.get("nrPatrimonio", ""),
                    "dataInicio": solucao.get("dataInicio", ""),
                    "dataFim": self.data,
                    "ativo": False,
                }
                break

        # Coleta negociações e altera para negociações finalizadas
        neg_list = []
        for negociation in response_get_neg:
            if (negociation.get("dtFimNegociacao") is None) or (not negociation.get("dtFimNegociacao")) or (negociation.get("dtFimNegociacao")==""):
                negociation["dtFimNegociacao"] = self.data
                neg_list.append(negociation)
        
        # Fecha qualquer OS que ainda esteja com status ativo no contrato
        protocol_list = []
        for protocol in response_get_protocol:
            if protocol.get("cdStatus")==1 or not protocol["cdStatus"]:
                protocol["cdStatus"] = 2
                protocol["dtFechamento"] = self.data
                protocol["dtUltimaExecucao"] = self.data
                protocol_list.append(protocol)

        del response_get_contrato, response_get_contrato_equip, response_get_neg, response_get_protocol

        # Segundo conjunto de requests: Atualização do contrato
        check_update_contract = self._make_request(url=self.settings.get("requests_url").get("url_put_contrato"), method="PUT", payload=new_contrato)
        if not check_update_contract:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    "Erro ao atualizar", "---", "---", "---"]
        check_update_equip = self._make_request(url=self.settings.get("requests_url").get("url_put_contrato_equip"), method="PUT", payload=new_equip)
        if not check_update_equip:
            return [str(getattr(row, "EC")),
                    str(getattr(row, "CONTRATO")),
                    "Atualizado", "Erro ao atualizar", "---", "---"]
        for negociation in neg_list:
            check_update_neg = self._make_request(url=self.settings.get("requests_url").get("url_put_neg"), method="PUT", payload=negociation)
        for protocol in protocol_list:
            check_update_protocol = self._make_request(url=self.settings.get("requests_url").get("url_put_protocol"), method="PUT", payload=protocol)

        return [str(getattr(row, "EC")),
                str(getattr(row, "CONTRATO")),
                "Atualizado", "Atualizado", "Atualizado", "Atualizado"]
    
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

        with ThreadPoolExecutor(max_workers=min(max_workers, len(contratos))) as executor:
            futures = [executor.submit(self._process_contract, df.itertuples(index=False))]

            for future in tqdm(futures, total=len(contratos), desc="Cancelando contratos"):
                try:
                    result = future.result(timeout=30)
                    self.contratos.extend(result)
                except Exception:
                    self.log.error("[!!] Erro ao cancelar contrato.", exc_info=True)
        
        tempo_total = time.time() - inicio
        self.log.info(f"[+] Cancelamentos finalizados em {tempo_total:.2f} segundos.")
        self.log.info(f"[+] Total de contratos processados: {len(self.contratos)}")
    
    def _process_contract_async(self, session:aiohttp.ClientSession, row:tuple) -> List[List[Any]]:
        """
        Cancela um contrato de forma assíncrono

        Args:
            session: Sessão aiohttp
            contract: Contrato a ser cancelado
        """
        # Implementação assíncrona similar à síncrona
        # (necessário implementar usando aiohttp)
        # Por brevidade, mantemos a versão síncrona aqui
        return self._process_contract(row=row)

    async def process_async(self, batch_size:int=50) -> None:
        """
        Processa os dados de forma assíncrona.

        Args:
            batch_size: Tamanho do lote de processamento assíncrono.
        """
        self.log.info(f"[+] Iniciando processamento assíncrono com batch de {batch_size}")

        try:
            df = pl.read_csv(self.input_path, separator=";")
            contratos = df["CONTRATO"].to_list()
        except:
            df = pd.read_csv(self.input_path, sep=";")
            contratos = df["CONTRATO"].unique().tolist()

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
        
        colunas = ["EC", "CONTRATO",
                   "STATUS_CONTRATO", "STATUS_EQUIP",
                   "STATUS_NEGOCIACAO", "STATUS_PROTOCOLO"]
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
                self.log.error("[!!] Tipo de arquivo não informado ou não mapeado.", exc_info= True)
                return

    def run(self, method:str="sync", **kwargs) -> None:
        """
        Executa o fluxo completo de cancelamento.

        Args:
            method: Método de processamento (sync, async)
            **kwargs: Argumentos adicionais para o método de processamento.
        """
        self.log.info("[+] Iniciando cancelamento de contratos.")

        try:
            match match.lower():
                case "async":
                    asyncio.run(self.process_async(**kwargs))
                case "sync":
                    self.process_sync(**kwargs)
                case _:
                    self.log.error("[!!] Método não informado.")
                    return
            self.save_result()
            self.log.info("[+] Cancelamento concluído com sucesso.")
        except:
            self.log.error(f"[!!] Erro durante o cancelamento.", exc_info=True)
            raise