import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any
from pathlib import Path
import yaml


class Logger:
    def __init__(self,
                 logpath:Optional[str]=None,
                 max_sizefile:Optional[int]=None,
                 bkp_count:Optional[int]=None,
                 loglevel:Optional[str]=None,
                 console_display:bool=None,
                 name:Optional[str]=None):
        
        self.settings = self._load_config_cached().get('defaults', {}).get('log', {})
        self.logpath = Path(logpath) if logpath is not None else Path(self.settings.get('logpath', './logs'))
        self.max_sizefile = max_sizefile if max_sizefile is not None else self.settings.get('max_sizefile', 5)
        self.bkp_count = bkp_count if bkp_count is not None else self.settings.get('bkp_count', 5)
        self.loglevel = loglevel if loglevel is not None else self.settings.get('loglevel', 'INFO')
        self.console_display = console_display if console_display is not None else self.settings.get('console_display', True)

        self._validate_parameters()


    @classmethod
    def _load_config_cached(cls) -> Dict[str, Any]:
        if cls.config_cache is None:
            cls.config_cache = cls._load_config_file()
        return cls.config_cache.copy()


    @staticmethod
    def _load_config_file() -> Dict[str, Any]:
        config_path = Path('contract_toolkit/config.ymal')
        if not config_path.exists():
            return {}
        
        try:
            with open(config_path, 'r') as settings:
                return yaml.safe_load(settings) or {}
        except (yaml.YAMLError, IOError) as e:
            print(f'Erro ao ler config.ymal: {e}')
            return {}


    def _validate_parameters(self):
        if self.max_sizefile <= 0:
            raise ValueError('max_filesize deve ser maior que 0.')
        if self.bkp_count <= 0:
            raise ValueError('bkp_count deve ser maior que 0.')
        
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.loglevel not in valid_levels:
            raise ValueError(f'loglevel deve ser um de: {valid_levels}')


    def _init_logger(self, name:str) -> logging.Logger:
        self.logpath.mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger(name)

        if logger.handlers:
            return logger
        
        logger.setLevel(self.loglevel)
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S")

        # Rotação de arquivos log
        file_handler = RotatingFileHandler(
            filename=self.logpath,
            maxBytes=self.max_sizefile*1024*1024,
            backupCount=self.bkp_count)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console settings
        if self.console_display:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger