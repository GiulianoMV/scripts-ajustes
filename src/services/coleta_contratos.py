import yaml
from typing import Optional, Dict, Any
from pathlib import Path

from src.logger import Logger


class Coleta_Contrato:
    def __init__(self,
                 input_path:Optional[str]=None,
                 output_path:Optional[str]=None):

        self.entry = input_path if input_path is not None else self.settings.get('input_path', None)

        pass


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