import sys
import yaml
import importlib
from src.logger import Logger


with open('config.yaml', 'r') as f:
    settings = yaml.safe_load(f)
    print('INFO - [+] Configurações yaml carregadas.')

log = Logger(settings=settings, name=__name__)._init_logger()

def main():
    if len(sys.argv) < 2:
        log.warning("[!] Uso: contract-toolkit <serviço> [args]")
        sys.exit(1)

    service = sys.argv[1]
    args = sys.argv[2:]

    module_path = f"contract_toolkit.src.services.{service}"

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        log.error(f"[!!] Serviço '{service}' não existe")
        sys.exit(2)

    module.run(args)

if __name__ == "__main__":
    main()