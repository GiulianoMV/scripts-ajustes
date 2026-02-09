import sys
import yaml
import importlib
from src.logger import Logger


with open('config.yaml', 'r') as f:
    settings = yaml.safe_load(f)
    print('INFO - [+] Configurações yaml carregadas.')

log = Logger(settings=settings.get('defaults', {}).get('log', {}), name=__name__)._init_logger()

def main():
    if len(sys.argv) < 1:
        log.warning("[!] Uso: contract-toolkit --service [args]")
        log.warning("[!] Comandos: contract-toolkit --commands")
        sys.exit(1)
    elif sys.argv[1]=="--commands":
        log.info("[-] Lista de comandos disponíveis:")
        log.info("[-] contract-toolkit --coleta_contratos")
        log.info("[-] contract-toolkit --isenta_contratos")
        log.info("[-] contract-toolkit --reativa_contratos")
        sys.exit(0)

    service = str(sys.argv[1]).replace('--','')

    module_path = f"src.services.{service}"

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        log.error(f"[!!] Serviço '{service}' não existe")
        sys.exit(2)

    if len(sys.argv) > 2:
        args = sys.argv[2:]
        if hasattr:
            module.run(args)
        elif hasattr(module, 'ColetaContratos'):
            service_class = getattr(module, 'ColetaContratos')
            service_instance = service_class(settings=settings.get('services', {}).get('coleta_contratos', {}))
            service_instance.run()
        elif hasattr(module, "IsentaContratos"):
            service_class = getattr(module, "IsentaContratos")
            service_instance = service_class(settings=settings.get("services", {}).get("isenta_contratos", {}))
            service_instance.run()
        elif hasattr(module, "ReativaContrato"):
            service_class = getattr(module, "ReativaContrato")
            service_instance = service_class(settings=settings.get("service", {}).get("reativa_contratos", {}))
            service_instance.run()
    else:
        if hasattr(module, 'run'):
            module.run()
        else:
            match service:
                case "coleta_contrato":
                    service_class = getattr(module, 'ColetaContratos')
                    service_instance = service_class(settings=settings.get('services', {}).get('coleta_contratos', {}))
                    service_instance.run()
                case "isenta_contrato":
                    service_class = getattr(module, "IsentaContratos")
                    service_instance = service_class(settings=settings.get("services", {}).get("isenta_contratos", {}))
                    service_instance.run()
                case "reativa_contrato":
                    service_class = getattr(module, "ReativaContrato")
                    service_instance = service_class(settings=settings.get("service", {}).get("reativa_contratos", {}))
                    service_instance.run()

if __name__ == "__main__":
    main()