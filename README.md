# Scripts auxiliares para ajustes contratuais

![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-Em%20Desenvolvimento-yellow.svg)

Compilado de scripts auxiliares usados para ajustes contratuais utilizando endpoints HTTP (GET, PUT e POST) para alteração direto no banco de dados contratual.

---

## Objetivo

Manter público um compilado de scripts auxiliares para ajustes de contratos utilizando requisições HTTP.

---

## Tecnologias

- **Python 3.11+**
- **Polars**
- **APIs**
- **JSON**
- **Requisições HTTP**

---

## Estrutura do Projeto

```bash
ajustes_scripts/
│
├── logs/                           # Pasta com logs de execução (YYYY-mm-dd.txt)
├── entry/                          # Pasta de arquivos de entrada (CSV only)
├── output/                         # Pasta de arquivos de saída (Mixed suffix)
├── src/
│   ├── logger.py                   # Script de logger
│   └── services/
│       ├── coleta_contratos.py     # Serviço de coleta de contratos
│       ├── isenta_contratos.py     # Serviço de reprecificação/isenção de contratos
│       ├── massive_cancel.py       # Serviço de cancelamento massivo de contratos (sem OS de descredenciamento)
│       ├── massive_desc.py         # Serviço de descredenciamento massivo de contratos (com OS de descredenciamento)
│       └── reativa_contrato.py     # Serviço de reativação de contratos
├── cli.py                          # Orquestrador do projeto, arquivo de chamada de serviços
├── config.yaml                     # Arquivo de configurações gerais do projeto
├── README.md
└── requirements.txt                # Dependências
```

---

## Funcionalidades

1. Coleta de contratos

- Extrai todos os contratos presentes nos CNPJs informados no arquivo de entrada.
- Retorna relatório contendo informações cadastrais do cliente (EC, CNPJ, Razão), contratos, serial e patrimônio ativos, status do contrato e informações da negociação vigente.

2. Reprecificação de contratos

- Realiza a alteração de valores de tarifas de contratos de forma síncrona usando multiplas threads.

3. Cancelamento massivo (Com e sem OS)

- O serviço possui 2 módulos, sendo ele o cancelamento e o descredenciamento.
- O cancelamento massivo visa cancelar o contrato de forma forçada, sem a necessidade de OS. Altera o status do contrato para **CANCELADO** e fecha todos os protocolos/OS, bem como finaliza todas as negociações e altera o status do equipamento ativo para cancelado.
- O descredenciamento massivo via solicitar o cancelamento dos contratos/equipamentos. Altera o status dos contratos para **CANCELAMENTO SOLICITADO**, deixando o sistema seguir o fluxo normal de cancelamento via protocolo/OS.

4. Reativação de contrato

- Realiza a alteração de status do contrato para **INSTALADO**, independente do status original. Também realiza a ativação do último equipamento instalado no contrato, porém mantém a negociação finalizada para não gerar cobranças indevidas no caso de uma reativação indevida.

---

## Como Executar

1. Clonar o repositório

```bash
git clone https://github.com/GiulianoMV/scripts-ajustes
cd scripts-ajustes
```

2. Criar e ativar ambiente virtual

```bash
python -m venv .venv
source .venv/bin/activate       # Linux/Mac
.venv/Scripts/activate          # Windows
```

3. Instalar dependências

```bash
pip install -r requirements.txt
```

4. Configurar variáveis

- Criar um config.yaml com base no exemplo dado em "example-config.yaml"
- Configure a pasta de entrada, saída, tal como endpoints, credenciais e outros parâmetros necessários

5. Executar o projeto

```bash
python cli.py --service
```

Troque "service" pelo nome do serviço desejado. Importante que o arquivo de entrada já esteja na pasta indicada.

---

## Notas Importantes

> **Nenhuma** credencial ou endpoint deve ser commitado no repositório.
> Todas as configurações devem ser definidas no arquivo config.yaml

---

## Contribuição

Passos padrões:

1. Faça um fork
2. Crie uma branch: git checkout -b feature/nova-feature
3. Commit: git commit -m "Descrição breve da mudança"
4. Envie: git push origin feature/nova-feature
5. Abra um Pull Request

---

## Licença

Distribuído sob licença MIT
Consulte LICENSE para mais detalhes.

---

## Contato

Giuliano Vieira<br>
E-mail: gmossv603@gmail.com<br>
LinkedIn: https://www.linkedin.com/in/giuliano-vieira1/