# 🏢 WS Data Pipeline — Airflow 3 + dbt + Cosmos

Pipeline de dados construído com **Apache Airflow 3.2.0**, **dbt-core 1.7.9** e **Astronomer Cosmos**, seguindo a arquitetura **Medallion** (Bronze → Silver → Gold) para consolidar dados de empresas de 3 fontes distintas: **Google**, **iFood** e **Receita Federal do Brasil (RFB)**.

---

## 📋 Visão Geral do Projeto

| Componente | Tecnologia | Versão |
|---|---|---|
| Orquestrador | Apache Airflow | 3.2.0 |
| Transformação | dbt-core | 1.7.9 |
| Adaptador | dbt-postgres | 1.7.9 |
| Integração Airflow ↔ dbt | Astronomer Cosmos | última |
| Pacote dbt | dbt_utils | 1.1.1 |
| Banco de dados | PostgreSQL | 13 |
| Executor | LocalExecutor | — |
| Infraestrutura | Docker Compose | — |

### Números do Projeto

| Métrica | Quantidade |
|---|---|
| DAGs no Airflow | 3 |
| Modelos dbt (`.sql`) | 12 |
| Fontes de dados (sources) | 3 |
| Camadas do Medallion | 3 (Bronze, Silver, Gold) |
| Tabelas Gold de saída | 5 |

### Arquitetura das DAGs

O projeto utiliza uma **DAG orquestradora (master)** que coordena a execução das outras duas DAGs em sequência:

```
master_ws_data_orchestrator (agendada figurativamente às 01:00 diariamente)
│
├── 1. trigger_ingestion_raw_data
│       └── ingestion_raw_data  →  Lê CSVs e carrega na camada Raw (Python + Pandas)
│
└── 2. trigger_dbt_transformation
        └── dbt_ws_data_pipeline  →  Executa dbt Bronze → Silver → Gold (Cosmos DbtTaskGroup)
```

- **`master_ws_data_orchestrator`** — DAG master que usa `TriggerDagRunOperator` com `wait_for_completion=True` para garantir a ordem de execução.
- **`ingestion_raw_data`** — DAG Python com `@task` do Airflow SDK que lê 3 arquivos CSV e carrega no schema `raw` do PostgreSQL via `pandas.to_sql()`.
- **`dbt_ws_data_pipeline`** — DAG dbt via Cosmos com 3 `DbtTaskGroup` separados por camada ('bronze', 'silver' e 'gold'), executados sequencialmente.

---

## 📊 Resultados — Tabela de Cobertura

A tabela `gold.cobertura` contabiliza a presença de cada empresa nas 3 fontes de dados, medindo a qualidade da consolidação:

### Volumes por Fonte

| Métrica | Valor |
|---|---|
| **Total geral de empresas** | **142** |
| Total com dados do iFood | 93 |
| Total com dados da RFB | 106 |
| Total com dados do Google | 96 |

### Interseções entre Fontes

| Cruzamento | Empresas |
|---|---|
| Google × iFood | 64 |
| Google × RFB | 78 |
| iFood × RFB | 65 |
| **Presente nas 3 fontes** | **54** |

### Exclusivos por Fonte

| Fonte | Apenas nesta fonte |
|---|---|
| Apenas Google | 8 |
| Apenas iFood | 18 |
| Apenas RFB | 17 |

> 💡 **Interpretação:** Das 142 empresas únicas identificadas, **54 (38%)** foram encontradas nas 3 fontes simultaneamente.

---

##  Arquitetura Medallion — Detalhamento por Camada

### 📁 Raw — Camada de Ingestão

**Schema:** `raw` | **Materialização:** tabela física (via Pandas) | **Tabelas:** 3

A camada Raw é a entrada bruta dos dados no pipeline. Os CSVs são lidos pela DAG `ingestion_raw_data` usando `pandas.read_csv()` e carregados no PostgreSQL com `to_sql(if_exists='replace')`.

| Tabela | Fonte | Descrição |
|---|---|---|
| `raw.raw_google` | `google.csv` | Dados do Google (estabelecimentos, ratings, coordenadas) |
| `raw.raw_ifood` | `ifood.csv` | Dados do iFood (restaurantes, CNPJs, endereços, categorias) |
| `raw.raw_rfb` | `rfb.csv` | Dados da Receita Federal (razão social, CNAE, situação cadastral) |

**Decisões técnicas:**
- Dados na camada raw sempre sobreescrevendo
- Atualização das bases via airflow (simulando dados vindo de API ou outras fontes).
- Airflow é uma ferramenta que além de orquestrar pode ser utilizada como motor para algumas transformações e por isso foi escolhida para este projeto. 

---

### 🥉 Bronze — Camada de Tipagem

**Schema:** `bronze` | **Materialização:** `view` | **Modelos:** 3

A camada Bronze faz a leitura das tabelas raw via `{{ source() }}` e aplica **tipagem explícita** (`CAST`) em todas as colunas. Não há transformação de negócio — apenas garantir que os tipos de dados estejam corretos.

| Modelo | Source | O que faz |
|---|---|---|
| `stg_google.sql` | `raw.raw_google` | Cast de todas as colunas + gera `lat_lon` (concat latitude+longitude) + adiciona `data_carga_bronze` e `sistema_origem` |
| `stg_ifood.sql` | `raw.raw_ifood` | Cast de todas as colunas + gera `lat_lon` + adiciona metadados de carga |
| `stg_rfb.sql` | `raw.raw_rfb` | Cast de todas as colunas + adiciona metadados de carga |

**Decisões técnicas:**
- **Materialização como `view`** — Economiza espaço, já que são apenas projeções tipadas da raw.
- **`lat_lon`** — Coluna calculada com `CONCAT(latitude, longitude)` para servir como chave de join entre Google e iFood na Silver.
- **`sistema_origem`** — String literal identificando a fonte (ex: `'google'`, `'ifood'`, `'rfb'`).
- Cada source é declarado em seu próprio arquivo `.yml` (`src_google.yml`, `src_ifood.yml`, `src_rfb.yml`).

---

### 🥈 Silver — Camada de Limpeza e Consolidação

**Schema:** `silver` | **Materialização:** `table` | **Modelos:** 4

A camada Silver é onde acontece a **higienização**, **padronização** e **consolidação** dos dados. Cada fonte passa por tratamento individual e depois as 3 são unificadas em uma única tabela mestre.

#### Modelos Individuais (Higienização)

| Modelo | O que faz |
|---|---|
| `int_google.sql` | `UPPER(TRIM())` em textos, normaliza `R.` → `RUA` no street_name, extrai `street_number` e `complement` via regex, formata CEP com máscara `XXXXX-XXX`, gera surrogate key `sk_google` |
| `int_ifood.sql` | `UPPER(TRIM())`, normaliza `R.` → `RUA`, formata CNPJ com máscara `XX.XXX.XXX/XXXX-XX` (LPAD + SUBSTRING), formata CEP, gera `sk_ifood` |
| `int_rfb.sql` | `UPPER(TRIM())`, formata CNPJ e CEP com máscaras, renomeia colunas pt-BR para en-US (`razao_social` → `real_name`, `logradouro` → `street_name`), gera `sk_rfb` |

**Decisões técnicas:**
- **Surrogate Keys** via `{{ dbt_utils.generate_surrogate_key() }}` — Cada tabela gera uma SK baseada no identificador natural da fonte (`cid` para Google, `slug` para iFood, `cnpj` para RFB).
- **Normalização de endereço** — Abreviações como `R.` são expandidas para `RUA` para permitir JOINs por endereço.
- **LPAD no CNPJ** — Garante 14 dígitos antes de aplicar a máscara, tratando CNPJs que perderam zeros à esquerda.

#### Modelo de Consolidação

| Modelo | O que faz |
|---|---|
| `int_companies.sql` | Unifica as 3 fontes em uma única tabela mestre com 303 linhas de SQL |

**Estratégia de JOINs (do mais forte ao mais fraco):**

```
PASSO 1: iFood LEFT JOIN RFB → por CNPJ (identificador único de empresa)
PASSO 2: Resultado LEFT JOIN Google → por lat_lon (localização exata)
         + Google LEFT JOIN por street_name + street_number (fallback endereço com RFB)
PASSO 3: Google órfãos LEFT JOIN RFB → por street_name + street_number + district
PASSO 4: RFB órfãos (registros que não casaram com nada)
```

- **COALESCE** prioriza dados: `iFood > RFB > Google` para campos de endereço, maximizando o preenchimento.
- **ROW_NUMBER() OVER (PARTITION BY cid)** na parte de Google órfãos para evitar duplicatas no match por endereço.
- **3 partes unidas por UNION ALL** — nenhum registro se perde.
- **Coluna `sistema_origem`** indica de qual passo/fonte o registro veio.

---

### 🥇 Gold — Camada de Consumo

**Schema:** `gold` | **Materialização:** `table` | **Modelos:** 5

A camada Gold contém tabelas prontas para consumo analítico, cada uma com um propósito específico:

| Modelo | Propósito |
|---|---|
| `companies.sql` | Tabela master de empresas com ID surrogado (`generate_surrogate_key`), todas as 22 colunas consolidadas + `source` + SKs das 3 fontes |
| `cobertura.sql` | Tabela de métricas: volumes totais, interseções 2 a 2, exclusivos por fonte e presença nas 3 fontes |
| `google_matches.sql` | Relação `company_id ↔ google_cid` (apenas empresas com match no Google) |
| `ifood_matches.sql` | Relação `company_id ↔ ifood_slug` (apenas empresas com match no iFood) |
| `rfb_matches.sql` | Relação `company_id ↔ rfb_document_number` (apenas empresas com match na RFB) |

**Decisões técnicas:**
- **`companies.sql`** gera um `id` único com `generate_surrogate_key(['ifood_slug', 'google_cid', 'rfb_document_number'])`, garantindo idempotência.
- **Tabelas de matches** servem como tabelas-ponte para análises por fonte sem duplicar dados da companies.
- **`cobertura.sql`** usa `COUNT(CASE WHEN ... THEN 1 END)` sobre as surrogate keys para calcular interseções.

---

## 🚀 Como Clonar e Executar o Projeto

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/install/) instalados
- Git
- Mínimo de 4GB de RAM disponível para os containers
- Processador i5

### Passo 1 — Clonar o repositório

```bash
git clone <URL_DO_REPOSITORIO>
cd meu_ambiente_local
```

### Passo 2 — Criar o arquivo `.env`

Crie um arquivo `.env` na raiz do projeto com o seguinte conteúdo:

```env
# ══════════════════════════════════════════════════════
# .env.example — Copie este conteúdo para um arquivo .env
# ══════════════════════════════════════════════════════

# Configurações básicas
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE_DEFAULT_TIMEZONE=America/Sao_Paulo
AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=America/Sao_Paulo

# Motor de execução leve (sem depender do Celery/Redis)
AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Gerenciador de Autenticação obrigatória no Airflow 3
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

# Conexão com o Banco de Dados (postgres local)
# O nome do host "postgres" reflete o nome do serviço no docker-compose
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow

# Permissões locais do WSL (evita problemas de arquivos travados no root)
AIRFLOW_UID=50000

# Usuario e senha padrao para acessar o site do Airflow
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

AIRFLOW__API__BASE_URL=http://localhost:8080

# Chave JWT compartilhada entre todos os serviços do Airflow 3
# (necessária para autenticação interna entre scheduler, api-server, etc.)
# Gere uma nova com: openssl rand -base64 32
AIRFLOW__API_AUTH__JWT_SECRET=<GERAR_COM_openssl_rand_-base64_32>
```

> ⚠️ **Importante:** Gere uma chave JWT única para o seu ambiente:
> ```bash
> openssl rand -base64 32
> ```
> Copie o resultado e substitua `<GERAR_COM_openssl_rand_-base64_32>` no `.env`.

### Passo 3 — Subir os containers

```bash
docker compose build
docker compose up -d
```

Aguarde todos os containers estarem `running`:


### Passo 4 — Acessar a interface do Airflow

Abra o navegador em **http://localhost:8080**

- **Usuário:** `admin`
- **Senha:** `admin`

### Passo 5 — Criar as conexões no Airflow

Na interface do Airflow, vá em **Admin → Connections** e crie a seguinte conexão:

| Campo | Valor |
|---|---|
| Connection Id | `postgres_ws_data` |
| Connection Type | `Postgres` |
| Host | `postgres` |
| Schema | `ws_data` |
| Login | `airflow` |
| Password | `airflow` |
| Port | `5432` |

### Passo 6 — Criar o banco `ws_data` no PostgreSQL

```bash
docker exec -i meu_ambiente_local-postgres-1 psql -U airflow -c "CREATE DATABASE ws_data;"
```

Crie o schema `raw` dentro do banco:

```bash
docker exec -i meu_ambiente_local-postgres-1 psql -U airflow -d ws_data -c "CREATE SCHEMA IF NOT EXISTS raw;"
```

### Passo 7 — Instalar dependências do dbt (dbt_utils)

```bash
docker exec -i meu_ambiente_local-airflow-scheduler-1 bash -c "cd /opt/airflow/dbt/ws_data && /home/airflow/.local/bin/dbt deps"
```

### Passo 8 — Colocar os arquivos de dados

Coloque os 3 arquivos CSV na pasta `ws_data_dados/data/`:

```
ws_data_dados/
└── data/
    ├── google.csv
    ├── ifood.csv
    └── rfb.csv
```

### Passo 9 — Executar o pipeline

Na interface do Airflow (http://localhost:8080):

1. Ative as 3 DAGs: `ingestion_raw_data`, `dbt_ws_data_pipeline` e `master_ws_data_orchestrator`
2. Dispare manualmente a DAG **`master_ws_data_orchestrator`** clicando no botão ▶ (trigger)
3. Aguarde a execução completa (a master dispara as outras 2 em sequência)

### Passo 10 — Verificar os resultados

```bash
# Ver a tabela de cobertura
docker exec -i meu_ambiente_local-postgres-1 psql -U airflow -d ws_data -c "SELECT * FROM gold.cobertura;"

# Ver as empresas consolidadas
docker exec -i meu_ambiente_local-postgres-1 psql -U airflow -d ws_data -c "SELECT count(*) FROM gold.companies;"
```

---

## 📂 Estrutura do Projeto

```
meu_ambiente_local/
├── .env                          # Variáveis de ambiente (NÃO committar)
├── Dockerfile                    # Imagem base: apache/airflow:3.2.0
├── docker-compose.yml            # 6 serviços: postgres, init, apiserver, scheduler, dag-processor, triggerer
├── requirements.txt              # astronomer-cosmos, apache-airflow-providers-postgres, pandas
├── requirements-dbt.txt          # dbt-core==1.7.9, dbt-postgres==1.7.9, protobuf<5.0.0
│
├── airflow/
│   ├── dags/
│   │   ├── ws_data_orquestrador.py     # DAG Master (TriggerDagRunOperator)
│   │   ├── ingestion_raw_data.py       # DAG de Ingestão (CSV → Raw)
│   │   └── dbt_ws_data.py              # DAG dbt (Cosmos DbtTaskGroup)
│   │
│   ├── dbt/ws_data/
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml                # Targets: dev (localhost) e prd (container)
│   │   ├── packages.yml                # dbt_utils 1.1.1
│   │   └── models/
│   │       ├── bronze/                 # 3 views de tipagem (stg_*.sql + src_*.yml)
│   │       ├── silver/                 # 4 tabelas de limpeza e consolidação (int_*.sql)
│   │       └── gold/                   # 5 tabelas de consumo (companies, cobertura, *_matches)
│   │
│   ├── logs/
│   └── plugins/
│
└── ws_data_dados/                # Volume com os CSVs de entrada
    └── /data/
        ├── google.csv
        ├── ifood.csv
        └── rfb.csv
```

---

## 🛠️ Tecnologias e Dependências

| Arquivo | Conteúdo |
|---|---|
| `requirements.txt` | `astronomer-cosmos`, `apache-airflow-providers-postgres`, `pandas` |
| `requirements-dbt.txt` | `dbt-core==1.7.9`, `dbt-postgres==1.7.9`, `protobuf<5.0.0` |
| `packages.yml` | `dbt-labs/dbt_utils` versão `1.1.1` |

O **dbt_utils** é utilizado para a macro `generate_surrogate_key()` que cria chaves surrogadas (hash MD5) nas camadas Silver e Gold, permitindo rastrear a origem de cada registro nas tabelas consolidadas.

---

## 📝 Licença

Apache Airflow 3 + dbt + Cosmos.
