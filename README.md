# 🚍 Urban Mobility Data Pipeline — Belo Horizonte

## 📌 Visão Geral

Este projeto implementa um pipeline de dados ponta a ponta utilizando arquitetura Medallion (Bronze, Silver e Gold) para ingestão, processamento e análise de dados públicos de mobilidade urbana da cidade de Belo Horizonte.

O objetivo é coletar dados de posicionamento em tempo real dos ônibus e dados históricos do Mapa de Controle Operacional (MCO), armazená-los em um data lake, realizar transformações e disponibilizar tabelas analíticas prontas para consumo por ferramentas de BI ou modelos de Machine Learning.

O pipeline foi desenvolvido utilizando Python e PySpark em ambiente local, seguindo boas práticas de DataOps, com foco em qualidade, organização e rastreabilidade dos dados.

---

## 📊 Fontes de Dados

### 🚍 Ônibus — Tempo Real

- Origem: Portal de Dados Abertos de Belo Horizonte (CKAN)
- Tipo de acesso: API (`datastore_search`)
- Formato recebido: JSON
- Característica: dados transacionais em tempo real

### 🗺️ Mapa de Controle Operacional (MCO)

- Origem: Portal de Dados Abertos de Belo Horizonte (CKAN)
- Tipo de acesso: arquivos CSV mensais
- Período utilizado: Janeiro a Dezembro de 2024
- Característica: dados históricos

---

## 🏗️ Arquitetura Medallion

O pipeline é estruturado em três camadas:

### 🥉 Bronze

- Armazena dados crus (raw)
- Sem transformações de negócio
- Formato: Parquet
- Organização por fonte e período

### 🥈 Silver

- Limpeza e padronização de colunas
- Tipagem de dados
- União dos arquivos mensais do MCO
- Remoção de duplicados
- Validação de esquema
- Formato: Delta Lake

### 🥇 Gold

- Tabelas agregadas
- Métricas analíticas
- Dados prontos para BI e Machine Learning
- Formato: Delta Lake

---

## 📁 Estrutura do Projeto

```
urban-mobility-pipeline/
│
├── src/
│   ├── bronze/
│   ├── gold/
│   ├── orchestration/
│   ├── silver/
│   └── utils/
│
├── enviroment.yml
├── README.md
└── requirements.txt
```

---

## ⚙️ Estratégia de Ingestão

### Ônibus (Tempo Real)

Fluxo:

```
API CKAN → JSON → Spark DataFrame → Parquet (Bronze)
```

### MCO

Fluxo:

```
Download CSV → Spark DataFrame → Parquet (Bronze)
```

Cada mês do MCO é armazenado separadamente na camada Bronze para preservar granularidade e rastreabilidade.

Nenhuma agregação é realizada nesta etapa.

---

## 🚧 Desafios Encontrados e Soluções

### 403 Forbidden ao acessar CKAN

- Causa: ausência de User-Agent nos requests HTTP.
- Solução: inclusão de header `User-Agent` em todas as requisições.

### Spark não lê CSV diretamente via HTTPS

- Solução: download local do arquivo antes do processamento pelo Spark.

### Avisos de memória do Spark

- Heap padrão insuficiente (~1GB).
- Solução: configuração explícita de `spark.driver.memory = 4g`.

### Porta padrão do SparkUI ocupada

- Spark automaticamente utilizou porta alternativa (4041).

---

## 🧠 Decisões Técnicas

- Spark em modo local para simplificação do ambiente.
- Parquet utilizado na Bronze por eficiência de armazenamento.
- Delta Lake nas camadas Silver e Gold para controle transacional.
- Separação clara entre ingestão e transformação.
- Bronze contém apenas dados crus.
- Transformações somente a partir da Silver.

---

## ✅ Qualidade de Dados

Planejado para Silver e Gold:

- Verificação de valores nulos
- Remoção de duplicados
- Validação de esquema
- Contagem de registros entre camadas

---

## 📈 Camada Gold (Planejado)

Exemplos de tabelas:

- Quantidade de ônibus por linha
- Volume de registros MCO por mês
- Métricas temporais
- Dados prontos para dashboards

---

## ⏱️ Orquestração

Atualmente executado via scripts Python.

Planejado:

- Airflow ou Databricks Workflows

---

## ▶️ Como Executar

### 1️⃣ Criar ambiente Conda

```bash
conda env create -f environment.yml
conda activate urban-pipeline
```

> O `environment.yml` define a versão do Python e dependências base do projeto.

---

### 2️⃣ Instalar dependências adicionais (se necessário)

```bash
pip install -r requirements.txt
```

> Utilizado para garantir compatibilidade exata das bibliotecas (Delta Lake, PySpark, etc).

---

### 3️⃣ Executar o pipeline completo

```bash
python run_pipeline.py
```

Este comando executa automaticamente:

- Bronze → ingestão e armazenamento em Parquet
- Silver → limpeza, padronização e enriquecimento (Delta Lake)
- Gold → agregações analíticas (Delta Lake)

---

## 🔁 Reprodutibilidade

Para reproduzir o projeto do zero basta:

```bash
git clone <repo>
cd urban-mobility-pipeline
conda env create -f environment.yml
conda activate urban-pipeline
pip install -r requirements.txt
python run_pipeline.py
```

---

## 🔮 Melhorias Futuras

- Carga incremental
- Particionamento por data
- Monitoramento
- Logs estruturados
- Alertas automáticos
- Deploy em cloud

---

## 🗺️ Diagrama de Arquitetura

```
CKAN APIs / CSV
        ↓
     Bronze
        ↓
     Silver
        ↓
      Gold
        ↓
     BI / ML
```

---

## 👤 Autor

Paulo Ricardo Dantas  
Projeto desenvolvido como estudo de caso para vaga de Engenheiro de Dados Pleno.

---

# ⭐ Nota

Este projeto prioriza clareza arquitetural, boas práticas de engenharia de dados e rastreabilidade completa do pipeline.
