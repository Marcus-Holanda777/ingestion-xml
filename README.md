# Pipeline de Dados com Camadas Bronze, Silver e Delta Lake Gold para Exportação de XML do SQL Server ao MinIO

O objetivo deste projeto de engenharia de dados é extrair informações em formato XML referente a notas fiscais eletrônicas (NFE) de um banco de dados SQL Server, processá-las e armazená-las em um sistema de armazenamento baseado no MinIO, organizando os dados em três camadas: Bronze, Silver e Gold. Cada camada tem um propósito específico no ciclo de vida dos dados, visando transformar dados brutos em informações úteis e prontas para análise, de maneira escalável e eficiente.

![alt text for screen readers](img/project_xml.png)

## Tecnologias utilizadas

| Tecnologia        | Função Principal                                             |
|-------------------|--------------------------------------------------------------|
| **Python**        | Linguagem principal para desenvolvimento do pipeline         |
| **PyODBC**        | Conectar e extrair dados XML do SQL Server                   |
| **lxml**          | Processar e validar dados XML                                |
| **Pandas**        | Manipular e transformar dados nas camadas Silver             |
| **PyArrow**       | Manipular e converter dados para Parquet                     |
| **Delta Lake**    | Armazenar dados na camada Gold com suporte a transações ACID |
| **Boto3**         | Interagir com o MinIO para upload/download de arquivos       |
| **S3/MinIO**      | Sistema de armazenamento distribuído compatível com S3       |
| **Docker**        | Gerenciamento de contêineres para garantir ambiente isolado  |
| **DuckDB**        | Consultar e ler dados de tabelas Delta localmente            |
| **DBeaver**       | Editor SQL para consultas e manipulação de dados             |

### Estrutura de Dados no MinIO
- **Bronze**: Dados brutos extraídos do SQL Server (XML).
- **Silver**: Dados transformados e limpos (Parquet).
- **Gold**: Dados agrupados e prontos para análise (Delta Lake).

1. **Camada Bronze**
 - **Descrição**: Nesta camada, os dados são armazenados em seu formato bruto, exatamente como foram extraídos da fonte (SQL Server, no caso). Não são realizadas transformações ou limpezas significativas.
 - **Objetivo**: Manter um histórico fiel dos dados originais, preservando a integridade dos dados brutos para permitir rastreamento de qualquer erro ou mudança.
 - **Formato**: Arquivos XML no MinIO. 

