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