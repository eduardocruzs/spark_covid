# SPARK_COVID - EDUARDO CRUZ

Projeto para integrar dados públicos da COVID e gerar análises.

## Este projeto está dividido e organizado em algumas etapas, liguagens e arquivos. 

 - **Etapa 1:**
    * Envio dos arquivos para dentro do container
    * Envio dos arquivos do container para o HDFS
    * Execução de todas as demandas do projeto a partir do spark-shell

- **Etapa 2:** 
    * Execução de todas as demandas do projeto a partir do jupyter-notebook

- **Etapa 3:**
    * Transformação de todo código em uma aplicação spark

### Arquivos: 

**Makefile** - Foi criado esse Makefile para simplificar a execução de comandos dentro dos containers e o uso de variáveis de ambiente. 

    Nele foram criadas as seguintes rotinas: 

        make help                           Exibe esta ajuda, com a lista de comandos e o que cada um faz. 

        make mkdir-diretorio-tz             Run para criar diretório no HDFS para armazenar dados brutos Transient Zone (Zone transitório).

        make ls-diretorio-tz                Run para listar diretório tz no HDFS - Transient Zone (Zone transitório).

        make rm-diretorio-tz                Run para remover tudo no HDFS na pasta TZ - Transient Zone (Zone transitório).

        make cp-arquivos-namenode           Exec para enviar arquivos contendo os dados para dentro do container namenode. Depois de copia, os arquivos são listados.

        make put-arquivos-tz                Exec para enviar arquivos do container para o HDFS. Depois do PUT, os arquivos são listados.

        make ver-arquivo-tz                 Exec para enviar arquivos do container para o HDFS. Depois do PUT, os arquivos são listados.

        make ls-warehouse                   Exec para listar diretorio no warehouse - HIVE. 

    

**.env** - Arquivo que contém as variáveis de ambiente que será enviadas para o container na execução dos comandos make.

**/scala/spark-shell.scala** - Arquivo com todo o código para execução das etapas do projeto dentro do skark-shell.

**/pyspark/spark_covid.ipynb** - arquivo do Jupyter Notebook com todas as demandas do projeto em python. 

**/input_files** - Diretório contendo os arquivos csv integrados.

