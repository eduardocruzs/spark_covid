# SPARK_COVID - EDUARDO CRUZ

Projeto para integrar dados públicos da COVID e gerar análises.

## Este projeto está dividido e organizado em algumas etapas, liguagens e arquivos. 

 - **Etapa 1:**
    * Envio dos arquivos para dentro do container
    * Envio dos arquivos do container para o HDFS
    * Execução de todas as demandas do projeto a partir do spark-shell
    * Integração com o KAFKA OK
    * Integração com o ELASTICSEARCH OK
    * Desenhado Dashboad no Kibana - Link de acesso abaixo:
    ### Dashboard - Dados Covid - Eduardo Cruz - Projeto Final Academy Semantix
    http://104.197.191.85:5601/app/dashboards#/view/05983ef0-4285-11ec-9dba-270c2f86924e?_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A(from%3Anow-15m%2Cto%3Anow))

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

    

**.env** - Arquivo que contém as variáveis de ambiente que serão enviadas para o container na execução dos comandos make.

**/scala/spark-shell.scala** - Arquivo com todo o código para execução das etapas do projeto dentro do skark-shell.

**/pyspark/spark_covid.ipynb** - arquivo do Jupyter Notebook com todas as demandas do projeto em python.

**/input_files** - Diretório contendo os arquivos csv integrados.

## Visualização dos resultados gerados a partir do spark-shell 

Gerando a visualização do Total de:
- Casos Recuperados 
- Em Acompanhamento

![image](https://user-images.githubusercontent.com/79167966/140811106-7f110f09-ce83-436a-a502-992a00c94c44.png)

Gerando a visualização do Total de:
- Casos Confirmados
  - Acumulado
  - Casos novos 
  - Incidência 

![image](https://user-images.githubusercontent.com/79167966/140811278-d5a0eb42-0213-4209-9217-ec6f763f337a.png)

Gerando a visualização do Total de:
 - Óbitos Confirmados
   - Acumulado
   - Casos Novos
   - Letalidade
   - Mortalidade

![image](https://user-images.githubusercontent.com/79167966/140811416-beed766b-5a8a-4123-80c7-c98914b4a8bf.png)

Salvando a visualização de Obitos Confirmados em um topico no KAFKA e demonstrando o resultado a partir de um consumer no KAFKA
![image](https://user-images.githubusercontent.com/79167966/140811575-25d96c44-36cb-4482-ac2a-be149c10e04d.png)

Gerando a visualização da Sintese dos dados de casos, obitos, incidencia e mortalidade.
![image](https://user-images.githubusercontent.com/79167966/140812433-e2d1bc75-6b16-4ce7-876f-bc4c02a513b5.png)

