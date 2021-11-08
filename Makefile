cnf ?= .env
include $(cnf)
export $(shell sed 's/=.*//' $(cnf))

# Get the latest tag
TAG=$(shell git describe --tags --abbrev=0)
GIT_COMMIT=$(shell git log -1 --format=%h)

# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

mkdir-diretorio-tz: ## Run para criar diretório no HDFS para armazenar dados brutos - Transient Zone (Zone transitório)
	  docker exec namenode hdfs dfs -mkdir -p $(TZ)
# make mkdir-diretorio-tz

ls-diretorio-tz: ## Run para listar diretório tz no HDFS - Transient Zone (Zone transitório)
	  docker exec namenode hdfs dfs -ls $(TZ)
# make ls-diretorio-tz

rm-diretorio-tz: ## Run para remover tudo no HDFS na pasta TZ - Transient Zone (Zone transitório)
	  docker exec namenode hdfs dfs -rm -r -f $(TZ)
# make rm-diretorio-tz

cp-arquivos-namenode: ## Exec para enviar arquivos contendo os dados para dentro do container namenode. Depois de copia, os arquivos são listados.
	  docker cp $$PWD/input_files/ namenode:$(INPUT)
	  docker exec namenode ls $(INPUT)
# make cp-arquivos-namenode

put-arquivos-tz: ## Exec para enviar arquivos do container para o HDFS. Depois do PUT, os arquivos são listados.
	  docker exec namenode hdfs dfs -put -f $(INPUT) $(TZ)
	  docker exec namenode hdfs dfs -ls $(TZ)covid/
# make put-arquivos-tz

ver-arquivo-tz: ## Exec para enviar arquivos do container para o HDFS. Depois do PUT, os arquivos são listados.
	  docker exec namenode hdfs dfs -cat $(TZ)covid/HIST_PAINEL_COVIDBR_2020_Parte1_06jul2021.csv | head -n 2
# make ver-arquivo-tz

ls-warehouse: ## Exec para listar diretorio no warehouse - HIVE. 
	  docker exec namenode hdfs dfs -ls $(WAREHOUSE)covid/
# make ls-arquivo-tz