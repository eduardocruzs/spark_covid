// Acessar o spark-shell 
spark-shell --conf spark.hadoop.hive.exec.max.dynamic.partitions=30000

// Importar pacotes
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// Criar schema e ler dados vinculando ao schema.
val columnsList = List( StructField("regiao", StringType), StructField("estado", StringType), StructField("municipio", StringType), StructField("coduf", StringType), StructField("codmun", StringType), StructField("codRegiaoSaude", StringType), StructField("nomeRegiaoSaude", StringType), StructField("data", DateType), StructField("semanaEpi", StringType), StructField("populacaoTCU2019", LongType), StructField("casosAcumulado", LongType), StructField("casosNovos", LongType), StructField("obitosAcumulado", LongType), StructField("obitosNovos", LongType), StructField("Recuperadosnovos", LongType), StructField("emAcompanhamentoNovos", LongType), StructField("interiormetropolitana", StringType))
val covidSchema = StructType(columnsList)
val covidDF = spark.read.option("header","true").option("delimiter", ";").schema(covidSchema).csv("/user/eduardo/spark_covid/tz/covid/*.csv")

// Setar configurações do Hive para permitir particionamento dinâmico 
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

// Salvar dados em uma tabela hive particionada por "municipio"
covidDF.write.mode("overwrite").partitionBy("municipio").format("hive").saveAsTable("covid")

// Gerando a visualização do Total de:
- Casos Recuperados 
- Em Acompanhamento
import org.apache.spark.sql.functions._
val casosRecAcom = covidDF.sort(desc("data")).filter(covidDF("regiao") === "Brasil").limit(1).agg(format_number(max("Recuperadosnovos"), 1).alias("Casos_Recuperados"),format_number(max("emAcompanhamentoNovos"),1).alias("Em_Acompanhamento"))
casosRecAcom.show()

/* 
Gerando a visualização do Total de:
- Casos Confirmados
  - Acumulado
  - Casos novos 
  - Incidência 
*/
 val casosConfirmados = covidDF.sort(desc("data")).filter(covidDF("regiao") === "Brasil").limit(1).agg(format_number(max("casosAcumulado"),1).alias("Casos_Acumulados"),format_number(max("casosNovos"),1).alias("Casos_Novos"), format_number(sum((covidDF("casosAcumulado") / covidDF("populacaoTCU2019")) * 100000),1).alias("Incidencia"))
 casosConfirmados.show()

 /* 
 Gerando a visualização do Total de:
 - Óbitos Confirmados
   - Acumulado
   - Casos Novos
   - Letalidade
   - Mortalidade 
 */
  val obitosConfirmados = covidDF.sort(desc("data")).filter(covidDF("regiao") === "Brasil").limit(1).agg(format_number(max("obitosAcumulado"),1).alias("Obitos_Acumulados"),format_number(max("obitosNovos"),1).alias("Obitos_Novos"), format_number(sum((covidDF("obitosAcumulado") / covidDF("casosAcumulado")) * 100),1).alias("Letalidade"), format_number(sum((covidDF("obitosAcumulado") / covidDF("populacaoTCU2019")) * 100000),1).alias("Mortalidade"))
  obitosConfirmados.show()

 // Salvando a visualização de Casos Recuperados e Casos em acompanhamento em uma tabela Hive
 casosRecAcom.write.mode("overwrite").format("hive").saveAsTable("casosRecAcom")

 // Salvando a visualização de Casos Confirmados em formato parquet e compressão snappy
 casosConfirmados.write.option("compression", "snappy").parquet("/user/eduardo/casosConfirmados")

 // PENDENTE # Salvando a visualização de Obitos Confirmados em um topico no KAFKA
 obitosConfirmados.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("topic", "topic-obitosConfirmados").save()

 // Criar visualização da Sintese dos dados de casos, obitos, incidencia e mortalidade.
 val sinteseEstados = covidDF.filter(covidDF("regiao") .isNotNull).groupBy("estado", "regiao").agg(max("casosAcumulado").alias("casosAcumulado"), max("obitosAcumulado").alias("obitosAcumulado"), max("populacaoTCU2019").alias("populacaoTCU2019"), last("data").alias("data")).sort(asc("regiao"),asc("estado"),desc("data"))
 val sinteseRegiao = sinteseEstados.groupBy("regiao").agg(sum("casosAcumulado").alias("Casos"), sum("obitosAcumulado").alias("Obitos"), sum("populacaoTCU2019")alias("populacaoRegiao"), last("data").alias("Atualizacao")).sort(asc("regiao"))
 val sinteseGeral = sinteseRegiao.withColumn("Incidencia", (sinteseRegiao("Casos") / sinteseRegiao("populacaoRegiao")) * 100000).withColumn("Mortalidade", (sinteseRegiao("Obitos") / sinteseRegiao("populacaoRegiao")) * 100000)
 val sinteseFinal = sinteseGeral.groupBy("regiao").agg(format_number(last("Casos"),1).alias("Casos"),format_number(last("Obitos"),1).alias("Obitos"),format_number(last("Incidencia"),1).alias("Incidencia"),format_number(last("Mortalidade"),1).alias("Mortalidade"),last("Atualizacao"))
 sinteseFinal.show()