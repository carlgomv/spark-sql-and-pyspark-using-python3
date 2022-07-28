

#
# from transform_classes.create_transform import SDLF_CreateTransform
#

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import when
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

bankData = [
    (1001,"BOGOTA","BANCO",8622441612,870982,"CUENTA PUENTE ACH","REJECTED"),
    (1002,"POPULAR","BANCO",9644268579,103264,"CUENTA PUENTE ACH",""),
    (1006,"ITAU","BANCO",6618214252,643577,"",""),
    (1014,"ITAU*","BANCO",6618214252,449328,"","REJECTED"),
    (1032,"CAJA SOCIAL BCSC","BANCO",1111009053,0,"",""),
    (1052,"AVVILLAS","BANCO",2548338895,0,"","REJECTED"),
    (1053,"BOGOTA","BANCO",8622441612,850640,"CUENTA PUENTE ACH",""),
    (1065,"POPULAR","BANCO",9644268579,0,"",""),
    (1014,"ITAU*","BANCO",6618214252,449328,"",""),
    (2380,"OPERADOR 1","OPERADOR DE INFORMACION",1685991319,397595,"",""),
    (4685,"OPERADOR 2","OPERADOR DE INFORMACION",9077631582,437664,"",""),
    (2046,"OPERADOR 3","OPERADOR DE INFORMACION",8239190256,320653,"",""),
        ]

#schema = ["partyexternalidentifier","partyname","partytype"]
schema = ["partyexternalidentifier","partyname","partytype","numrastreo","valortransac","cuentapuente","status"]

bancos_df = spark.createDataFrame(data=bankData, schema = schema)
bancos_df.printSchema()
bancos_df.show(truncate=False)

# Table bancos
# resumenTransac_df = resumenTransac_df \
#                 .withColumn('numrastreo', resumenTransac_df["numrastreo"][0:8]) \
#                 .withColumnRenamed('numrastreo', "codigobancoorigen") \
#                 .withColumn('valorcero', when(resumenTransac_df["valortransac"] == 0, 1).otherwise(0)) \
#                 .withColumn('cuentapuente', udfCuentaPuente("nomdestinatario")) \
#                 .groupBy('idlote','fecha', 'periodo', 'year', 'month', 'day','codigotransaccion' \
#                         ,'codigobancoorigen','codigobancodestino','valorcero', 'cuentapuente') \
#                 .agg(F.count('idlote').alias('cantidad'), F.sum('valortransac').alias('valor'))

bancos_df.drop('partyexternalidentifier')

bancos_df = bancos_df.withColumn('partyname',when(bancos_df["partyname"] == 'ITAU*', 'ITAU').otherwise(bancos_df["partyname"])) \
                     .withColumnRenamed('partyname','nombre') \
                     .withColumn('numrastreo', bancos_df["numrastreo"][0:8]) \
                     .withColumnRenamed('numrastreo', "codigobancoorigen") \
                     .withColumn('valorcero', when(bancos_df["valortransac"] == 0, 1).otherwise(0)) \
                     .groupBy('nombre', 'partytype', 'codigobancoorigen', \
                        'valorcero', 'cuentapuente','status') \
                     .agg(F.count('nombre').alias('cantidad'), F.sum('valortransac').alias('valor'))

bancos_df.show()

bancos_df = bancos_df.filter("partytype = 'BANCO'") \
                     .filter("status <> 'REJECTED'")

bancos_df.show()


# bancos_df = bancos_df.withColumnRenamed('partyname','nombre') \
#                      .withColumnRenamed('partyexternalidentifier','codigobanco').show(truncate=False)

# bancos_df = bancos_df.withColumn('nombre', \
#                       when(bancos_df["nombre"] == 'ITAU*', 'ITAU') \
#                     .otherwise(bancos_df["nombre"])) \
#                     .na.fill('NO EXISTE EN EL CATALOGO', ["nombre"]) \
#                     .filter("partytype = 'BANCO'").show(truncate=False)
