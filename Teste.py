import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
                .builder \
                .appName("Teste") \
                .config("spark.some.config.option", "some-value") \
                .getOrCreate()

def Lista(path):
    header = ['host','data_hora','requisicao','codigo','bytes']
    lista = []
    with open(path,'r') as arq:
        for line in arq.readlines():
            Dict = {}
            linha = line.split(' ')
            linhaLimpa = []
            for c in range(len(linha)):
                if (linha[c][:1] == '-') | (linha[c].replace('"','') == 'GET') | (linha[c][:1] == 'H'):
                    pass
                else:
                    linhaLimpa.append(linha[c].replace('[','').replace('\n',''))
            if len(linhaLimpa) == 4:
                linhaLimpa.append(0)
            for h in range(len(header)):
                Dict[header[h]] = linhaLimpa[h]
            lista.append(Dict)
    arq.close()
    return lista

def Hosts_Unicos(df):
    df = df.groupBy('host').count()
    df = df.filter(col('count') == '1')
    print('Numero de Hosts unico: {}'.format(df.count()))
        
def Erro_404(df):
    df_total = df.filter(col('codigo') == '404')
    print('Total de erro 404: {}'.format(df_total.count()))
    
    df_maior_quantidade = df.groupBy('requisicao','codigo').count()
    df_maior_quantidade = df_maior_quantidade.filter(col('codigo') == '404')
    df_maior_quantidade = df_maior_quantidade.orderBy('count', ascending=False)
    print('As 5 urls que mais causaram erro 404:')
    df_maior_quantidade.show(5,False)
    
    df_dia = df.withColumn(
        'data_hora',
        substring(df['data_hora'],1,10)
    )
    df_dia = df_dia.groupBy('data_hora','codigo').count()
    df_dia = df_dia.filter(col('codigo') == '404')
    print('Quantidade de erros 404 por dia:')
    df_dia.show(30,False)
    
def Total_Bytes(df):
    df = df.withColumn('marcador', lit('1'))
    df = df.withColumn('bytes', df['bytes'].cast(IntegerType()))
    df = df.groupBy('marcador').sum('bytes')
    df = df.drop('marcador')
    df = df.withCOlumnRenamed('sum(bytes)','Total_Bytes')
    print('Total de Bytes retornados:')
    df.show()
              
df = spark.createDataFrame(Lista('/FileStore/tables/access_log_Jul95'))

Hosts_Unicos(df)
Erro_404(df)
Total_Bytes(df)

spark.stop()
