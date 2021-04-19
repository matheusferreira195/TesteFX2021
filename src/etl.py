from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (BooleanType, ByteType, DateType, IntegerType,
                               LongType, ShortType, StringType, StructField,
                               StructType, TimestampType)
from pyspark.sql.window import Window
from itertools import chain
from config import Config

config = Config()

def create_spark_session():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("b2w") \
        .getOrCreate()
    
    return spark

def load_json_file(spark_session, file_path):
    df = spark_session.read \
            .format("json") \
            .option("header", "true") \
            .option("timestampFormat", "dd/MM/yyyy HH:mm:ss") \
            .load(file_path)
    
    return df

def calculate_steps(df):
    # Selecionando apenas as colunas relevantes a analise e ordenando de forma conveniente
    df_steps = df.select("load_timestamp", "visit_id", "page_type").sort("visit_id", "load_timestamp")

    # Definindo a função janela que permitirá comparar uma linha do df a anterior
    step_window = Window.partitionBy().orderBy("visit_id", "load_timestamp")
    linha_anterior_page_type = F.lag(df_steps.page_type).over(step_window)
    linha_anterior_visit_id = F.lag(df_steps.visit_id).over(step_window)

    # Criando coluna nova que comparará a linha atual a anterior, se for diferente, escreverá qual foi o passo anterior a esse
    df_steps = df_steps.withColumn("passo_anterior", F.when( (df_steps.page_type != linha_anterior_page_type) & (df_steps.visit_id == linha_anterior_visit_id), linha_anterior_page_type).otherwise(None))
    
    # Contagem das ocorrencias por par step-anterior / step-atual
    df_count = df_steps.groupby("page_type", "passo_anterior").count()

    return df_count

def tidy_df(df):
    # Renomeando colunas para nomes significativos e dropando ocorrencias nulas (descartando casos onde saiu para o mesmo passo que estava)
    df_count_final = df.select(F.col("passo_anterior"), F.col("page_type").alias("passo_atual"), F.col("count")).dropna()
    
    # Convertendo as strings em indices para facilitar analises posteriores
    mapeamento_tags = F.create_map([F.lit(tag) for tag in chain(*Config.TAG_PASSOS.items())])
    df_count_final = df_count_final.withColumn('passo_anterior', mapeamento_tags[df_count_final['passo_anterior']])
    df_count_final = df_count_final.withColumn('passo_atual', mapeamento_tags[df_count_final['passo_atual']])

    return df_count_final

def export_data(summary_df, raw_df):
    # Simula a escrita num banco de dados
    summary_df.toPandas().to_csv(f'{Config.OUTPUT_PATH}/navigational_data_steps.csv')
    raw_df.toPandas().to_csv(f'{Config.OUTPUT_PATH}/navigational_data_ingested.csv')

def main():
    
    spark_session = create_spark_session()
    file_path = Config.FILE_PATH
    
    raw_df = load_json_file(spark_session, file_path)

    summary_df = calculate_steps(raw_df)
    summary_df = tidy_df(summary_df)
    
    export_data(summary_df, raw_df)

    return summary_df
