import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, lit

# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('datalake_enem_small_upsert')
logger.setLevel(logging.DEBUG)

# Definicao da Spark Session
spark = (SparkSession.builder.appName("DeltaExercise")
    # .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


logger.info("Importing delta.tables...")
from delta.tables import *


logger.info("Produzindo novos dados...")
enemnovo = (
    spark.read.format("delta")
        ##vai ter q arrumar pastas, partições por ano...

    .load("s3://datalake-eric-igti-edc-tf/staging-zone/enem")
)

# Define algumas inscricoes (chaves) que serao alteradas
inscricoes = [200001625680,
200002087222,
200003625161,
200003681653,
200004665417,
200006794353,
200005415692,
200004962008,
200003123544,
200002719199,
200003149625,
200002435247,
200004568005,
200003157529,
200005749762,
200003867656,
200006261689,
200005284494,
200002441594,
200001373961,
200002716561,
200002886129,
200002684764,
200002853246,
200002080465,
200001918966,
200004856662,
200004578008,
200004386936,
200002332587,
200004058493,
200004276648,
200005967737,
200002149360,
200003798493,
200004733752,
200003136075,
200004122286,
200004944262,
200004331797,
200004608691,
200006145315,
200001837194,
200003097172,
200002988796,
200006064887,
200002399441,
200006113041,
200001268177,
200004511148]


logger.info("Reduz a 50 casos e faz updates internos no municipio de residencia")
enemnovo = enemnovo.where(enemnovo.NU_INSCRICAO.isin(inscricoes))
enemnovo = enemnovo.withColumn("NO_MUNICIPIO_ESC", lit("NOVA CIDADE")).withColumn("CO_MUNICIPIO_ESC", lit(10000000))


logger.info("Pega os dados do Enem velhos na tabela Delta...")
    ##vai ter q arrumar pastas, partições por ano...

enemvelho = DeltaTable.forPath(spark, "s3://datalake-eric-igti-edc-tf/staging-zone/enem")


logger.info("Realiza o UPSERT...")
(
    enemvelho.alias("old")
    .merge(enemnovo.alias("new"), "old.NU_INSCRICAO = new.NU_INSCRICAO")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

logger.info("Atualizacao completa! \n\n")

logger.info("Gera manifesto symlink...")
enemvelho.generate("symlink_format_manifest")

logger.info("Manifesto gerado.")