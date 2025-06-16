package raw

import org.apache.spark.sql.{DataFrame, SparkSession}

object Raw {
  def loadTransactions(path: String)(implicit spark: SparkSession): DataFrame = spark.read.parquet(path)

  def loadCurrenciesRegistry(path: String)(implicit spark: SparkSession): DataFrame = spark.read.parquet(path)

  def loadReceiversRegistry(path: String)(implicit spark: SparkSession): DataFrame = spark.read.parquet(path)
}
