package raw

import org.apache.spark.sql.types.{ArrayType, DateType, DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Raw {
  def loadTransactions(path: String)(implicit spark: SparkSession): DataFrame = {
    val transactionSchema = StructType(Seq(
      StructField("TRANSACTION_ID", StringType),
      StructField("GUARANTORS", ArrayType(
        StructType(Seq(
          StructField("NAME", StringType),
          StructField("PERCENTAGE", DecimalType.SYSTEM_DEFAULT),
        )))),
      StructField("SENDER_NAME", StringType),
      StructField("RECEIVER_NAME", StringType),
      StructField("TYPE", StringType),
      StructField("COUNTRY", StringType),
      StructField("CURRENCY", StringType),
      StructField("AMOUNT", DecimalType.SYSTEM_DEFAULT),
      StructField("STATUS", StringType),
      StructField("DATE", DateType),
      StructField("PARTITION_DATE", StringType)
    ))

    spark.read.schema(transactionSchema).parquet(path)
  }

  def loadCurrenciesRegistry(path: String)(implicit spark: SparkSession): DataFrame = {
    val currencySchema = StructType(Seq(
      StructField("FROM_CURRENCY", StringType),
      StructField("TO_CURRENCY", StringType),
      StructField("RATE", DecimalType.SYSTEM_DEFAULT),
      StructField("PARTITION_DATE", StringType)
    ))

    spark.read.schema(currencySchema).parquet(path)
  }

  def loadReceiversRegistry(path: String)(implicit spark: SparkSession): DataFrame = {
    val registrySchema = StructType(Seq(
      StructField("RECEIVER_NAME", StringType),
      StructField("COUNTRY", StringType),
      StructField("PARTITION_DATE", StringType)
    ))

    spark.read.schema(registrySchema).parquet(path)
  }
}
