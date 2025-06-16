package refined

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, max}

object Refined {
  def refineTransactions(dataFrame: DataFrame, month: Int, year: Int): DataFrame = {
    dataFrame
      .filter(col("STATUS") === "OPEN")
      .filter(functions.month(col("DATE")) === month)
      .filter(functions.year(col("DATE")) === year)
      .dropDuplicates("TRANSACTION_ID")
  }

  def refineReceiversRegistry(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("LATEST", max("PARTITION_DATE").over)
      .filter(col("PARTITION_DATE") >= col("LATEST"))
      .drop("PARTITION_DATE")
      .drop("LATEST")
  }

  def refineCurrenciesRegistry(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("LATEST", max("PARTITION_DATE").over)
      .filter(col("PARTITION_DATE") >= col("LATEST"))
      .drop("PARTITION_DATE")
      .drop("LATEST")
  }
}
