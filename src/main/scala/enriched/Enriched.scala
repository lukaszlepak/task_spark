package enriched

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, col, explode}

object Enriched {
  def enrichTransactions(transactions: DataFrame, receivers: DataFrame, currencies: DataFrame): DataFrame = {
    val filteredTransactions = transactions
      .join(receivers, Seq("RECEIVER_NAME", "COUNTRY"))

    val explodedTransactions = filteredTransactions
      .withColumn("GUARANTORS_EXPLODED", explode(col("GUARANTORS")))
      .withColumn("AMOUNT_CALC", col("GUARANTORS_EXPLODED.PERCENTAGE") * col("AMOUNT"))

    val transactionsByType = explodedTransactions
      .groupBy(col("GUARANTORS_EXPLODED.NAME"), col("COUNTRY"), col("CURRENCY"), col("PARTITION_DATE"))
      .pivot(col("TYPE"), Seq("CLASS_A", "CLASS_B", "CLASS_C", "CLASS_D"))
      .sum("AMOUNT_CALC")
      .na.fill(0)
      .sort(asc("NAME"))

    val transactionsWithRate = transactionsByType
      .join(currencies, transactionsByType("CURRENCY") === currencies("FROM_CURRENCY"))

    val windowSpec = Window.partitionBy("COUNTRY")

    transactionsWithRate
      .withColumn("AVG_CLASS_A", avg("CLASS_A").over(windowSpec) * col("RATE"))
      .withColumn("AVG_CLASS_B", avg("CLASS_B").over(windowSpec) * col("RATE"))
      .withColumn("AVG_CLASS_C", avg("CLASS_C").over(windowSpec) * col("RATE"))
      .withColumn("AVG_CLASS_D", avg("CLASS_D").over(windowSpec) * col("RATE"))
      .sort(asc("NAME"))
      .drop("RATE")
      .drop("CURRENCY")
      .drop("FROM_CURRENCY")
      .drop("TO_CURRENCY")
  }
}
