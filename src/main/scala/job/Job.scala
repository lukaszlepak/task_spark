package job

import enriched.Enriched
import org.apache.spark.sql.SparkSession
import raw.Raw
import refined.Refined

object Job {
  def processTransactions(month: Int, year: Int, transactionsInputPath: String, receiversInputPath: String, currenciesInputPath:String, outputPath: String)(implicit spark: SparkSession): Unit = {
    val rawTransactions = Raw.loadTransactions(transactionsInputPath)
    val rawReceiverRegistry = Raw.loadReceiversRegistry(receiversInputPath)
    val rawCurrencyRegistry = Raw.loadCurrenciesRegistry(currenciesInputPath)

    rawTransactions.show
    rawReceiverRegistry.show
    rawCurrencyRegistry.show

    val refinedTransactions = Refined.refineTransactions(rawTransactions, month, year)
    val refinedReceiverRegistry = Refined.refineReceiversRegistry(rawReceiverRegistry)
    val refinedCurrencyRegistry = Refined.refineCurrenciesRegistry(rawCurrencyRegistry)

    refinedTransactions.show
    refinedReceiverRegistry.show
    refinedCurrencyRegistry.show

    val enrichedTransactions = Enriched.enrichTransactions(refinedTransactions, refinedReceiverRegistry, refinedCurrencyRegistry)

    enrichedTransactions.show // just to show it works and produce proper result
    enrichedTransactions.write.partitionBy("PARTITION_DATE").format("parquet").mode("overwrite").save(outputPath)
  }
}
