import com.typesafe.config.{Config, ConfigFactory}
import job.Job
import org.apache.spark.sql.SparkSession


object App {
  def main(args: Array[String]): Unit = {

    val config: Config = ConfigFactory.load

    val month = config.getInt("month")
    val year = config.getInt("year")
    val transactionsInputPath = config.getString("transactionsInputPath")
    val receiversInputPath = config.getString("receiversInputPath")
    val currenciesInputPath = config.getString("currenciesInputPath")
    val outputPath = config.getString("outputPath")

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("sparkTask")
      .getOrCreate()

    Job.processTransactions(month, year, transactionsInputPath, receiversInputPath, currenciesInputPath, outputPath)
  }
}

