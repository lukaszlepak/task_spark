import job.Job
import org.apache.spark.sql.SparkSession
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}

class JobSpec extends AnyWordSpec{

  "Job" should {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("sparkTask")
      .getOrCreate()

    "process data and save result file" in {
      Job.processTransactions(
        month = 1,
        year = 2024,
        transactionsInputPath = "src/test/resources/transactions.parquet",
        receiversInputPath = "src/test/resources/receivers.parquet",
        currenciesInputPath = "src/test/resources/currencies.parquet",
        outputPath = "src/test/resources/result"
      )

      Files.list(Paths.get("src/main/resources/result")).toList should not be empty
    }
  }
}
