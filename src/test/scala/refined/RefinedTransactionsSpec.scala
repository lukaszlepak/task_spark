package refined

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DateType, DecimalType, StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate
import scala.jdk.CollectionConverters._

class RefinedTransactionsSpec extends AnyWordSpec {

  "Refined object" should {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("sparkTask")
      .getOrCreate()

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

    val rawTransactions = spark.createDataFrame(List(
      Row("T_001", Seq(("G_001", BigDecimal(0.6))), "COMP_1", "COMP_2", "CLASS_A", "ES", "EUR", BigDecimal(1000), "OPEN", LocalDate.parse("2024-01-01"), "25.01.2024"),
      Row("T_002", Seq(("G_002", BigDecimal(0.5))), "COMP_2", "COMP_3", "CLASS_B", "ES", "EUR", BigDecimal(2000), "OPEN", LocalDate.parse("2024-01-03"), "25.01.2024"),
      Row("T_003", Seq(("G_001", BigDecimal(0.5)),("G_003", BigDecimal(0.5))), "COMP_1", "COMP_4", "CLASS_C", "ES", "EUR", BigDecimal(3000), "CLOSED", LocalDate.parse("2024-01-05"), "25.01.2024"),
      Row("T_004", Seq(("G_005", BigDecimal(0.5))), "COMP_5", "COMP_6", "CLASS_A", "US", "USD", BigDecimal(4000), "OPEN", LocalDate.parse("2024-01-07"), "25.01.2024"),
      Row("T_005", Seq(("G_005", BigDecimal(0.6))), "COMP_5", "COMP_7", "CLASS_D", "US", "USD", BigDecimal(5000), "OPEN", LocalDate.parse("2024-01-09"), "25.01.2024"),
      Row("T_006", Seq(("G_007", BigDecimal(0.5))), "COMP_5", "COMP_8", "CLASS_B", "US", "USD", BigDecimal(6000), "OPEN", LocalDate.parse("2024-01-11"), "25.01.2024"),
      Row("T_007", Seq(("G_006", BigDecimal(0.2)),("G_007", BigDecimal(0.2))), "COMP_6", "COMP_7", "CLASS_A", "US", "USD", BigDecimal(7000), "OPEN", LocalDate.parse("2024-01-13"), "25.01.2024"),
      Row("T_008", Seq(("G_001", BigDecimal(0.5)),("G_004", BigDecimal(0.5))), "COMP_3", "COMP_2", "CLASS_B", "ES", "EUR", BigDecimal(8000), "OPEN", LocalDate.parse("2024-01-15"), "25.01.2024"),
      Row("T_009", Seq(("G_002", BigDecimal(0.5))), "COMP_4", "COMP_3", "CLASS_B", "ES", "EUR", BigDecimal(9000), "OPEN", LocalDate.parse("2022-01-01"), "25.01.2024"),
      Row("T_001", Seq(("G_001", BigDecimal(0.5))), "COMP_1", "COMP_2", "CLASS_A", "ES", "EUR", BigDecimal(1000), "OPEN", LocalDate.parse("2024-01-01"), "25.01.2024")
    ).asJava, transactionSchema
    )

    "filter out invalid currency rates" in {
      Refined.refineTransactions(rawTransactions, 1, 2024).count() shouldBe 7
    }
  }
}
