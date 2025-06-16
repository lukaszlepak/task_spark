package refined

import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class RefinedCurrencySpec extends AnyWordSpec {

  "Refined object" should {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("sparkTask")
      .getOrCreate()

    val currencySchema = StructType(Seq(
      StructField("FROM_CURRENCY", StringType),
      StructField("TO_CURRENCY", StringType),
      StructField("RATE", DecimalType.SYSTEM_DEFAULT),
      StructField("PARTITION_DATE", StringType)
    ))

    val rawCurrency = spark.createDataFrame(List(
      Row("EUR",	"EUR",	BigDecimal(1.0), "20.01.2024"),
      Row("USD",	"EUR",	BigDecimal(0.9), "20.01.2024"),
      Row("GBP",	"EUR",	BigDecimal(1.2),	"20.01.2024"),
      Row("EUR",	"EUR",	BigDecimal(1.0),	"01.01.2024"),
      Row("USD",	"EUR",	BigDecimal(0.95),	"01.01.2024"),
      Row("GBP",	"EUR",	BigDecimal(1.25), "01.01.2024")
    ).asJava, currencySchema
    )

    "filter out invalid currency rates" in {
      Refined.refineCurrenciesRegistry(rawCurrency).count() shouldBe 3
    }

    "have 3 columns" in {
      Refined.refineCurrenciesRegistry(rawCurrency).columns.length shouldBe 3
    }
  }
}
