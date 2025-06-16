package refined

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class RefinedReceivers extends AnyWordSpec{

  "Refined object" should {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("sparkTask")
      .getOrCreate()

    val registrySchema = StructType(Seq(
      StructField("RECEIVER_NAME", StringType),
      StructField("COUNTRY", StringType),
      StructField("PARTITION_DATE", StringType)
    ))

    val rawReceivers = spark.createDataFrame(List(
      Row("COMP_1",	"ES",	"15.01.2024"),
      Row("COMP_2",	"ES",	"15.01.2024"),
      Row("COMP_3",	"ES",	"15.01.2024"),
      Row("COMP_4",	"ES",	"15.01.2024"),
      Row("COMP_5",	"US",	"15.01.2024"),
      Row("COMP_6",	"US",	"15.01.2024"),
      Row("COMP_7",	"US",	"15.01.2024"),
      Row("COMP_1",	"ES",	"01.02.2023"),
      Row("COMP_2",	"ES",	"01.02.2023"),
      Row("COMP_7",	"US",	"01.02.2023")
    ).asJava, registrySchema
    )

    "filter out invalid receivers rates" in {
      Refined.refineReceiversRegistry(rawReceivers).count() shouldBe 7
    }

    "have 2 columns" in {
      Refined.refineReceiversRegistry(rawReceivers).columns.length shouldBe 2
    }
  }
}
