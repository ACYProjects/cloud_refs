import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try
import org.apache.spark.sql.SparkSession

object GcsExample {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("GcsExample")
      .getOrCreate()

    val inputPath = "gs://my-bucket/input/*.csv"
    val outputPath = "gs://my-bucket/output"

    def parseDate(dateString: String): Option[LocalDate] = {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      Try(LocalDate.parse(dateString, formatter)).toOption
    }

    def validateRow(row: Array[String]): Boolean = {
      row.length == 3 && parseDate(row(0)).isDefined && row(1).nonEmpty && row(2).toDoubleOption.isDefined
    }

    val inputData = spark.read
      .option("header", "true")
      .csv(inputPath)
      .rdd
      .map(row => row.toSeq.map(_.toString).toArray)
      .filter(validateRow)

    case class Data(date: LocalDate, name: String, value: Double)

    def rowToData(row: Array[String]): Data = {
      Data(parseDate(row(0)).get, row(1), row(2).toDouble)
    }

    val data = inputData.filterMap(row => Try(rowToData(row)).toOption)

    val startDate = LocalDate.parse("2022-01-01")
    val endDate = LocalDate.parse("2022-01-31")
    val filteredData = data.filter(d => d.date.isAfter(startDate) && d.date.isBefore(endDate))
    
    val groupedData = filteredData.groupBy(_.name).mapValues { group =>
      val sum = group.map(_.value).sum
      val count = group.length
      sum / count
    }

    // Convert the grouped data to a DataFrame and write it back to Google Cloud Storage
    val outputData = spark.createDataFrame(groupedData.toSeq).toDF("name", "average_value")
    outputData.write
      .option("header", "true")
      .csv(outputPath)

    // Stop the SparkSession
    spark.stop()
  }
}
