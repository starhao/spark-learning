import org.apache.spark._
import org.apache.spark.sql.SparkSession

object BasicParseJson {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local").setAppName("BasicParseCsv")
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

    // 解析单行的json数据文件
    //val df = spark.read.format("json").json("/Users/wuchenghao/Temp/spark-example-data/json_single_line.json")

    //解析多行json数据文件
    val df = spark.read.option("multiline","true").json("/Users/wuchenghao/Temp/spark-example-data/json_multi_line.json")
    df.show()
  }
}