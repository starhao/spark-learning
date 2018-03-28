import org.apache.spark._

object BasicAvgFromFile {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local").setAppName("AvgFile")
    val sc = new SparkContext(conf)

    val input = sc.textFile("/Users/wuchenghao/Temp/input.txt")
    val result = input.map(_.toInt).aggregate((0, 0))((acc, value) =>
      (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avg = result._1 / result._2.toFloat
    println(result)
    println(avg)
  }
}
