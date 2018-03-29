import org.apache.spark._

object BasicAvgWithKryo {
  //没明白什么意思
  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local").setAppName("BasicAvgWithKryo")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List(1, 2, 3, 4, 5, 6))

    val result = input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2))

    val avg = result._1 / result._2
    println(avg)
  }
}
