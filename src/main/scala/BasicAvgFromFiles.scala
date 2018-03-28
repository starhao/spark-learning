import org.apache.spark._

object BasicAvgFromFiles {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("BasicAvgFiles")
    val sc = new SparkContext(conf);

    val input = sc.wholeTextFiles("/Users/wuchenghao/Temp/inputFiles")

    val result = input.mapValues { y =>
      val nums = y.split(" ").map(_.toDouble)
      nums.sum / nums.size.toDouble
    }
//    result.saveAsTextFile("./outputFile")
    result.foreach(i=>
      println(i)
    )
  }

}
