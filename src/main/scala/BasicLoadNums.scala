import org.apache.spark._
import org.apache.spark.SparkContext._

object BasicLoadNums {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("BasicLoadNums")
    val sc = new SparkContext(conf)

    val file = sc.textFile("/Users/wuchenghao/Temp/access.log")
    val errorLines = sc.accumulator(0)
    val dataLines = sc.accumulator(0)

    val counts = file.flatMap(line => {
      try {
        val input = line.split(" ")
        val data = Some((input(0), input(1)))
        dataLines += 1
        data
      } catch {
        case e: java.lang.NumberFormatException => {
          errorLines += 1
          None
        }
        case e: java.lang.ArrayIndexOutOfBoundsException => {
          errorLines += 1
          None
        }
      }
    }).reduceByKey(_ + _)

    counts.foreach(i => println(i))
  }
}

/***
  *避免使用null , null是关键字不是对象,对它的调用任何方法都是非法的.
  * Scala中函数返回值或变量可使用Option类型,有值时用Some包起来,没有值的时候用None.
  * Some代表成功返回了一个String,None代表没有字符串可以给你
  **/