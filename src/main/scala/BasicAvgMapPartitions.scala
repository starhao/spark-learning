import org.apache.spark._

object BasicAvgMapPartitions {


  /** *
    * Case Class 样例类 自动生成常用方法.
    * 1.变量默认val;
    * 2.创建伴生对象,实现apply方法,使用时不需要new;
    * 3.实现自己的toString、hashCode、copy、equals方法
    */
  case class AvgCount(var total: Int = 0, var num: Int = 0) {
    def merge(other: AvgCount): AvgCount = {
      total += other.total
      num += other.num
      this
    }

    def merge(input: Iterator[Int]): AvgCount = {
      input.foreach { it =>
        total += it
        num += 1
      }
      this
    }

    def avg(): Float = {
      total / num.toFloat
    }
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local").setAppName("app")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List(6, 6, 6, 6, 6, 6))

    val result = input.mapPartitions(part =>
      Iterator(AvgCount(0, 0).merge(part))) //part是个分区的迭代器,包含若干个数字,交给AvgCount对象的merge(Iterator)方法计算
      .reduce((x, y) => x.merge(y))         //每个分区的数算完后, 返回的是avgCount对象集合, 用AvgCount的merge(avgCount)方法进行聚合

    printf(result.avg().toString)
  }

  /** *
    * mapPartitions 对RDD中每一个分区的迭代器进行操作.
    */

}


