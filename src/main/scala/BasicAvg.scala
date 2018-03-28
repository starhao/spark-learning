import org.apache.spark._
import org.apache.spark.rdd.RDD

object BasicAvg {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }

    val sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"))

    val input = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    val result = computeAvg(input)
    val avg = result._1 / result._2.toFloat
    println(avg)
  }

  // 个人理解: aggregate函数 (0,0)为初始元素, 第一次遍历时(x,y)相当于((0,0),y),把遍历的第一个元素代入到y,变成((0,0),1)
  // 通过给的函数(x._1 + y, x._2 + 1)计算过程为 ((0+1,0+1),1) ---> ((1,1),1) 算完把最后一个1 换成第二个元素,变成((1,1),2)
  // 再用(x._1 + y, x._2 + 1)函数计算:((1+2,1+1),1) ---> ((3,2),2) ....以此下去.换到最后一个为止.
  // 实际spark是分布式计算,分成多块list数组 , 用函数 (x._1 + y._1, x._2 + y._2) 进行聚合
  def computeAvg(input: RDD[Int]) = {
    input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2))
  }
}
