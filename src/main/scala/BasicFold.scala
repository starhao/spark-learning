
import org.apache.spark.{SparkConf, SparkContext}

object BasicFold {
  def main(args: Array[String]): Unit = {

    //
    val sc = new SparkContext("local", "fold_functional")

    //用来求和
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.fold(0)((x, y) => (x + y))
    println(result)

    //用来找出最大值
    val employeeData = List(("jack", 1000), ("Bob", 2000), ("Carl", 30000))
    val employeeRDD = sc.makeRDD(employeeData)
    val dunmmyEmployee = ("dummy", 0)
    val maxSalaryEmployee = employeeRDD.fold(dunmmyEmployee)((acc, employee) => {
      if (acc._2 < employee._2) employee else acc
    })
    println(maxSalaryEmployee)

    //
  }
}

//fold in spark :
// def fold[T](acc:T)((acc,value)=>acc)
// T : 返回一个RDD数据
// acc:初始值或RDD
// acc value 。acc:一个fold操作后返回的值的累加器
// value, 下一个被操作的值