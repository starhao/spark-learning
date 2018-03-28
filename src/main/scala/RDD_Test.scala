import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader

object RDD_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    //----map()对RDD所有数求平方
    //val input = sc.parallelize(List(1, 2, 3, 3))
    //val result = input.map(x => x * x);
    //println(result)
    //-----

    //----pairRDD 的操作
    //    val lines = sc.parallelize(List(2, 2, 3, 4, 3, 6))
    //转成pairRDD
    //    val pairs = lines.map(x => (x, x+1))
    //    println(pairs.collect().mkString(","))
    //    (pairs.reduceByKey((x, y) => x + y).foreach(i => {
    //      println(i)
    //    }));
    //-------

    //    val start = System.currentTimeMillis()
    //----单词计数 reduceByKey方法
    //    val input = sc.textFile("/Users/wuchenghao/Temp/english.txt")
    //    val lines = input.flatMap(line => line.split(" "))
    //    val result = lines.map(x => (x, 1)).reduceByKey((x, y) => x + y,10)
    //    println(s"cost time : ${System.currentTimeMillis() - start}")
    //    result.collect().foreach(i => println(s"${i}"))
    //----

    //textFile读取CSV
    val freeStudent = sc.textFile("/Users/wuchenghao/Temp/原始csv/免学费综合信息三年级.CSV");
    val result = freeStudent.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }

    result.foreach(i => {
      println(i.toString())
    })
  }
}
