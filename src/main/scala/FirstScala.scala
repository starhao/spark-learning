import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object FirstScala {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("myApp")
    val sc = new SparkContext(conf)

    println("begin")

    //-----统计log文件的request数量
    val accessLogFile = sc.textFile("/Users/wuchenghao/Temp/access.log")
    val filted = accessLogFile.cache()
    filted.count()

//    input.persist()
//    println("共"+input.count()+"条")
//
//    val POST_Result = input.filter(lines => lines.contains("POST"))
//    println("POST共:" + POST_Result.count() + "条")
//
//    val GET_Result = input.filter(lines=>lines.contains("GET"))
//    println("GET共:"+GET_Result.count()+"条")

    val start = System.currentTimeMillis()
    val headResult = filted.filter(_.contains("HEAD")).count()
    val postEnd = System.currentTimeMillis()
    println(s"head count ${headResult}, cost ${postEnd - start}")

    //postResult = [read(xxx) => cache()]  => filter(()->)

    val getResult = filted.filter(_.contains("GET")).count()
    val getEnd = System.currentTimeMillis()
    println(s"get count ${getResult}, cost ${getEnd - postEnd}")

    println(s"total cost ${System.currentTimeMillis() - start}")
    //getResult = [read(xxx) => cache()]  => filter(()->)


    //-------



    //    val words = input.flatMap(line=>{line.split(" ")})
    //    val counts = words.map(word=>(word,1)).reduceByKey{case (x,y)=>x+y}
    //    counts.saveAsTextFile("output")


  }
}
