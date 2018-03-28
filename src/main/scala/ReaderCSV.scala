import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.{StringReader, StringWriter}
import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter

object ReaderCSV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("readCSV");
    val sc = new SparkContext(conf);


    //读取免学费表
    val freeStudent = sc.textFile("/Users/wuchenghao/Temp/原始csv/免学费综合信息三年级.CSV")
    val freeStudentResult = freeStudent.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext();
    }

    //读取助学金表
    val grantStudent = sc.textFile("/Users/wuchenghao/Temp/原始csv/助学金资助名单.CSV")
    val grantStudentResult = grantStudent.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext();
    }

    //全市贫困户表
    val allPoorPeople = sc.textFile("/Users/wuchenghao/Temp/原始csv/全市扶贫基础数据(20180314总).CSV")
    val allPoorPeopleResult = allPoorPeople.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext();
    }
    //    println(allPoorPeopleResult.count())

    // ① freeStudent 是一个字符串array 例如:[string,string,string....]
    // ② freeStudentResult是个字符串组对象的array 例如[[string, string ...],[string, string ...],[string, string ...]... ]

    //        freeStudentResult.foreach(i => {
    //          println(i(5))
    //          i.foreach(j => {
    //            println(j)
    //          })
    //        })

    //    freeStudent.foreach(i => {
    //      println(i)
    //    })


    //map() => 返回结果是字符串组对象的array //flatMap()=> 返回结果是字符串array
    //    val splitResult = freeStudent.map(line => line.split(","))
    //    splitResult.foreach(i => {
    //      i.foreach(j => {
    //        println(j)
    //      })
    //      println("--------")
    //    })
    //    allPoorPeopleResult.foreach(i => {
    //      println(i(8))
    //      println("---")
    //    })

    val freeIDNumberRDD = freeStudentResult.map(f => (f(5), f))

    val poorPeopleRDD = allPoorPeopleResult.map(p => (p(8), ""))

    val grantIDNumberRDD = grantStudentResult.map(g => (g(3), g))

    val finalResult = freeIDNumberRDD.join(poorPeopleRDD).map(it => it._2._1)
    val t1 = System.currentTimeMillis()

    val grantPoorResult = grantIDNumberRDD.join(poorPeopleRDD).map(it => it._2._1)

    //助学金-贫困人口比对
    //println(grantPoorResult.count())

    //免学费-贫困人口比对
    println(finalResult.count())

    //TODO:写入CSV待完成
    finalResult.map(data => List("1","23","sd").toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter()
        val csvWriter = new CSVWriter(stringWriter)
//        csvWriter.writeAll()
        Iterator(stringWriter.toString)
      }.saveAsTextFile("./out.csv")

    println(s"taking: ${(System.currentTimeMillis() - t1).doubleValue() / 1000}s")

  }

}
