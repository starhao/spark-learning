import java.util.Properties
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark._

object BasicSparkToMysql {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkSQLTestCase")
    val sc = new SparkContext(conf)

    /** *
      * 过时方法
      */
    //    val sqlContext = new SQLContext(sc)
    //
    //    val jdbcDF = sqlContext.read.format("jdbc").options(
    //      Map("url" -> "jdbc:mysql://localhost:3306/test", "user" -> "root", "password" -> "", "dbtable" -> "free")
    //    ).load()
    //
    //    jdbcDF.registerTempTable("table1")
    //
    //    sqlContext.sql("select * from table1").show()
    /** ****/

    /***
      * 2.3 的sparkSession.builder()方法
      */
    val sparkSession = SparkSession.builder().master("local[*]").appName("mysql").getOrCreate()
    //属性集
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "")

    val url = "jdbc:mysql://localhost:3306/test";
    val jdbcDF1 = sparkSession.read.jdbc(url, "free", connectionProperties)
    jdbcDF1.createOrReplaceTempView("table1")
    sparkSession.sql("select * from table1").take(1000).foreach(println)
  }
}