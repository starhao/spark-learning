import scala.io.Source;

object AccessLog {
  def main(args: Array[String]): Unit = {
    var getResult = 0
    var postResult = 0
    var total = 0

    val start = System.currentTimeMillis();

    val accessLogFile = Source.fromFile("/Users/wuchenghao/Temp/access.log").getLines()
    println(s"read time : ${System.currentTimeMillis() - start}")
    accessLogFile.foreach(i => {
      total += 1
      if (i.contains("GET")) {
        getResult += 1
      } else if (i.contains("POST")) {
        postResult += 1
      }
    })

    val end = System.currentTimeMillis()
    println(s"cost time: ${end - start}")
    println(s"总数:${total}条")
    println(s"get count :${getResult}条")
    println(s"post count :${postResult}条")
  }
}
