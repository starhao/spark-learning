import org.apache.spark._
import org.eclipse.jetty.client.{ContentExchange, HttpClient}


object BasicMapPartitions {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setMaster("local").setAppName("MapPartitions")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List("KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB"))

    /***
      * mapPartitions方法以迭代器为单位执行任务,如下开HttpClient接口不需要每个元素开一个, 只要每个数据块开一个,数据块的数据共用这一个HttpClient
      */
    val result = input.mapPartitions {
      sings =>
        val client = new HttpClient()
        client.start()
        sings.map { sign =>
          val exchange = new ContentExchange(true)
          exchange.setURL(s"http://qrzcq.com/call/${sign}")
          client.send(exchange)
          exchange
        }.map { exchange =>
          exchange.waitForDone()
          exchange.getResponseContent()
        }
    }
    println(result.collect().mkString(","))
  }
}
