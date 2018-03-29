import org.apache.spark._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.ClassTag

object BasicIntersectByKey {

  def intersectByKey[K: ClassTag, V: ClassTag](rdd1: RDD[(K, V)], rdd2: RDD[(K, V)]): RDD[(K, V)] = {
    /** *
      * cogroup 具有相同Key的K,V进行合并.例子里的合并结果为:
      * (1,(CompactBuffer(panda),CompactBuffer(pandas)))
      * (3,(CompactBuffer(),CompactBuffer(OK)))
      * (2,(CompactBuffer(happy),CompactBuffer(cat)))
      */
    val cur = rdd1.cogroup(rdd2)

    cur.foreach(i => println(i))

    /** *
      * 两个rdd中有共同Key并且Value不为空的筛选出来
      */
    cur.flatMapValues {
      case (Nil, _) => None
      case (_, Nil) => None
      case (x, y) => x ++ y //++ 操作符:合并两个迭代器 返回 RDD[K,(Iterable[V],Iterable[V])]
    }

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("BasicIntersectByKey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List((1, "panda"), (2, "happy")))
    val rdd2 = sc.parallelize(List((1, "pandas"), (2, "cat"), (3, "OK")))
    val iRdd = intersectByKey(rdd1, rdd2)
    println("---")
    //    iRdd.foreach(i => println(i))
    val panda: List[(Int, String)] = iRdd.collect().toList
    panda.map(println(_))
    sc.stop()
  }

}
