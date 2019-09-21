import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//TODO: Comment everything
object PageRank {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    sc.stop()
  }
}
