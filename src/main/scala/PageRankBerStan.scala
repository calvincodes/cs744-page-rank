import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//TODO: Comment everything
object PageRankBerStan {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PageRankBerStan")
    val sc = new SparkContext(conf)

    val webBerkStanLocation = "hdfs://128.104.223.172:9000/input_3/web-BerkStan.txt"

    val rawData = sc.textFile(webBerkStanLocation)

    val filteredData = rawData.filter(!_.startsWith("#"))

    val link2EachDest = filteredData.map(row => (row.split("\t")(0),row.split("\t")(1)))

    val links = link2EachDest.groupByKey().cache() // TODO: Cache this?

    var ranks = links.map(link => (link._1, 1.0))

    for (i <- 1 to 10) {

      val contributions = links.join(ranks).flatMap {
        case(_, (linksList, rank)) =>
          linksList.map(destUrl => (destUrl, rank / linksList.size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).mapValues(sum => (0.15 + (0.85 * sum)))
    }

    ranks.saveAsTextFile("hdfs://128.104.223.172:9000/output_3_berstan/")
//    val output = ranks.collect() // Final output passed back to driver

    sc.stop()
  }
}
