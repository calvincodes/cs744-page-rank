import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

//TODO: Comment everything
object PageRankWiki {

  def main(args: Array[String]): Unit = {

    // Create Spark Context object
    val conf = new SparkConf().setAppName("PageRankWiki")
//      .setMaster("spark://c220g2-011101vm-1.wisc.cloudlab.us:7077")
    val sc = new SparkContext(conf)

    val inputFiles = "hdfs://10.10.1.1:9000/input_3/enwiki-pages-articles/link-enwiki-20180601-pages-articles*"

    val rawData = sc.textFile(inputFiles)

    val lowerCasedData = rawData.map(_.toLowerCase)

    val lowerCasedFilteredData = lowerCasedData.filter(row => row.split("\t").length == 2)

    val link2EachDest = lowerCasedFilteredData.map(row => (
      row.split("\t")(0),
      if (row.split("\t").size == 2) {
        row.split("\t")(1)
      } else {
        println(s"WTF! NO DATA FOR $row")
        row.split("\t")(1)
      }
    ))

    val filteredLink2EachDest = link2EachDest.filter(row => !row._2.contains(":") || row._2.startsWith("Category:"))

    val links = filteredLink2EachDest.groupByKey().cache() // TODO: Cache this?
//    val links = filteredLink2EachDest.groupByKey().persist(StorageLevel.DISK_ONLY) // TODO: Cache this?

    var ranks = links.map(link => (link._1, 1.0))

    for (i <- 1 to 5) {

      val contributions = links.join(ranks).flatMap {
        case(_, (linksList, rank)) =>
          linksList.map(destUrl => (destUrl, rank / linksList.size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).mapValues(sum => (0.15 + (0.85 * sum)))
    }

//    ranks.saveAsTextFile("hdfs://128.104.223.172:9000/output_3_wiki/")
    val output = ranks.collect() // Final output passed back to driver

    sc.stop()
  }
}
