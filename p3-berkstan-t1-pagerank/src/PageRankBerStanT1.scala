import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//TODO: Comment everything
object PageRankBerStanT1 {

  def main(args: Array[String]): Unit = {

    // Get SparkContext
    val conf = new SparkConf().setAppName("PageRankBerStan")
    val sc = new SparkContext(conf)

    // Fetch input and output file locations from input args
    val inputFile = args(0) // Testing "hdfs://10.10.1.1:9000/input_3/web-BerkStan.txt"
    val outputLocation = args(1) // Testing "hdfs://10.10.1.1:9000/output_3_berstan/"

    val rawData = sc.textFile(inputFile)

    val filteredData = rawData.filter(!_.startsWith("#"))

    val link2EachDest = filteredData.map(row => (row.split("\t")(0),row.split("\t")(1)))

    //    val links = link2EachDest.groupByKey().cache() // TODO: Cache this?
    val links = link2EachDest.groupByKey() // TODO: Cache this?

    var ranks = links.map(link => (link._1, 1.0))

    for (i <- 1 to 10) {

      val contributions = links.join(ranks).flatMap {
        case(_, (linksList, rank)) =>
          linksList.map(destUrl => (destUrl, rank / linksList.size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).mapValues(sum => (0.15 + (0.85 * sum)))
    }

    ranks.saveAsTextFile(outputLocation)

    sc.stop()
  }
}
