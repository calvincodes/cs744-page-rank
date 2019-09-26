import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PageRankBerkStanT1 {

  def main(args: Array[String]): Unit = {

    // Get SparkContext
    val conf = new SparkConf().setAppName("PageRankBerStan")
    val sc = new SparkContext(conf)

    // Fetch input and output file locations from input args
    val inputFile = args(0) // Testing "hdfs://10.10.1.1:9000/input_3/web-BerkStan.txt"
    val outputLocation = args(1) // Testing "hdfs://10.10.1.1:9000/output_3_berstan/"

    // Read the data from input file into RDD
    val rawData = sc.textFile(inputFile)

    // Pre-process 1. Ignore rows with comments in the initial few lines of the data
    val filteredData = rawData.filter(!_.startsWith("#"))

    // Create RDD with each row as a Array[String]. Array(0) = srcUrl, Array(1) = destUrl
    val link2EachDest = filteredData.map(row => (row.split("\t")(0),row.split("\t")(1)))

    // Create RDD with each row as srlUrl -> List<destUrl> mapping
    val links = link2EachDest.groupByKey() // TODO: Cache this?

    // Create RDD with each row as srlUrl -> 1.0 mapping
    var ranks = links.map(link => (link._1, 1.0))

    // Running PageRank for 10 iterations
    for (i <- 1 to 10) {
      // Compute contribution of each destUrl from the corresponding srcUrl.
      // This will result in destUrl -> contributionFromSrcUrl mapping.
      val contributions = links.join(ranks).flatMap {
        case(_, (linksList, rank)) =>
          linksList.map(destUrl => (destUrl, rank / linksList.size))
      }
      // Reduce the contributions RDD on its key (destUrl) and use the sum to compute rank using PageRank formula.
      ranks = contributions.reduceByKey((x,y) => x+y).mapValues(sum => (0.15 + (0.85 * sum)))
    }

    // Save resultRdd as a text file at user provided outputLocation
    ranks.saveAsTextFile(outputLocation)

    sc.stop()
  }
}
