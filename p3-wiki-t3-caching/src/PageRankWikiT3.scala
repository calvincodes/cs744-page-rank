import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PageRankWikiT3 {

  def main(args: Array[String]): Unit = {

    // Get SparkContext
    val conf = new SparkConf().setAppName("PageRankWiki")
    val sc = new SparkContext(conf)

    // Fetch input and output file locations from input args
    val inputFiles = args(0) // Testing "hdfs://10.10.1.1:9000/input_3/enwiki-pages-articles/link-enwiki-20180601-pages-articles*"
    val outputLocation = args(1) // Testing "hdfs://10.10.1.1:9000/output_3_wiki/"

    // Read the data from input file into RDD
    val rawDataRdd = sc.textFile(inputFiles)

    // Pre-process 1. Convert everything to lowercase
    val lowerCasedData = rawDataRdd.map(_.toLowerCase)

    // Pre-process 2. Remove rows which have srcUrl but do not have a targetUrl
    val filteredData = lowerCasedData.filter(row => row.split("\t").length == 2)

    // Create RDD with each row as a Array[String]. Array(0) = srcUrl, Array(1) = destUrl
    val link2EachDest = filteredData.map(row => (row.split("\t")(0), row.split("\t")(1)))

    // Remove links which do contains ":", but do not starts with "Category:"
    val filteredLink2EachDest = link2EachDest.filter(row => !row._2.contains(":") || row._2.startsWith("Category:"))

    // Create RDD with each row as srlUrl -> List<destUrl> mapping
    val links = filteredLink2EachDest.groupByKey().cache()

    // Create RDD with each row as srlUrl -> 1.0 mapping
    var ranks = links.map(link => (link._1, 1.0))

    // Running PageRank for 5 iterations
    for (i <- 1 to 5) {
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
