//PAGERANK - CUSTOM PARTITIONING

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner

object PageRankWikiWithPartitioning {

	class UrlPartitioner(numberOfPartitioners: Int) extends Partitioner {
	  override def numPartitions: Int = numberOfPartitioners

	  override def getPartition(key: Any): Int = {
	    Math.abs(key.asInstanceOf[String].hashCode()% numPartitions)  
	  }

	  override def equals(other: Any): Boolean = other match {
	    case partitioner: UrlPartitioner => partitioner.numPartitions == numPartitions
	    case _ => false
	  }
	}

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("PageRankWiki")
    val sc = new SparkContext(conf)

    // Reverted to local fs from the hdfs to avoid memory overflow error
    val inputFiles = "/proj/uwmadison744-f19-PG0/data-part3/enwiki-pages-articles/link-enwiki-20180601-pages-articles*" 

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

    val numPartitions = 400

    val links = filteredLink2EachDest.groupByKey().partitionBy(new UrlPartitioner(numPartitions)).cache() // TODO: Cache this?

    var ranks = links.map(link => (link._1, 1.0)).partitionBy(new UrlPartitioner(numPartitions))

    for (i <- 1 to 5) {

      val contributions = links.join(ranks).flatMap {
        case(_, (linksList, rank)) =>
          linksList.map(destUrl => (destUrl, rank / linksList.size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).mapValues(sum => (0.15 + (0.85 * sum))).partitionBy(new UrlPartitioner(numPartitions))
      //TODO: Check if the partitioning is required here
    }

//    ranks.saveAsTextFile("hdfs://128.104.223.172:9000/output_3_wiki/")
    val output = ranks.collect() // Final output passed back to driver

    sc.stop()
  }
}
