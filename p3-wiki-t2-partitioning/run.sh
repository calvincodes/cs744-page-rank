echo 'Running command: "sbt package"'
echo ""
sbt package

echo ""
echo 'Submitting spark job'
echo ""
spark-submit \
  --class "PageRankWikiT2" \
  target/scala-2.11/cs744-spark-page-rank_2.11-0.1.jar \
  "$1" \
  "$2"