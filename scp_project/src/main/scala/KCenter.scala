import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.Map
import scala.io.Source
object KCenter extends clustering_alg {
    val k: Int = 3
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder
          .appName("KMeansSpark")
          .config("spark.master", "local")
          .getOrCreate()

        // read from trait clustering_alg
        val data = loadData(spark, file_path)
        val clusters = kCenter(spark, data.collect.toList, k)
        printResults(clusters)
        spark.stop()

    }

    //This implementation uses the KMeans class from the Apache Spark MLlib library to perform clustering.
    //It takes a list of data points and the number of clusters (k) as input and returns a map of centroids to their respective clusters as output.
    def kCenter(spark: SparkSession,data: List[(Double, Double)], k: Int): Map[(Double, Double), List[(Double, Double)]] = {
        import spark.implicits._

        val df = data.toDF("x", "y")
        val assembler = new VectorAssembler().setInputCols(Array("x", "y")).setOutputCol("features")
        val dataset = assembler.transform(df)

        val kmeans = new KMeans().setK(k).setSeed(1L)
        val model = kmeans.fit(dataset)

        val predictions = model.transform(dataset)
        val clusters = predictions.groupBy("prediction").agg(avg($"x"), avg($"y")).collect()

        val clusterMap = Map.empty[(Double, Double), List[(Double, Double)]]
        for (cluster <- clusters) {
            val centroid = (cluster(1).asInstanceOf[Double], cluster(2).asInstanceOf[Double])
            val points = predictions.filter($"prediction" === cluster(0)).select("x", "y").collect()
            val pointsList = points.map(point => (point(0).asInstanceOf[Double], point(1).asInstanceOf[Double])).toList
            clusterMap += (centroid -> pointsList)
        }
        clusterMap
    }
}
