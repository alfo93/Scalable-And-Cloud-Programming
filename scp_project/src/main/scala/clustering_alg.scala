import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.ClusteringEvaluator
trait clustering_alg {
	val file_path: String = "./src/resources/roma_xy.csv"

	// Euclidean distance between two points
	def euclideanDistance(p1: (Double, Double), p2: (Double, Double)): Double = {
		val x = p1._1 - p2._1
		val y = p1._2 - p2._2
		math.sqrt(x * x + y * y)
	}

	def loadData(spark: SparkSession, file_path: String) = {
		spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
		  .select("x", "y")
		  .rdd
		  .map(row => (row.getDouble(0), row.getDouble(1)))
		  .cache()
	}

	def initializeCentroids(k: Int, data: org.apache.spark.rdd.RDD[(Double, Double)]): List[(Double, Double)] = {
		val xCoords = data.map(_._1)
		val yCoords = data.map(_._2)
		val xRange = xCoords.max - xCoords.min
		val yRange = yCoords.max - yCoords.min
		val centroids = for (i <- 1 to k) yield {
			val x = xCoords.min + (xRange * scala.util.Random.nextDouble)
			val y = yCoords.min + (yRange * scala.util.Random.nextDouble)
			(x, y)
		}
		centroids.toList
	}

	def printResults(clusters: Map[(Double, Double), List[(Double, Double)]]): Unit = {
		println("\n\nCluster:")
		clusters.foreach(cluster => {
			val centroid = cluster._1
			val points = cluster._2
			println(s"Centroid: $centroid, Points: ${points.length}")
		})
	}

}
