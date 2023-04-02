import org.apache.spark.sql.SparkSession
import scala.io.Source
import scala.collection.mutable.Map

object Kmeans extends clustering_alg {
	val k: Int = 10
	val maxIterations: Int = 100

	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder
		  .appName("KMeansSpark")
		  .config("spark.master", "local")
		  .getOrCreate()

		val data = loadData(spark, file_path)
		val centroids = initializeCentroids(k, data)
		val clusters = kMeans(data.collect.toList, centroids, maxIterations)
		printResults(clusters)
		spark.stop()
	}

	def kMeans(data: List[(Double, Double)], centroids: List[(Double, Double)], maxIterations: Int): Map[(Double, Double), List[(Double, Double)]] = {
		var currentCentroids = centroids
		var iteration = 0
		var clusters = Map.empty[(Double, Double), List[(Double, Double)]]
		while (iteration < maxIterations) {
			clusters = Map.empty.withDefaultValue(List.empty)
			for (point <- data) {
				val distances = currentCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				val closestCentroid = currentCentroids(distances.indexOf(minDistance))
				clusters += (closestCentroid -> (point :: clusters(closestCentroid)))
			}
			currentCentroids = clusters.keys.toList.map(centroid => {
				val pointsInCluster = clusters(centroid)
				(
				  pointsInCluster.map(_._1).sum / pointsInCluster.length,
				  pointsInCluster.map(_._2).sum / pointsInCluster.length
				)
			})
			iteration += 1
		}
		clusters
	}
}