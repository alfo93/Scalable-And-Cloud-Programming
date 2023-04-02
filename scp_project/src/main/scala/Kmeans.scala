import org.apache.spark.sql.SparkSession
import scala.io.Source

object Kmeans extends clustering_alg {
	val k: Int = 3
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

	def printResults(clusters: Map[(Double, Double), List[(Double, Double)]]): Unit = {
		println("\n\nCluster:")
		clusters.foreach(cluster => {
			val centroid = cluster._1
			val points = cluster._2
			println(s"Centroid: $centroid, Points: ${points.length}")
		})
	}
}