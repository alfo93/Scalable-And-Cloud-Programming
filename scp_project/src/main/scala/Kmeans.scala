import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.collection.mutable.Map

object Kmeans extends clustering_alg {
	val k: Int = 25
	val maxIterations: Int = 100

	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder
		  .appName("KMeans")
		  .config("spark.master", "local")
		  .getOrCreate()

		val data = loadData(spark, file_path)
		val centroids = initializeCentroids(k, data)
		val clusters = kMeans_mr(data, centroids, maxIterations)
		printResults(clusters)
		spark.stop()
	}

	def kMeans(data: RDD[(Double, Double)], centroids: List[(Double, Double)], maxIterations: Int): Array[(Double, Double)] = {
		var currentCentroids = centroids
		var iteration = 0
		var clusters = Map.empty[(Double, Double), List[(Double, Double)]]
		while (iteration < maxIterations) {
			clusters = Map.empty.withDefaultValue(List.empty)
			for (point <- data.collect()) {
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
		currentCentroids.toArray
	}

	def kMeans_mr(data: RDD[(Double, Double)], centroids: List[(Double, Double)], maxIterations: Int): Array[(Double, Double)] = {
		var currentCentroids = centroids
		var iteration = 0
		while (iteration < maxIterations) {
			val centroidMap = data.flatMap { case (x, y) =>
				val distances = currentCentroids.map(c => (c, euclideanDistance((x, y), c)))
				val minDist = distances.minBy(_._2)
				Seq(minDist._1 -> Seq((x, y)))
			}.reduceByKey(_ ++ _).collectAsMap()

			currentCentroids = centroidMap.map { case (centroid, points) =>
				val (sumX, sumY) = points.foldLeft((0.0, 0.0)) { case ((accX, accY), (x, y)) =>
					(accX + x, accY + y)
				}
				(sumX / points.length, sumY / points.length)
			}.toList.sortBy(_._1)

			iteration += 1
		}
		currentCentroids.toArray
	}


}