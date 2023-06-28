package partitional_clustering.sequential

import org.apache.spark.sql.SparkSession
import partitional_clustering.PartitionalClustering

object KMeans extends PartitionalClustering {

	def main(): (Int, Double) = {
		val spark = SparkSession.builder().appName("Sequential-KMeans").master("local[*]").getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		println("\nSequential KMeans ")
		val data = loadData(spark).collect().toList
		val (bestK:Int, time:Double)  = elbowMethod(data, kMin, kMax)
		println("\nBest K: " + bestK)
		spark.stop()
		(bestK, time)
	}

	private def kMeans(data: List[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
		var currentCentroids = centroids
		var iteration = 0
		var clusters = Map.empty[(Double, Double), List[(Double, Double)]]
		var isConverged = false

		while (!isConverged) {
			print("\rIteration: " + iteration)
			clusters = Map.empty.withDefaultValue(List.empty)

			for (point <- data) {
				val distances = currentCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				val closestCentroid = currentCentroids(distances.indexOf(minDistance))
				clusters += (closestCentroid -> (point :: clusters(closestCentroid)))
			}

			val newCentroids = clusters.keys.toList.map(centroid => {
				val pointsInCluster = clusters(centroid)
				(
				  pointsInCluster.map(_._1).sum / pointsInCluster.length,
				  pointsInCluster.map(_._2).sum / pointsInCluster.length
				)
			})

			// Check if the centroids have converged
			isConverged = checkConvergence(currentCentroids, newCentroids)

			currentCentroids = newCentroids
			iteration += 1
		}

		currentCentroids.toArray
	}

	def elbowMethod(data: List[(Double, Double)], minK: Int, maxK: Int): (Int, Double) = {
		val ks = Range(minK, maxK + 1)

		val start = System.nanoTime()
		val wcss = ks.map(k => {
			print(s"K: $k\r")
			val centroids = scala.util.Random.shuffle(data).take(k)
			val clusterCentroids = kMeans(data, centroids)
			saveCluster(k, clusterCentroids)
			val squaredErrors = data.map(point => {
				val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				minDistance * minDistance
			})
			squaredErrors.sum
		})
		val end = System.nanoTime()
		val time = (end - start) / 1e9d
		print("Time: " + time + "\n")

		saveClusterCsv(data, "./src/resources/sequential/kmeans_")
		saveWcss("./src/resources/sequential/kmeans_elbow.csv", ks, wcss)

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		saveRun("./src/resources/sequential/kmeans_run.csv", minK, maxK, bestK, time)

		(bestK, time)
	}
}
