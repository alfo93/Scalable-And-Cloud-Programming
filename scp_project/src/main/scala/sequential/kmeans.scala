package sequential

import org.apache.spark.sql.SparkSession

object kmeans extends scala.clustering_alg {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Sequential-KMeans").master("local[*]").getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		println("\n\nSequential KMeans ")
		val data = loadData(spark).collect().toList
		val bestK = elbowMethod(data, 2, 10, 10)
		println("Best K: " + bestK)
		spark.stop()
	}

	private def kMeans(data: List[(Double, Double)], centroids: List[(Double, Double)], maxIterations: Int): Array[(Double, Double)] = {
		var currentCentroids = centroids
		var iteration = 0
		var clusters = Map.empty[(Double, Double), List[(Double, Double)]]
		while (iteration < maxIterations) {
			print("\rIteration: " + iteration)
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
		currentCentroids.toArray
	}


	def elbowMethod(data: List[(Double, Double)], minK: Int, maxK: Int, maxIterations: Int): Int = {
		val ks = Range(minK, maxK + 1)

		val start = System.nanoTime()
		val wcss = ks.map(k => {
			println(s"\nK: $k")
			val centroids = scala.util.Random.shuffle(data).take(k)
			val clusterCentroids = kMeans(data, centroids, maxIterations)
			save_cluster(k, clusterCentroids)
			val squaredErrors = data.map(point => {
				val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				minDistance * minDistance
			})
			squaredErrors.sum
		})
		val end = System.nanoTime()
		val time = end - start
		print("\n\nTime: " + (end - start) / 1e9d + "s")

		save_cluster_csv(data, "./src/resources/sequential/kmeans_")
		save_wcss("./src/resources/sequential/kmeans_elbow.csv", ks, wcss)
		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		save_run("./src/resources/sequential/kmeans_run.csv", minK, maxK, maxIterations, bestK, time)

		bestK
	}
}
