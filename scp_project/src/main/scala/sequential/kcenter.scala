package sequential

import org.apache.spark.sql.SparkSession
import scala.util.Random

object kcenter extends scala.clustering_alg {
	def main(args: Array[String]): Unit = {
		val random = new Random(42)
		println("Sequential KCenter")
		val spark = SparkSession.builder().appName("Sequential-KCenter").master("local[*]").getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		val data = loadData(spark).collect().toList
		val start = System.nanoTime()
		val bestK = elbowMethod(data, 2, 100)
		val end = System.nanoTime()
		println("Time: " + (end - start) / 1e9d + "s\n")
		println("Best K: " + bestK)
		spark.stop()
		bestK
	}

	def kCenter(data: List[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
		val clusters = new Array[(Double, Double)](centroids.length)

		// Initialize the clusters with the centroids
		for (i <- clusters.indices) {
			clusters(i) = centroids(i)
		}

		// Find the farthest point from the nearest cluster for each point
		for ((x, y) <- data) {
			var minDistance = Double.PositiveInfinity
			var nearestCluster = -1
			for (i <- clusters.indices) {
				val distance = euclideanDistance((x, y), clusters(i))
				if (distance < minDistance) {
					minDistance = distance
					nearestCluster = i
				}
			}
			if (minDistance > euclideanDistance((x, y), clusters(nearestCluster))) {
				clusters(nearestCluster) = (x, y)
			}
		}

		clusters
	}

	def elbowMethod(data: List[(Double, Double)], minK: Int, maxK: Int): Int = {
		val ks = Range(minK, maxK + 1)
		val wcss = ks.map(k => {
			println(s"K: $k")
			val centroids = scala.util.Random.shuffle(data).take(k)
			val clusterCentroids = kCenter(data, centroids)
			val squaredErrors = data.map(point => {
				val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				minDistance * minDistance
			})
			squaredErrors.sum
		})

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		ks(diff.indexOf(diff.max) + 1)

	}
}
