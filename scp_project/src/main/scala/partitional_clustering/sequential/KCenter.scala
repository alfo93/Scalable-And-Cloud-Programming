package partitional_clustering.sequential

import org.apache.spark.sql.SparkSession
import partitional_clustering.PartitionalClustering

object KCenter extends PartitionalClustering {
	override var filePath: String = ""

	def main(args: Array[String]): (Int, Double) = {
		// Check if the file path was provided
		if (args.length != 1) {
			println("Usage: KCenter <file path>")
			System.exit(1)
		}
		filePath = args(0)

		// Initialize Spark
		println("\nSequential KCenter")
		val spark = SparkSession.builder()
		  .appName("Sequential-KCenter")
		  .master("local[*]")
		  .config("spark.driver.maxResultSize", "2g")
		  .getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		println("\nSequential KCenter")
		val data = loadData(spark).collect().toList
		val (bestK: Int, time: Double) = elbowMethod(data, kMin, kMax)
		println("\nBest K: " + bestK)
		spark.stop()
		(bestK, time)
	}

	private def kCenter(data: List[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
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

	def elbowMethod(data: List[(Double, Double)], minK: Int, maxK: Int): (Int, Double) = {
		val ks = Range(minK, maxK + 1)
		val start = System.nanoTime()
		val wcss = ks.map(k => {
			print(s"K: $k\r")
			val centroids = scala.util.Random.shuffle(data).take(k)
			val clusterCentroids = kCenter(data, centroids)
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

		saveClusterCsv(data, "./src/resources/sequential/kcenter_")
		saveWcss("./src/resources/sequential/kcenter_elbow.csv", ks, wcss)

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		saveRun("./src/resources/sequential/kcenter_run.csv", minK, maxK, bestK, time)
		(bestK, time)
	}
}
