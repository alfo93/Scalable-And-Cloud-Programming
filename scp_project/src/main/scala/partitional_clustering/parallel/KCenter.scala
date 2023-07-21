package partitional_clustering.parallel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import partitional_clustering.PartitionalClustering

object KCenter extends PartitionalClustering {
	override var filePath: String = ""

	def main(args: Array[String]): (Int, Double) = {
		// Check if the file path is provided
		if (args.length != 1) {
			println("Usage: KCenter <file path>")
			System.exit(1)
		}

		filePath = args(0)
		// Initialize Spark
		val spark = SparkSession.builder()
		  .appName("Parallel-KCenter")
		  .master("local[*]")
		  .config("spark.executor.cores", "2")
		  .config("spark.driver.maxResultSize", "2g")
		  .getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		println("\nParallel KCenter ")
		val data = loadData(spark)
		val (bestK: Int, time: Double) = elbowMethod(data, kMin, kMax)
		println(s"\nBest K: $bestK")
		spark.stop()
		(bestK, time)
	}

	/**
	 * Performs the Farthest-First Traversal algorithm to select k initial centroids for the k-means clustering algorithm.
	 * The algorithm starts with an empty list of centroids and iteratively adds k centroids by selecting the farthest point
	 * from the nearest existing centroid in the data.
	 *
	 * @param data An RDD of data points, each represented as a tuple of (x, y) coordinates.
	 * @param k    The number of centroids to select and the number of clusters to create.
	 * @return An array containing k initial centroids selected by the Farthest-First Traversal algorithm.
	 */
	private def farthestFirstTraversal(data: RDD[(Double, Double)], k: Int): Array[(Double, Double)] = {
		var centers = List.empty[(Double, Double)]

		// Select the first center at random
		centers = data.takeSample(withReplacement = false, num = 1).toList

		// Broadcast the centers to all worker nodes
		var centersBroadcast = data.sparkContext.broadcast(centers)

		// Loop until we have found all the centers
		while (centers.length < k) {
			// Find the farthest point from the nearest center for each point
			val farthestPoint = data.map(point => (point, centersBroadcast.value.minBy(center => euclideanDistance(center, point))))
			  .reduce((a, b) => if (euclideanDistance(a._1, a._2) > euclideanDistance(b._1, b._2)) a else b)
			  ._1

			// Add the farthest point to the centers list
			centers = farthestPoint :: centers

			// Update the broadcast variable with the new centers list
			centersBroadcast.unpersist()
			centersBroadcast = data.sparkContext.broadcast(centers)
		}

		// Convert the centers list to an array and return it
		centers.reverse.toArray
	}

	def elbowMethod(data: RDD[(Double, Double)], minK: Int, maxK: Int): (Int, Double) = {
		val ks = Range(minK, maxK + 1)
		val start = System.nanoTime()
		val wcss = ks.map(k => {
			print(s"K: $k \r")
			val clusterCentroids = farthestFirstTraversal(data, k)
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
		print("Time: " + time)

		saveClusterCsv(data.collect().toList, "./src/resources/parallels/kcenter_")
		saveWcss("./src/resources/parallels/kcenter_elbow.csv", ks, wcss)

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		saveRun("./src/resources/sequential/kcenter_run.csv", minK, maxK, bestK, time)
		(bestK, time)
	}


}