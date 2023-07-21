package partitional_clustering.parallel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import partitional_clustering.PartitionalClustering

object KMeans extends PartitionalClustering {
	override var filePath: String = ""

	def main(args: Array[String]): (Int, Double) = {
		// Check if the file path is provided
		if (args.length != 1) {
			println("Usage: KMeans <file path>")
			System.exit(1)
		}
		filePath = args(0)

		// Initialize Spark
		val spark = SparkSession.builder()
		  .appName("Parallel-KMeans")
		  .master("local[*]")
		  .getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		println("\nParallel KMeans ")
		val data = loadData(spark)
		val (bestK: Int, time: Double) = elbowMethod(data, kMin, kMax)
		println("\nBest K: " + bestK)
		spark.stop()
		(bestK, time)
	}

	/**
	 * Performs the k-means clustering algorithm on a given set of data points represented as an RDD of (x, y) coordinates.
	 * The algorithm aims to partition the data into K clusters and determine the optimal positions of the cluster centers
	 * (centroids) based on the input `centroids` list. The function iteratively updates the centroids until convergence,
	 * where the centroids stop changing significantly.
	 *
	 * @param data      An RDD of data points, each represented as a tuple of (x, y) coordinates.
	 * @param centroids A list of initial centroids, each represented as a tuple of (x, y) coordinates, used as starting points.
	 * @return An array containing the final set of centroids after the algorithm converges.
	 */
	private def kMeans(data: RDD[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
		var currentCentroids = centroids
		var isConverged = false
		var iteration = 0
		val maxIterations = 100

		while (!isConverged && iteration < maxIterations) {
			// Assign each data point to the closest centroid
			val closestCentroids = data.mapPartitions(iter => {
				val localCentroids = currentCentroids

				// For each data point in this partition, find the closest centroid and emit (centroid, point) pairs
				iter.map(point => (closestCentroid(point, localCentroids), point))
			}).persist() // Cache the closestCentroids RDD to speed up computations in subsequent iterations

			val centroidSumCountRDD = closestCentroids
			  .map { case (centroid, point) =>
				  val (x, y) = point
				  (centroid, (x, y, 1))
			  }
			  .reduceByKey { case ((x1, y1, count1), (x2, y2, count2)) =>
				  (x1 + x2, y1 + y2, count1 + count2)
			  }

			// Calculate the new centroid by dividing the sum of (x, y) coordinates by the count of data points
			val newCentroidsRDD = centroidSumCountRDD
			  .map { case (centroid, (sumX, sumY, count)) =>
				  (centroid, (sumX / count, sumY / count))
			  }

			// Extract the new centroids as a list on the driver
			val newCentroidsList = newCentroidsRDD.values.collect().toList

			// Unpersist the cached closestCentroids RDD since we don't need it anymore
			closestCentroids.unpersist()

			// Check if the centroids have converged
			isConverged = checkConvergence(currentCentroids, newCentroidsList)

			currentCentroids = newCentroidsList
			iteration += 1
		}

		currentCentroids.toArray
	}

	def elbowMethod(data: RDD[(Double, Double)], minK: Int, maxK: Int): (Int, Double) = {
		val ks = Range(minK, maxK + 1)

		val start = System.nanoTime()
		val wcss = ks.map(k => {
			print(s"K: $k \r")
			val centroids = initializeCentroids(k, data)
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

		saveClusterCsv(data.collect().toList, "./src/resources/parallels/kmeans_")
		saveWcss("./src/resources/parallels/kmeans_elbow.csv", ks, wcss)

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		saveRun("./src/resources/parallels/kmeans_run.csv", minK, maxK, bestK, time)

		(bestK, time)
	}
}