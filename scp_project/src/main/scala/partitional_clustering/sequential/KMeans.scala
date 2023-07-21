package partitional_clustering.sequential
import org.apache.spark.sql.SparkSession
import partitional_clustering.PartitionalClustering

object KMeans extends PartitionalClustering {
	override var filePath: String = ""

	def main(args: Array[String]): (Int, Double) = {
		// Check if the file path was provided
		if (args.length != 1) {
			println("Usage: KMeans <file path>")
			System.exit(1)
		}
		filePath = args(0)

		// Initialize Spark
		val spark = SparkSession.builder()
		  .appName("Sequential-KMeans")
		  .master("local[*]")
		  .config("spark.driver.maxResultSize", "2g")
		  .getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		println("\nSequential KMeans ")
		val data = loadData(spark).collect().toList
		val (bestK:Int, time:Double)  = elbowMethod(data, kMin, kMax)
		print("Best K: " + bestK)
		spark.stop()
		(bestK, time)
	}

	/**
	 * Performs the k-means clustering algorithm on a list of data points represented as tuples of (x, y) coordinates.
	 * The algorithm aims to partition the data into K clusters and determine the optimal positions of the cluster centers
	 * (centroids) based on the input `centroids` list. The function iteratively updates the centroids until convergence,
	 * where the centroids stop changing significantly.
	 *
	 * @param data      A list of data points, each represented as a tuple of (x, y) coordinates.
	 * @param centroids A list of initial centroids, each represented as a tuple of (x, y) coordinates, used as starting points.
	 * @return An array containing the final set of centroids after the algorithm converges.
	 */
	private def kMeans(data: List[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
		// Initialize variables
		var currentCentroids = centroids
		var iteration = 0
		val maxIterations = 200
		var clusters = Map.empty[(Double, Double), List[(Double, Double)]]
		var isConverged = false


		while (!isConverged && iteration < maxIterations) {
			// Reset clusters for each iteration
			clusters = Map.empty.withDefaultValue(List.empty)
			// Assign each data point to the closest centroid
			for (point <- data) {
				// Calculate the distance between the point and each centroid
				val distances = currentCentroids.map(centroid => euclideanDistance(point, centroid))
				// Find the minimum distance, which corresponds to the closest centroid
				val minDistance = distances.min
				// Get the closest centroid based on the minimum distance
				val closestCentroid = currentCentroids(distances.indexOf(minDistance))
				// Append the point to the cluster of the closest centroid
				clusters += (closestCentroid -> (point :: clusters(closestCentroid)))
			}

			// Calculate new centroids based on the average of the points in each cluster
			val newCentroids = clusters.keys.toList.map(centroid => {
				val pointsInCluster = clusters(centroid)
				(
				  pointsInCluster.map(_._1).sum / pointsInCluster.length,
				  pointsInCluster.map(_._2).sum / pointsInCluster.length
				)
			})

			// Check if the centroids have converged
			isConverged = checkConvergence(currentCentroids, newCentroids)

			// Update the centroids and iteration count for the next iteration
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
