package partitional_clustering.sequential

import org.apache.spark.sql.SparkSession
import partitional_clustering.PartitionalClustering
import partitional_clustering.sequential.KMeans.checkConvergence

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

	/**
	 * Performs the Farthest-First Traversal algorithm to select k initial centroids for the k-means clustering algorithm.
	 * The algorithm starts with an empty array of centroids and iteratively adds k centroids by selecting the farthest
	 * point from the nearest existing centroid in the data.
	 *
	 * @param data A list of data points, each represented as a tuple of (x, y) coordinates.
	 * @param k    The number of centroids to select and the number of clusters to create.
	 * @return An array containing k initial centroids selected by the Farthest-First Traversal algorithm.
	 */
	private def farthestFirstTraversal(data: List[(Double, Double)], k: Int): Array[(Double, Double)] = {
		val clusters = new Array[(Double, Double)](k)

		// Initialize the clusters with random point from the data
		clusters(0) = data(scala.util.Random.nextInt(data.length))

		// Function to find the farthest point from the nearest cluster for each point
		def findFarthestPoint(points: List[(Double, Double)], centers: Array[(Double, Double)]): (Double, Double) = {
			points.maxBy { point =>
				centers.map(euclideanDistance(point, _)).min
			}
		}

		// Apply Farthest First Traversal
		for (i <- 1 until k) {
			val farthestPoint = findFarthestPoint(data, clusters.take(i))
			clusters(i) = farthestPoint
		}

		clusters
	}

	def elbowMethod(data: List[(Double, Double)], minK: Int, maxK: Int): (Int, Double) = {
		val ks = Range(minK, maxK + 1)
		val start = System.nanoTime()
		val wcss = ks.map(k => {
			print(s"K: $k\r")
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
		print("Time: " + time + "\n")

		saveClusterCsv(data, "./src/resources/sequential/kcenter_")
		saveWcss("./src/resources/sequential/kcenter_elbow.csv", ks, wcss)

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		saveRun("./src/resources/sequential/kcenter_run.csv", minK, maxK, bestK, time)
		(bestK, time)
	}
}
