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

	private def kCenter(data: List[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
		var currentCentroids = centroids
		var iteration = 0
		var clusters = Map.empty[(Double, Double), List[(Double, Double)]]
		var isConverged = false
		val maxIterations = 100

		while (!isConverged && iteration < maxIterations) {
			clusters = Map.empty.withDefaultValue(List.empty)

			for (point <- data) {
				val distances = currentCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				val closestCentroid = currentCentroids(distances.indexOf(minDistance))

				// Update `clusters` with new point assigned to `closestCentroid`
				clusters += (closestCentroid -> (point :: clusters(closestCentroid)))
			}

			val newCentroids = clusters.keys.toList.map(centroid => {
				val pointsInCluster = clusters(centroid)

				// Compute the farthest point in the cluster from the centroid
				val farthestPoint = pointsInCluster.maxBy(p => euclideanDistance(p, centroid))

				// Set the new centroid as the farthest point
				farthestPoint
			})

			// Check if the centroids have converged
			isConverged = checkConvergence(data, currentCentroids, newCentroids)
		
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
