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

	private def kMeans(data: RDD[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
		var currentCentroids = centroids
		val K = centroids.length
		var isConverged = false

		while (!isConverged) {
			// Assign each data point to the closest centroid
			val closestCentroids = data.mapPartitions(iter => {
				val localCentroids = currentCentroids
				iter.map(point => (closestCentroid(point, localCentroids), point))
			})
			  .persist()

			// Calculate the mean of all points assigned to each centroid and set it as the new centroid
			val newCentroids = closestCentroids
			  .aggregateByKey((0.0, 0.0, 0))(
				  (acc, point) => (acc._1 + point._1, acc._2 + point._2, acc._3 + 1),
				  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
			  )
			  .mapValues { case (sumX, sumY, count) => (sumX / count, sumY / count) }
			  .values
			  .coalesce(K, shuffle = true)
			  .collect()
			  .toList

			closestCentroids.unpersist()

			// Check if the centroids have converged
			isConverged = checkConvergence(currentCentroids, newCentroids)

			currentCentroids = newCentroids
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