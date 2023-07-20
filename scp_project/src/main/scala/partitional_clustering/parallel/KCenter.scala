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

	def kCenter(data: RDD[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
		var currentCentroids = centroids
		var isConverged = false
		var iteration = 0

		def computeNewCentroid(centroid: (Double, Double), pointsInCluster: Iterable[(Double, Double)]): (Double, Double) = {
			if (pointsInCluster.isEmpty) centroid
			else pointsInCluster.maxBy(p => euclideanDistance(p, centroid))
		}

		while (!isConverged && iteration < 100) {
			val clusters = data
			  .map(point => (closestCentroid(point, currentCentroids), point))
			  .groupByKey()

			val newCentroids = clusters.mapValues(points => computeNewCentroid(points.head, points)).collectAsMap()

			// Check if the centroids have converged
			isConverged = checkConvergence(data, currentCentroids, newCentroids.values.toList)
			iteration += 1
			currentCentroids = newCentroids.values.toList
		}

		currentCentroids.toArray
	}


	def elbowMethod(data: RDD[(Double, Double)], minK: Int, maxK: Int): (Int, Double) = {
		val ks = Range(minK, maxK + 1)
		val start = System.nanoTime()
		val wcss = ks.map(k => {
			print(s"K: $k \r")
			val centroids = initializeCentroids(k, data)
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
		print("Time: " + time)

		saveClusterCsv(data.collect().toList, "./src/resources/parallels/kcenter_")
		saveWcss("./src/resources/parallels/kcenter_elbow.csv", ks, wcss)

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		saveRun("./src/resources/sequential/kcenter_run.csv", minK, maxK, bestK, time)
		(bestK, time)
	}


}