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

	private def getMaxDistance(oldCentroids: RDD[(Double, Double)], newCentroids: RDD[(Double, Double)]): Double = {
		val distances = oldCentroids.zipPartitions(newCentroids) { (iter1, iter2) =>
			iter1.zip(iter2).map { case ((x1, y1), (x2, y2)) =>
				euclideanDistance((x1, y1), (x2, y2))
			}
		}
		distances.max()
	}

	private def kCenter(data: RDD[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
		var currentCentroids = data.sparkContext.parallelize(centroids)
		val K = centroids.length
		var maxDistanceChange = Double.MaxValue

		val threshold = 1e-6
		var iteration = 0

		while (maxDistanceChange > threshold && iteration < 200) {
			// Broadcast the current centroids to all worker nodes
			val broadcastCentroids = data.sparkContext.broadcast(currentCentroids.collect())

			// Assign each data point to the closest centroid
			val closestCentroids = data.mapPartitions(iter => {
				val localCentroids = broadcastCentroids.value
				iter.map(point => (closestCentroid(point, localCentroids), point))
			}).persist()

			// Calculate the mean of all points assigned to each centroid and set it as the new centroid
			val newCentroids = closestCentroids
			  .aggregateByKey((0.0, 0.0, 0))(
				  (acc, point) => (acc._1 + point._1, acc._2 + point._2, acc._3 + 1),
				  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
			  )
			  .mapValues { case (sumX, sumY, count) => (sumX / count, sumY / count) }
			  .values
			  .repartition(currentCentroids.getNumPartitions) // Repartition to match the number of partitions of currentCentroids

			closestCentroids.unpersist()

			// Calculate the maximum distance change between the old and new centroids
			maxDistanceChange = getMaxDistance(currentCentroids, newCentroids)
			currentCentroids = newCentroids
			iteration += 1
		}

		currentCentroids.collect()
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