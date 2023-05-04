package parallel

import breeze.plot.{DomainFunction, Figure, plot}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import parallel.kmeans.closestCentroid

import scala.annotation.tailrec


object kmeans extends scala.clustering_alg {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Parallel-KMeans").master("local[*]").getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		println("\n\nParallel KMeans ")
		val data = loadData(spark)
		val start = System.nanoTime()
		val bestK = elbowMethod(data, 2, 10, 10)
		val end = System.nanoTime()
		println("\nTime: " + (end - start) / 1e9d + "s\n")
		println("Best K: " + bestK)
		spark.stop()
	}

	def kMeans1(data: RDD[(Double, Double)], centroids: RDD[(Double, Double)], maxIterations: Int): Array[(Double, Double)] = {
		var currentCentroids = centroids.collect()
		var K = centroids.count().toInt

		for (i <- 1 to maxIterations) {
			print("\rIteration: " + i)

			// Broadcast the current centroids to all workers
			val bcCentroids = data.context.broadcast(currentCentroids)

			// Assign each data point to the closest centroid
			val closestCentroids = data.map(point => (closestCentroid(point, bcCentroids.value), point))

			// Calculate the mean of all points assigned to each centroid and set it as the new centroid
			val newCentroids = closestCentroids
			  .groupByKey()
			  .mapValues(points => {
				  val (sumX, sumY, count) = points.foldLeft((0.0, 0.0, 0)) {
					  case ((accX, accY, accCount), (x, y)) => (accX + x, accY + y, accCount + 1)
				  }
				  (sumX / count, sumY / count)
			  })
			  .values
			  .collect()

			currentCentroids = newCentroids
		}

		currentCentroids
	}

	def kMeans2(data: RDD[(Double, Double)], centroids: RDD[(Double, Double)], maxIterations: Int): Array[(Double, Double)] = {
		var currentCentroids = centroids.collect().toList
		val K = centroids.count().toInt

		for (i <- 1 to maxIterations) {
			print("\rIteration: " + i)

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
			currentCentroids = newCentroids
		}
		currentCentroids.toArray
	}

	def kMeans3(data: RDD[(Double, Double)], centroids: RDD[(Double, Double)], maxIterations: Int): Array[(Double, Double)] = {
		var currentCentroids = centroids.collect().toList
		val K = centroids.count().toInt

		def kMeansIteration(data: RDD[(Double, Double)], centroids: List[(Double, Double)], iteration: Int): List[(Double, Double)] = {
			if (iteration >= maxIterations) {
				centroids
			} else {
				print("\rIteration: " + iteration)

				// Assign each data point to the closest centroid
				val closestCentroids = data.mapPartitions(iter => {
					val localCentroids = centroids
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
				  .collect()
				  .toList

				closestCentroids.unpersist()
				kMeansIteration(data, newCentroids, iteration + 1)
			}
		}

		kMeansIteration(data, currentCentroids, 1).toArray
	}





	def elbowMethod(data: RDD[(Double, Double)], minK: Int, maxK: Int, maxIterations: Int): Int = {
		val ks = Range(minK, maxK + 1)
		val wcss = ks.map(k => {
			println(s"\nK: $k")
			val centroids = initializeCentroids(k, data)
			val clusterCentroids = kMeans3(data, centroids, maxIterations)
			val squaredErrors = data.map(point => {
				val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				minDistance * minDistance
			})
			squaredErrors.sum
		})
		save_wcss("./src/resources/parallels/kmeans_elbow.csv", ks, wcss)
		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		ks(diff.indexOf(diff.max) + 1)
	}
}