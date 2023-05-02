package parallel
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.sql.SparkSession

object kmeans extends parallel.clustering_alg {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Sequential-KMeans").master("local[*]").getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		println("\n\nParallel KMeans")
		val data = loadData(spark)
		val start = System.nanoTime()
		val bestK = elbowMethod(data, 2, 10, 50)
		val end = System.nanoTime()
		println("\nTime: " + (end - start) / 1e9d + "s\n")
		println("Best K: " + bestK)
		spark.stop()
		bestK
	}

	//closestCentroid is a helper function that takes a data point and a list of centroids,
	// and returns the closest centroid to the data point based on Euclidean distance.
	def closestCentroid(point: (Double, Double), centroids: RDD[(Double, Double)]): (Double, Double) = {
		centroids.map(centroid => (centroid, euclideanDistance(point, centroid)))
		  .reduce((a, b) => if (a._2 < b._2) a else b)
		  ._1
	}


	def kMeans(data: RDD[(Double, Double)], centroids: RDD[(Double, Double)], maxIterations: Int): Array[(Double, Double)] = {
		var currentCentroids = centroids

		for (i <- 1 to maxIterations) {
			// Assign each data point to the closest centroid
			val closestCentroids = data.map(point => (closestCentroid(point, currentCentroids), point))

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
		}
		currentCentroids.collect()
	}


	def elbowMethod(data: RDD[(Double, Double)], minK: Int, maxK: Int, maxIterations: Int): Int = {
		val ks = Range(minK, maxK + 1)
		val wcss = ks.map(k => {
			println(s"\nK: $k")
			val centroids = initializeCentroids(k, data)
			val clusterCentroids = kMeans(data, centroids, maxIterations)
			val squaredErrors = data.map(point => {
				val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				minDistance * minDistance
			})
			squaredErrors.sum
		})

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		ks(diff.indexOf(diff.max) + 1)
	}
}