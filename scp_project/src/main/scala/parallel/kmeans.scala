package parallel

import breeze.linalg.euclideanDistance
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object kmeans extends parallel.clustering_alg {
	var maxIterations: Int = 100

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Sequential-KMeans").master("local[*]").getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		val data = loadData(spark).collect().toList
		val start = System.nanoTime()
		val bestK = elbowMethod(data, 2, 100, 10)
		val end = System.nanoTime()
		println("\nTime: " + (end - start) / 1e9d + "s\n")
		println(s"Best K: $bestK")
		spark.stop()
		bestK
	}

	def kMeans(data: RDD[(Double, Double)], centroids: List[(Double, Double)], maxIterations: Int): Array[(Double, Double)] = {

	}

	def elbowMethod(data: List[(Double, Double)], minK: Int, maxK: Int, maxIterations: Int): Unit = {
		val ks = Range(minK, maxK + 1)
		val wcss = ks.map(k => {
			println(s"\nK: $k")
			val centroids = scala.util.Random.shuffle(data).take(k)
			val clusterCentroids = kMeans(data, centroids, maxIterations)
			val squaredErrors = data.map(point => {
				val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				minDistance * minDistance
			})
			squaredErrors.sum
		})

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		bestK
	}
}