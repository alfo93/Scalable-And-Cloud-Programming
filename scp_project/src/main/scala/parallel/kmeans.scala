/*package parallel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.ElbowMethod.euclideanDistance

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
		/*var currentCentroids = centroids
		var iteration = 0
		var clusters = Map.empty[(Double, Double), List[(Double, Double)]]
		while (iteration < maxIterations) {
			print("\rIteration: " + iteration)
			clusters = Map.empty.withDefaultValue(List.empty)
			val distances = data.cartesian(sc.parallelize(currentCentroids)).map(pair => (pair._1, euclideanDistance(pair._1, pair._2)))
			val closestCentroids = distances.reduceByKey((x, y) => math.min(x, y)).map(pair => (pair._1, currentCentroids(distances.filter(_._1 == pair._1).map(_._2).indexOf(pair._2))))
			val clustersRDD = closestCentroids.groupByKey().mapValues(_.toList)
			clusters = clustersRDD.collect().toMap
			currentCentroids = clusters.keys.toList.map(centroid => {
				val pointsInCluster = clusters(centroid)
				(
				  pointsInCluster.map(_._1).sum / pointsInCluster.length,
				  pointsInCluster.map(_._2).sum / pointsInCluster.length
				)
			})
			iteration += 1
		}
		currentCentroids.toArray
		*/
		1
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
}*/
