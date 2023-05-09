package scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.annotation.unused
import scala.collection.mutable
import scala.math.{pow, sqrt}
import scala.util.Random

trait clustering_alg {
	@unused
	private val random = new Random(42)
	private val file_path: String = "./src/resources/Umbria_xy.csv"
	private val results_by_k: mutable.Map[Int, Array[(Double,Double)]] = mutable.Map[Int, Array[(Double,Double)]]()

	def get_file_name: String = {
		file_path.split("/").last.split("\\.").head
	}

	def euclideanDistance(p1: (Double, Double), p2: (Double, Double)): Double = {
		sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2))
	}

	def loadData(spark: SparkSession): RDD[(Double, Double)] = {
		print("Loading data...")
		val res = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
		  .select("x", "y")
		  .rdd
		  .map(row => (row.getDouble(0), row.getDouble(1)))
		  .persist()
		print("OK\n")
		res
	}

	/*
	def initializeCentroids(k: Int, data: RDD[(Double, Double)]): RDD[(Double, Double)] = {
		val xCoords = data.map(_._1)
		val yCoords = data.map(_._2)
		val xRange = xCoords.max - xCoords.min
		val yRange = yCoords.max - yCoords.min
		val centroids = data.sparkContext.parallelize(
			for (_ <- 1 to k) yield {
				val x = xCoords.min() + (xRange * Random.nextDouble)
				val y = yCoords.min() + (yRange * Random.nextDouble)
				(x, y)
			}
		)
		centroids
	}*/

	def initializeCentroids(k: Int, data: List[(Double, Double)]): List[(Double, Double)] = {
		scala.util.Random.shuffle(data).take(k)
	}

	def initializeCentroids(k: Int, data: RDD[(Double, Double)]): List[(Double, Double)] = {
		// Randomly sample k points from the data
		data.takeSample(withReplacement = false, k, System.nanoTime().toInt).toList
	}


	// ClosestCentroid is a helper function that takes a data point and a list of centroids,
	// and returns the closest centroid to the data point based on Euclidean distance.

	// Closest centroid for List
	def closestCentroid(point: (Double, Double), centroids: List[(Double, Double)]): (Double, Double) = {
		centroids.map(centroid => (centroid, euclideanDistance(point, centroid)))
		  .reduce((a, b) => if (a._2 < b._2) a else b)
		  ._1
	}

	// Closest centroid for Array
	def closestCentroid(point: (Double, Double), centroids: Array[(Double, Double)]): (Double, Double) = {
		centroids.map(centroid => (centroid, euclideanDistance(point, centroid)))
		  .reduce((a, b) => if (a._2 < b._2) a else b)
		  ._1
	}

	// Closest centroid for RDD
	def closestCentroid(point: (Double, Double), centroids: RDD[(Double, Double)]): (Double, Double) = {
		centroids.map(centroid => (centroid, euclideanDistance(point, centroid)))
		  .reduce((a, b) => if (a._2 < b._2) a else b)
		  ._1
	}

	def save_cluster(k:Int, clusters: Array[(Double, Double)]): Unit = {
		results_by_k += (k -> clusters)
	}

	def save_cluster_csv(data: List[(Double, Double)], path: String): Unit = {
		print("\nSaving clusters...")
		results_by_k.foreach(x => {
			val k = x._1
			val clusters = x._2
			val clusters_data = data.map(point => {
				val centroid = closestCentroid(point, clusters)
				point._1.toString + "," + point._2.toString + "," + centroid._1.toString + "," + centroid._2.toString
			})
			val clusters_file = new java.io.PrintWriter(new java.io.File(path + k.toString + ".csv"))
			clusters_data.foreach(clusters_file.println)
			clusters_file.close()

		})
		print("OK\n")
	}

	def save_wcss(filename:String, ks: Range, wcss: Seq[Double]): Unit = {
		print("Saving WCSS...")
		val wcss_data = ks.zip(wcss).map(x => x._1.toString + "," + x._2.toString)
		val wcss_file = new java.io.PrintWriter(new java.io.File(filename))
		wcss_data.foreach(wcss_file.println)
		wcss_file.close()
		print("OK\n")
	}

	def save_run(filename: String, minK: Int, maxK: Int, bestK: Int, time: Double): Unit = {
		print("Saving run...")
		val run_data = minK.toString + "," + maxK.toString + ","  + bestK.toString + "," + time.toString
		val run_file = new java.io.PrintWriter(new java.io.File(filename))
		run_file.println(run_data)
		run_file.close()
		print("OK\n")
	}

	/* Da testare
	def clustering_function(data: RDD[(Double, Double)], centroids: RDD[(Double, Double)]): Array[(Double, Double)]
	def clustering_function(data: List[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)]

	def elbowMethod(path: String, data: RDD[(Double, Double)], minK: Int, maxK: Int): Unit = {
		val ks = Range(minK, maxK + 1)

		val start = System.nanoTime()
		val wcss = ks.map(k => {
			println(s"\nK: $k")
			val centroids = initializeCentroids(k, data)
			val clusterCentroids = clustering_function(data, centroids)
			save_cluster(k, clusterCentroids)
			val squaredErrors = data.map(point => {
				val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				minDistance * minDistance
			})
			squaredErrors.sum
		})
		val end = System.nanoTime()
		print("\n\nTime: " + (end - start) / 1e9d + "s")

		save_cluster_csv(data.collect().toList, path)
		save_wcss(path + "_elbow.csv", ks, wcss)

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		save_run(path + "_run.csv", minK, maxK, bestK, end - start)

		bestK
	}
	*/

}


