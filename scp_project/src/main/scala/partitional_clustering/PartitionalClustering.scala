package partitional_clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.math.{pow, sqrt}

trait PartitionalClustering {
	var filePath: String
	private val resultsByK: mutable.Map[Int, Array[(Double,Double)]] = mutable.Map[Int, Array[(Double,Double)]]()
	private val saveResults: Boolean = false
	val kMin = 5
	val kMax = 20

	def main(args: Array[String]): (Int, Double)
	
	def getAlgorithmName: String = this.getClass.getSimpleName

	// Euclidean distance between two points
	def euclideanDistance(p1: (Double, Double), p2: (Double, Double)): Double = sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2))

	// Load RDD from CSV file
	def loadData(spark: SparkSession): RDD[(Double, Double)] = {
		print("Loading data...")
		val res = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePath)
		  .select("x", "y")
		  .rdd
		  .map(row => (row.getDouble(0), row.getDouble(1)))
		  .persist()
		print("OK\n")
		res
	}

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

	// Function used to check convergence in fixed-point iteration
	def checkConvergence(oldCentroids: List[(Double, Double)], newCentroids: List[(Double, Double)]): Boolean = {
		val epsilon = 1e-6 // Define a small threshold for convergence

		// Check if the distance between old and new centroids is below the threshold for all centroids
		oldCentroids.zip(newCentroids).forall {
			case ((oldX, oldY), (newX, newY)) =>
				Math.sqrt(Math.pow(oldX - newX, 2) + Math.pow(oldY - newY, 2)) <= epsilon
		}
	}

	def saveCluster(k: Int, clusters: Array[(Double, Double)]): Unit = resultsByK += (k -> clusters)

	def saveClusterCsv(data: List[(Double, Double)], path: String): Unit = {
		if (!saveResults) return

		print("\nSaving clusters...")
		resultsByK.foreach(x => {
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

	def saveWcss(filename:String, ks: Range, wcss: Seq[Double]): Unit = {
		if (!saveResults) return

		print("Saving WCSS...")
		val wcss_data = ks.zip(wcss).map(x => x._1.toString + "," + x._2.toString)
		val wcss_file = new java.io.PrintWriter(new java.io.File(filename))
		wcss_data.foreach(wcss_file.println)
		wcss_file.close()
		print("OK\n")
	}

	def saveRun(filename: String, minK: Int, maxK: Int, bestK: Int, time: Double): Unit = {
		if (!saveResults) return

		print("Saving run...")
		val run_data = minK.toString + "," + maxK.toString + ","  + bestK.toString + "," + time.toString
		val run_file = new java.io.PrintWriter(new java.io.File(filename))
		run_file.println(run_data)
		run_file.close()
		print("OK\n")
	}



	/*
	def elbowMethod(path: String, data: RDD[(Double, Double)], minK: Int, maxK: Int): Unit = {
		val ks = Range(minK, maxK + 1)

		val start = System.nanoTime()
		val wcss = ks.map(k => {
			println(s"\nK: $k")
			val centroids = initializeCentroids(k, data)
			val clusterCentroids = clustering_function(data: RDD[(Double,Double)], centroids: )
			saveCluster(k, clusterCentroids)
			val squaredErrors = data.map(point => {
				val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				minDistance * minDistance
			})
			squaredErrors.sum
		})
		val end = System.nanoTime()
		print("\n\nTime: " + (end - start) / 1e9d + "s")

		saveClusterCsv(data.collect().toList, path)
		saveWcss(path + "_elbow.csv", ks, wcss)

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		saveRun(path + "_run.csv", minK, maxK, bestK, end - start)

		bestK
	}

	*/


}


