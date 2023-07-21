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

	/**
	 * Calculates the Euclidean distance between two points represented as tuples (Double, Double).
	 *
	 * @param p1 The first point as a tuple (x1, y1).
	 * @param p2 The second point as a tuple (x2, y2).
	 * @return The Euclidean distance between the two points.
	 */
	def euclideanDistance(p1: (Double, Double), p2: (Double, Double)): Double = sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2))

	/**
	 * Loads data from a CSV file into an RDD of tuples (Double, Double) using Spark.
	 *
	 * @param spark    The SparkSession object to use for reading data.
	 * @return An RDD containing tuples (x, y) where x and y are Double values extracted from the CSV file.
	 */
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

	/**
	 * Initialize centroids for the clustering algorithm using either a List or RDD data.
	 *
	 * @param k    The number of centroids to initialize.
	 * @param data The input data from which centroids will be initialized.
	 * @tparam T The type of the data (List or RDD). Must be convertible to Traversable[(Double, Double)].
	 * @return A List of centroids initialized from the input data.
	 * @throws IllegalArgumentException if the input data type is not supported (List or RDD[(Double, Double)]).
	 */
	def initializeCentroids[T](k: Int, data: T): List[(Double, Double)] = {
		data match {
			case listData: List[(Double, Double)] => scala.util.Random.shuffle(listData).take(k)
			case rddData: RDD[(Double, Double)] =>
				rddData.takeSample(withReplacement = false, k, System.nanoTime().toInt).toList
			case _ => throw new IllegalArgumentException("Unsupported data type. Use List or RDD[(Double, Double)].")
		}
	}

	/**
	 * Find the closest centroid to a given point from a collection of centroids.
	 *
	 * @param point     The point for which the closest centroid needs to be found.
	 * @param centroids The collection of centroids to compare against.
	 * @tparam T The type of the collection (List, Array, etc.). Must be convertible to Traversable[(Double, Double)].
	 * @return The centroid from the collection that is closest to the given point.
	 */
	def closestCentroid[T](point: (Double, Double), centroids: T)(implicit ev: T => Traversable[(Double, Double)]): (Double, Double) = {
		centroids
		  .map(centroid => (centroid, euclideanDistance(point, centroid)))
		  .reduce((a, b) => if (a._2 < b._2) a else b)
		  ._1
	}


	/**
	 * Checks the convergence of K-means algorithm by comparing the distance between old and new centroids.
	 *
	 * @param oldCentroids List of old centroids as tuples (x, y).
	 * @param newCentroids List of new centroids as tuples (x, y).
	 * @return True if the distance between each old and new centroid is below the defined threshold (epsilon);
	 *         otherwise, returns False, indicating that the centroids have not yet converged.
	 */
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
		resultsByK.foreach { case (k, clusters) =>
			val clusters_data = data.map(point => {
				val centroid = closestCentroid(point, clusters)
				s"${point._1},${point._2},${centroid._1},${centroid._2}"
			})
			val clusters_file = new java.io.PrintWriter(new java.io.File(path + k.toString + ".csv"))
			clusters_data.foreach(clusters_file.println)
			clusters_file.close()
		}
		print("OK\n")
	}

	def saveWcss(filename: String, ks: Range, wcss: Seq[Double]): Unit = {
		if (!saveResults) return

		print("Saving WCSS...")
		val wcss_data = ks.zip(wcss).map(x => s"${x._1},${x._2}")
		val wcss_file = new java.io.PrintWriter(new java.io.File(filename))
		wcss_data.foreach(wcss_file.println)
		wcss_file.close()
		print("OK\n")
	}

	def saveRun(filename: String, minK: Int, maxK: Int, bestK: Int, time: Double): Unit = {
		if (!saveResults) return

		print("Saving run...")
		val run_data = s"$minK,$maxK,$bestK,$time"
		val run_file = new java.io.PrintWriter(new java.io.File(filename))
		run_file.println(run_data)
		run_file.close()
		print("OK\n")
	}

}


