package scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import java.awt.{Color, Dimension}
import javax.swing.JFrame
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.{pow, sqrt}
import scala.util.Random



trait clustering_alg {
	val file_path: String = "./src/resources/roma_xy.csv"
	val random = new Random(42)

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

	def initializeCentroids(k: Int, data: RDD[(Double, Double)]): RDD[(Double, Double)] = {
		val xCoords = data.map(_._1)
		val yCoords = data.map(_._2)
		val xRange = xCoords.max - xCoords.min
		val yRange = yCoords.max - yCoords.min
		val centroids = data.sparkContext.parallelize(
			for (i <- 1 to k) yield {
				val x = xCoords.min() + (xRange * Random.nextDouble)
				val y = yCoords.min() + (yRange * Random.nextDouble)
				(x, y)
			}
		)
		centroids
	}


	//closestCentroid is a helper function that takes a data point and a list of centroids,
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

	def save_wcss(filename:String, ks: Range, wcss: Seq[Double]): Unit = {
		val wcss_data = ks.zip(wcss).map(x => x._1.toString + "," + x._2.toString)
		val wcss_file = new java.io.PrintWriter(new java.io.File(filename))
		wcss_data.foreach(wcss_file.println)
		wcss_file.close()
	}

}
