package parallel

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.math.{pow, sqrt}
import scala.util.Random

trait clustering_alg {
	val file_path: String = "./src/resources/umbria_xy.csv"

	def euclideanDistance(p1: (Double, Double), p2: (Double, Double)): Double = {
		sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2))
	}


	def loadData(spark: SparkSession): RDD[(Double, Double)] = {
		spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
		  .select("x", "y")
		  .rdd
		  .map(row => (row.getDouble(0), row.getDouble(1)))
		  .persist()
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
}
