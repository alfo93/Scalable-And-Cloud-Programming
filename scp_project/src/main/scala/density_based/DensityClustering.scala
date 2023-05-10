package density_based

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

trait DensityClustering {

    private val random = new Random(42)
    private val file_path: String = "./src/resources/Umbria_xy.csv"
    private val results_by_k: mutable.Map[Int, Array[(Double, Double)]] = mutable.Map[Int, Array[(Double, Double)]]()

    // Define a function to compute the distance between two data points
    def distance(p1: (Double, Double), p2: (Double, Double)): Double = {
        math.sqrt((p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p1._2 - p2._2))
    }

    def getNeighbors(p: (Double, Double), data: List[(Double, Double)], eps: Double): Set[(Double, Double)] = {
        data.filter(distance(_, p) <= eps).toSet
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

}
