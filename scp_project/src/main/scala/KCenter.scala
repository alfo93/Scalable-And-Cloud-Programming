import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object KCenter extends clustering_alg {
    val k: Int = 25
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder
          .appName("KCenter")
          .config("spark.master", "local")
          .getOrCreate()

        // read from trait clustering_alg
        val data = loadData(spark, file_path)
        val clusters = kCenter(data, k)
        printResults(clusters)
        spark.stop()
    }

    def kCenter(data: RDD[(Double, Double)], k: Int): Array[(Double, Double)] = {
        val points = data.collect()
        val centers = mutable.Map(points.head -> 0)
        while (centers.size < k) {
            val distances = points.map { p =>
                centers.keys.map(c => math.sqrt((p._1 - c._1) * (p._1 - c._1) + (p._2 - c._2) * (p._2 - c._2))).min
            }
            val newCenter = points(distances.indexOf(distances.max))
            centers(newCenter) = 0
        }
        centers.keys.toArray
    }

    def kCenter_mr(data: RDD[(Double, Double)], k: Int): Array[(Double, Double)] = {
        // Select the first point as the first center
        val centers = data.take(1)

        // Compute distances between points and centers
        val distances = data.map(p => (p, centers.map(c => euclideanDistance(p, c)).min))

        // Iterate k-1 times to select the remaining centers
        for (i <- 2 to k) {
            // Select the farthest point from the current centers
            val farthest = distances.reduce((a, b) => if (a._2 > b._2) a else b)._1

            // Add the farthest point to the centers list
            centers :+ farthest

            // Recompute distances between points and centers
            distances.map { case (p, _) => (p, centers.map(c => euclideanDistance(p, c)).min) }
        }

        // Return the final centers as an array
        centers.toArray
    }
}
