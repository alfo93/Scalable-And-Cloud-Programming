package parallel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object kcenter extends scala.clustering_alg {
    val spark = SparkSession.builder().appName("PARALLEL-KCenter").master("local[*]").getOrCreate()
    def main(args: Array[String]): Unit = {

        spark.sparkContext.setLogLevel("ERROR")
        val data = loadData(spark)
        val start = System.nanoTime()
        val bestK = elbowMethod(data,  2, 10)
        val end = System.nanoTime()
        println("\nTime: " + (end - start) / 1e9d + "s\n")
        println(s"Best K: $bestK")
        spark.stop()
    }


    def kCenter(data: RDD[(Double, Double)], centroids: RDD[(Double, Double)]): Array[(Double, Double)] = {
        var centers = Array(centroids.first())

        // Find the farthest point from the nearest cluster for each point
        for (i <- 1 until centroids.count().toInt) {
            val farthestPoint = data
              .map(point => (point, centers.minBy(center => euclideanDistance(center, point))))
              .reduce((a, b) => if (euclideanDistance(a._1, a._2) > euclideanDistance(b._1, b._2)) a else b)
              ._1

            centers = centers :+ farthestPoint
        }

        centers
    }


    def kCenter2(data: RDD[(Double, Double)], centroids: RDD[(Double, Double)]): Array[(Double, Double)] = {
        // Initialize the centers with the first centroid
        var centers = List(centroids.first())

        // Broadcast the centers to all worker nodes
        var centersBroadcast = spark.sparkContext.broadcast(centers)

        // Loop until we have found all the centers
        while (centers.length < centroids.count()) {
            // Find the farthest point from the nearest center for each point
            val farthestPoint = data.map(point => (point, centersBroadcast.value.minBy(center => euclideanDistance(center, point))))
              .reduce((a, b) => if (euclideanDistance(a._1, a._2) > euclideanDistance(b._1, b._2)) a else b)
              ._1

            // Add the farthest point to the centers list
            centers = farthestPoint :: centers

            // Update the broadcast variable with the new centers list
            centersBroadcast.unpersist()
            centersBroadcast = spark.sparkContext.broadcast(centers)
        }

        // Convert the centers list to an array and return it
        centers.reverse.toArray
    }
    def elbowMethod(data: RDD[(Double, Double)], minK: Int, maxK: Int): Int = {
        val ks = Range(minK, maxK + 1)
        val wcss = ks.map(k => {
            println(s"\nK: $k")
            val centroids = initializeCentroids(k, data)
            val clusterCentroids = kCenter(data, centroids)
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
