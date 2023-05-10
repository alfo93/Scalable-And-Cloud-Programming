package partitional_clustering.parallel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import partitional_clustering.PartitionalClustering

object kcenter extends PartitionalClustering {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder().appName("Parallel-KCenter").master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        val data = loadData(spark)
        val bestK = elbowMethod(data, 10, 30)
        println(s"Best K: $bestK")
        spark.stop()
    }


    def kCenter(data: RDD[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
        var centers = List(centroids.head)

        // Find the farthest point from the nearest cluster for each point
        for (i <- 1 until centroids.length.toInt) {
            val farthestPoint = data
              .map(point => (point, centers.minBy(center => euclideanDistance(center, point))))
              .reduce((a, b) => if (euclideanDistance(a._1, a._2) > euclideanDistance(b._1, b._2)) a else b)
              ._1

            centers = centers :+ farthestPoint
        }

        centers.toArray
    }


    private def kCenter2(data: RDD[(Double, Double)], centroids: List[(Double, Double)]): Array[(Double, Double)] = {
        // Initialize the centers with the first centroid
        var centers = List(centroids.head)

        // Broadcast the centers to all worker nodes
        var centersBroadcast = data.sparkContext.broadcast(centers)

        // Loop until we have found all the centers
        while (centers.length < centroids.length) {
            // Find the farthest point from the nearest center for each point
            val farthestPoint = data.map(point => (point, centersBroadcast.value.minBy(center => euclideanDistance(center, point))))
              .reduce((a, b) => if (euclideanDistance(a._1, a._2) > euclideanDistance(b._1, b._2)) a else b)
              ._1

            // Add the farthest point to the centers list
            centers = farthestPoint :: centers

            // Update the broadcast variable with the new centers list
            centersBroadcast.unpersist()
            centersBroadcast = data.sparkContext.broadcast(centers)
        }

        // Convert the centers list to an array and return it
        centers.reverse.toArray
    }



    def elbowMethod(data: RDD[(Double, Double)], minK: Int, maxK: Int): Int = {
        val ks = Range(minK, maxK + 1)
        val start = System.nanoTime()
        val wcss = ks.map(k => {
            println(s"\nK: $k")
            val centroids = initializeCentroids(k, data)
            val clusterCentroids = kCenter2(data, centroids)
            saveCluster(k, clusterCentroids)
            val squaredErrors = data.map(point => {
                val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
                val minDistance = distances.min
                minDistance * minDistance
            })
            squaredErrors.sum
        })
        val end = System.nanoTime()
        val time = end - start
        print("\n\nTime: " + (end - start) / 1e9d + "s")

        saveClusterCsv(data.collect().toList, "./src/resources/parallels/kcenter_")
        saveWcss("./src/resources/parallels/kcenter_elbow.csv", ks, wcss)

        val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
        val bestK = ks(diff.indexOf(diff.max) + 1)
        saveRun("./src/resources/sequential/kcenter_run.csv", minK, maxK, bestK, time)
        bestK
    }

}