package parallel

import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.Vector

import java.util
object kcenter extends clustering_alg {


    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("PARALLEL-KCenter").master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        val data = loadData(spark)
        val start = System.nanoTime()
        val bestK = elbowMethod(data, 2, 100, 10)
        val end = System.nanoTime()
        println("\nTime: " + (end - start) / 1e9d + "s\n")
        println(s"Best K: $bestK")
        spark.stop()
        bestK
    }

    //kCenter algorithm that use map reduce statements

    import org.apache.spark.rdd.RDD

    def kCenter(data: RDD[(Double, Double)], centroids: RDD[(Double, Double)]): Array[(Double, Double)] = {
        // Collect the centroids to the driver
        val collectedCentroids = centroids.collect()
        val numCentroids = collectedCentroids.length

        // Assign a random point in each partition as the initial center
        val partitions = data.randomSplit(Array.fill(numCentroids)(1.0), seed = 123)
        var clusters = collectedCentroids.clone()

        // Repeat until the radius of the set of centers does not change
        var radius = Double.MaxValue
        while (radius > 1e-6) {
            // Assign each data point to its nearest center
            val assigned = data.map(point => {
                val (x, y) = point
                var minDistance = Double.PositiveInfinity
                var nearestCluster = -1
                for (i <- clusters.indices) {
                    val distance = euclideanDistance((x, y), clusters(i))
                    if (distance < minDistance) {
                        minDistance = distance
                        nearestCluster = i
                    }
                }
                (nearestCluster, (x, y))
            })

            // Collect the assigned data points for each center and calculate the new center
            val newCenters = assigned
              .reduceByKey(sumPoints)
              .mapValues { case (sumX: Double, sumY: Double) => (sumX / numCentroids, sumY / numCentroids) }
              .collect()


            // Update the centers and radius
            radius = clusters
              .zip(newCenters.map(_._2)) // extract the second element of each tuple in newCenters
              .map { case (c1, (c2x, c2y)) => euclideanDistance(c1, (c2x, c2y)) }
              .max

            clusters = newCenters.map { case (_, center) => center }
        }

        clusters
    }

    def elbowMethod(data: RDD[(Double, Double)], minK: Int, maxK: Int, maxIterations: Int): Int = {
        val ks = Range(minK, maxK + 1)
        val wcss = ks.map(k => {
            println(s"\nK: $k")
            val centroids = data.takeSample(withReplacement = false, k, seed = 123)
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

    def sumPoints(p1: (Double, Double), p2: (Double, Double)): (Double, Double) = {
        (p1._1 + p2._1, p1._2 + p2._2)
    }

}
