package density_based.sequential

import density_based.DensityClustering
import org.apache.spark.sql.SparkSession
object dbscan extends DensityClustering {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("Sequential-DBSCAN>").master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        val data = loadData(spark)
        val eps = 0.0005
        val minPts = 5
        val res = dbscan(data.collect().toList, eps, minPts)
        println(res)
        spark.stop()
    }
    def dbscan(data: List[(Double, Double)], eps: Double, minPts: Int): Map[(Double, Double), Int] = {
        var clusterId = 0
        var visited = Map.empty[(Double, Double), Boolean]
        var clustered = Map.empty[(Double, Double), Boolean]
        var clusters = Map.empty[Int, Set[(Double, Double)]]

        while (visited.size < data.size) {
            print(visited.size + "/" + data.size + "\r")
            val unvisited = data.filterNot(visited.contains)
            val point = unvisited.head
            visited += point -> true
            var neighbors = getNeighbors(point, data, eps)

            if (neighbors.size < minPts) {
                clustered += point -> true
            } else {
                clusterId += 1
                var newCluster = Set(point)
                clustered += point -> true

                var i = 0
                while (i < neighbors.size) {
                    val neighbor = neighbors.toList(i)
                    if (!visited.getOrElse(neighbor, false)) {
                        visited += neighbor -> true
                        val neighborNeighbors = getNeighbors(neighbor, data, eps)
                        if (neighborNeighbors.size >= minPts) {
                            neighbors ++= neighborNeighbors
                        }
                    }
                    if (!clustered.getOrElse(neighbor, false)) {
                        newCluster += neighbor
                        clustered += neighbor -> true
                    }
                    i += 1
                }

                clusters += clusterId -> newCluster
            }
        }

        var result = Map.empty[(Double, Double), Int]
        for ((clusterId, cluster) <- clusters) {
            for (point <- cluster) {
                result += point -> clusterId
            }
        }
        println("")
        result
    }
}
