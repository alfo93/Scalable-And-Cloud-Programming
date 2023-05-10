package density_based.parallel

import density_based.DensityClustering
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object dbscan extends DensityClustering {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Sequential-DBSCAN>").master("local[*]").getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		val data = loadData(spark)
		val eps = 0.0005
		val minPts = 5
		val res = dbscan(data, eps, minPts)
		println(res)
		spark.stop()
	}

	import org.apache.spark.rdd.RDD

	def dbscan(data: RDD[(Double, Double)], eps: Double, minPts: Int): Map[(Double, Double), Int] = {
		var clusterId = 0
		var visited = Map.empty[(Double, Double), Boolean].withDefaultValue(false)
		var clustered = Map.empty[(Double, Double), Boolean].withDefaultValue(false)
		var clusters = Map.empty[Int, Set[(Double, Double)]]

		while (visited.values.count(_ == true) < data.count) {
			print(s"${visited.values.count(_ == true)}/${data.count}\r")
			val unvisited = data.filter(!visited(_))
			val point = unvisited.first
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
					if (!visited(neighbor)) {
						visited += neighbor -> true
						val neighborNeighbors = getNeighbors(neighbor, data, eps)
						if (neighborNeighbors.size >= minPts) {
							neighbors ++= neighborNeighbors
						}
					}
					if (!clustered(neighbor)) {
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

	def getNeighbors(point: (Double, Double), data: RDD[(Double, Double)], eps: Double): Set[(Double, Double)] = {
		data.filter(other => distance(other, point) <= eps).collect().toSet
	}




}
