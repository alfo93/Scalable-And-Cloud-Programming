package sequential
import org.apache.spark.sql.SparkSession


object ElbowMethod extends clustering_alg {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder
		  .appName("Sequential-ElbowMethod")
		  .config("spark.master", "local")
		  .getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")

		val data= loadData(spark)
		val k = elbowMethod(data.collect().toList, 2, 20, 10)
		println(s"\nBest K: " + k)
	}

	def elbowMethod(data: List[(Double, Double)], minK: Int, maxK: Int, maxIterations: Int): Unit = {
		val ks = Range(minK, maxK + 1)
		val wcss = ks.map(k => {
			println(s"\nK: $k")
			val centroids = scala.util.Random.shuffle(data).take(k)
			val clusterCentroids = kmeans.kMeans(data, centroids, maxIterations)
			val squaredErrors = data.map(point => {
				val distances = clusterCentroids.map(centroid => euclideanDistance(point, centroid))
				val minDistance = distances.min
				minDistance * minDistance
			})
			squaredErrors.sum
		})

		val diff = wcss.zip(wcss.tail).map(pair => pair._2 - pair._1)
		val bestK = ks(diff.indexOf(diff.max) + 1)
		bestK
	}
}
