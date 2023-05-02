package parallel
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.sql.SparkSession

object kmeans extends parallel.clustering_alg {
	var maxIterations: Int = 100

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Sequential-KMeans").master("local[*]").getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		val data = loadData(spark)
		val start = System.nanoTime()
		//val bestK = elbowMethod(data, 2, 100, 10)
		val end = System.nanoTime()
		println("\nTime: " + (end - start) / 1e9d + "s\n")
		//println(s"Best K: $bestK")
		spark.stop()
		//bestK
	}
}