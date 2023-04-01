import org.apache.spark.sql.SparkSession
import scala.io.Source

trait clustering_alg {
	val file_path: String = "./src/resources/roma_xy.csv"

	// Euclidean distance between two points
	def euclideanDistance(p1: (Double, Double), p2: (Double, Double)): Double = {
		val x = p1._1 - p2._1
		val y = p1._2 - p2._2
		math.sqrt(x * x + y * y)
	}

	def loadData(spark: SparkSession, file_path: String) = {
		spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
		  .select("x", "y")
		  .rdd
		  .map(row => (row.getDouble(0), row.getDouble(1)))
		  .cache()
	}
}
