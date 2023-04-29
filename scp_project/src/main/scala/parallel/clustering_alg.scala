package parallel

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

trait clustering_alg {
	val file_path: String = "./src/resources/umbria_xy.csv"

	def loadData(spark: SparkSession): RDD[(Double, Double)] = {
		spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
		  .select("x", "y")
		  .rdd
		  .map(row => (row.getDouble(0), row.getDouble(1)))
		  .persist()
	}
}
