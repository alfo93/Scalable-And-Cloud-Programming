import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

object Main {
	def main(args: Array[String]): Unit = {
		// Create a local SparkSession
		val spark = SparkSession
			.builder()
			.appName("MR k-center with outliers")
			.master("local[*]")
			.getOrCreate()

		// create an RDD from "resources/roma_xy.csv" file
		val data = spark
			.read
			.format("csv")
			.option("header", "true")
			.option("inferSchema", "true")
			.load("./src/resources/roma_xy.csv")

		// print the schema
		data.printSchema()

		// Prepare data for clustering
		val assembler = new VectorAssembler()
				.setInputCols(Array("x", "y"))
				.setOutputCol("features")
		val featureVector = assembler.transform(data)

		// Train KMeans model
		val k = 3 // number of clusters
		val kmeans = new KMeans()
		.setK(k)
		.setSeed(1L)
		.setMaxIter(10)
		val model = kmeans.fit(featureVector)

		// Get cluster centers
		val centers = model.clusterCenters
		println(s"Cluster Centers: ${centers.mkString(",")}")

		// Get predictions
		val predictions = model.transform(featureVector)
		predictions.show()

		// Stop SparkSession
		spark.stop()
	}
}