import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
object ElbowMethod extends clustering_alg {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder
          .appName("ElbowMethod")
          .config("spark.master", "local")
          .getOrCreate()

        // Load data from file
        val dataset = loadData(spark, file_path)
        val data = spark.createDataFrame(dataset).toDF("x", "y")

        // Convert features to vector
        val assembler = new VectorAssembler()
            .setInputCols(Array("x", "y"))
            .setOutputCol("features")


        val dataWithFeatures = assembler.transform(data)

        // Evaluate clustering models for different values of k
        //create array with 1...50 values
        val kValues = (2 to 50).toArray
        val evaluationScores = kValues.map { k =>
            val kmeans = new KMeans()
              .setK(k)
              .setSeed(1L)

            val model = kmeans.fit(dataWithFeatures)

            val predictions = model.transform(dataWithFeatures)

            val evaluator = new ClusteringEvaluator()

            evaluator.evaluate(predictions)
        }

        // Plot evaluation scores with the best k
        val bestK = kValues(evaluationScores.indexOf(evaluationScores.max))
        println(s"Best k: $bestK")
    }
}
