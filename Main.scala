import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
// $example off$
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // Create a local SparkSession
    val spark = SparkSession
      .builder()
      .appName("RandomForestClassifierExample")
      .master("local[*]")
      .getOrCreate()

    // create an RDD from "data/wine.csv" file
    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/roma_xy.csv")
  }
}