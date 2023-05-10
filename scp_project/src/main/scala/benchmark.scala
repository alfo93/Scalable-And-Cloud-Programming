import org.apache.spark.sql.SparkSession
import org.threeten.extra.scale.UtcRules.system
import partitional_clustering.parallel.{kcenter, kmeans}
import partitional_clustering.sequential

object benchmark {
	def main(args: Array[String]): Unit = {
		kmeans.main(Array())
		sequential.kmeans.main(Array())
		kcenter.main(Array())
		sequential.kcenter.main(Array())
	}
}
