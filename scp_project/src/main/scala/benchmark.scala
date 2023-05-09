import org.apache.spark.sql.SparkSession
import org.threeten.extra.scale.UtcRules.system
import parallel._

object benchmark {
	def main(args: Array[String]): Unit = {
		parallel.kmeans.main(Array())
		sequential.kmeans.main(Array())
		parallel.kcenter.main(Array())
		sequential.kcenter.main(Array())
	}
}
