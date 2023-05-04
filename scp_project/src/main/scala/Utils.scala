import org.apache.spark.rdd.RDD
import parallel.kmeans.closestCentroid
import java.io.File

object Utils {
	def save_cluster_csv(filename: String, data: RDD[(Double, Double)], centroids: List[(Double, Double)]): Unit = {
		val closest_centroids = data.map(x => closestCentroid(x, centroids))
		val data_centroids = data.zip(closest_centroids)
		val data_centroids_str = data_centroids.map(x => x._1._1.toString + "," + x._1._2.toString + "," + x._2._1.toString + "," + x._2._2.toString)
		data_centroids_str.coalesce(1).saveAsTextFile(filename)
	}

	def delete_data(path: String, name: String): Unit = {
		val dir = new File(path)
		val files = Option(dir.listFiles)
		  .getOrElse(Array.empty[File])
		  .filter(_.getName.startsWith(name))

		for (file <- files) {
			try {
				file.delete()
			} catch {
				case e: Exception =>
					println(s"Error deleting file ${file.getName}: ${e.getMessage}")
			}
		}
	}


}
