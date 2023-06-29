import partitional_clustering.parallel
import partitional_clustering.sequential
import partitional_clustering.PartitionalClustering

object benchmark {
	private val iters = 3
	private val test_KMeans = false
	private val test_KCenter = true

	def main(args: Array[String]): Unit = {
		if (test_KMeans) {
			test_alg(parallel.KMeans, args)
			test_alg(sequential.KMeans, args)
		}

		if (test_KCenter) {
			test_alg(parallel.KCenter, args)
			test_alg(sequential.KCenter, args)
		}
	}

	private def test_alg(func: PartitionalClustering, filePath: Array[String]): Unit = {
		val times = new Array[Double](iters)
		for (i <- 0 until iters) {
			val (_, time) = func.main(filePath)
			times(i) = time
		}

		val mean = times.sum / iters
		val std = Math.sqrt(times.map(x => Math.pow(x - mean, 2)).sum / iters)
		print(func.getAlgorithmName + " - Mean: " + mean + " - Std: " + std + "\n")

	}
}
