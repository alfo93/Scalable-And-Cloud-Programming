import partitional_clustering.parallel
import partitional_clustering.sequential
import partitional_clustering.PartitionalClustering

object benchmark {
	private val iters = 10
	private val test_KMeans = true
	private val test_KCenter = true

	def main(String: Array[String]): Unit = {
		if (test_KMeans) {
			test_alg(parallel.KMeans)
			test_alg(sequential.KMeans)
		}

		if (test_KCenter) {
			test_alg(parallel.KCenter)
			test_alg(sequential.KCenter)
		}
	}

	private def test_alg(func: PartitionalClustering): Unit = {
		val times = new Array[Double](iters)
		for (i <- 0 until iters) {
			val (_, time) = func.main()
			times(i) = time
		}

		val mean = times.sum / iters
		val std = Math.sqrt(times.map(x => Math.pow(x - mean, 2)).sum / iters)
		print(func.getAlgorithmName + " - Mean: " + mean + " - Std: " + std + "\n")

	}
}
