package partitional_clustering

object Benchmark {
	private val ITERS = 1
	private val TEST_SEQUENTIAL = true
	private val TEST_PARALLEL = true

	def main(args: Array[String]): Unit = {
		if (TEST_PARALLEL) {
			testAlg(parallel.KMeans, args)
			testAlg(parallel.KCenter, args)
		}

		if (TEST_SEQUENTIAL) {
			testAlg(sequential.KMeans, args)
			testAlg(sequential.KCenter, args)
		}
	}

	private def testAlg(func: PartitionalClustering, filePath: Array[String]): Unit = {
		val times = (1 to ITERS).map { _ =>
			val (_, time) = func.main(filePath)
			time
		}

		val mean = times.sum / ITERS
		val std = Math.sqrt(times.map(x => Math.pow(x - mean, 2)).sum / ITERS)
		println(s"${func.getAlgorithmName} - Mean: $mean - Std: $std")
	}
}
