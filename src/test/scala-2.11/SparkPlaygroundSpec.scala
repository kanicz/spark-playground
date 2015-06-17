import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._


class SparkPlaygroundSpec extends FlatSpec with Matchers with BeforeAndAfterAll {


	/**
	 *
	 * Master modes:
	 *
	 * local	           Run Spark locally with one worker thread (i.e. no parallelism at all).
	 * local[K]           Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine).
	 * local[*]           Run Spark locally with as many worker threads as logical cores on your machine.
	 * spark://HOST:PORT  Connect to the given Spark standalone cluster master. The port must be whichever one your master is configured to use, which is 7077 by default.
	 * yarn-client        Connect to a YARN cluster in client mode. The cluster location will be found based on the HADOOP_CONF_DIR or YARN_CONF_DIR variable.
	 * yarn-cluster       Connect to a YARN cluster in cluster mode. The cluster location will be found based on the HADOOP_CONF_DIR or YARN_CONF_DIR variable.
	 */
	val conf: SparkConf = new SparkConf().setAppName("foo").setMaster("local")
	val sc: SparkContext = new SparkContext(conf)

	/**
	 * Any storage supported by Hadoop:
	 * - local file
	 * - HDFS
	 * - Cassandra
	 * - HBASE
	 * - S3
	 */
	val logFilePath: String = getClass().getResource("sample_logs/access_log").getPath()

	/**
	 * RDDs:
	 * - immutable once created
	 * - automatically recompute lost data
	 * - enable operations on collections in paralell
	 */
	var logLines: RDD[String] = null

	it should "load sample log into RDD" in {
		logLines = sc.textFile(logFilePath, 4)
	}

	it should "calculate average, minimum and maximum content size of the response returned" in {
		val sumReducer: (Long, Long) => Long = (a, b) => a + b
		val parsedLogs: RDD[ApacheAccessLog] = logLines.map(ApacheAccessLog.parseFromLogLine).cache()

		/**
		 * Transformations are lazy, don't trigger any computations
		 *
		 * RDDs may be cached:
		 * - MEMORY_ONLY (default)
		 * - MEMORY_AND_DISK
		 * - MEMORY_ONLY_SER
		 * - MEMORY_AND_DISK_SER
		 */
		val contentSizes: RDD[Long] = parsedLogs.map(_.getContentSize).cache()

		/**
		 * Actions (reduce(), collect(), count(), first(), take(n), takeOrdered()...) trigger transformations.
		 */
		val averageContentSize: Long = contentSizes.reduce(sumReducer) / contentSizes.count()

		val minContentSize = contentSizes.min()
		val maxContentSize = contentSizes.max()

		assert(averageContentSize == 7079)
		assert(minContentSize == 0)
		assert(maxContentSize == 138789)
	}

	/**
	 * Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks.
	 */
	it should "broadcast variable" in {
		val broadcastVar = sc.broadcast(Array(1, 2, 3))

		assert(broadcastVar.value === Array(1, 2, 3))
	}

	/**
	 * Accumulators are variables that are only “added” to through an associative operation and can therefore be efficiently supported in parallel.
	 * Common use case: count errors seen across workers.
	 */
	it should "use accumulator" in {
		val accum = sc.accumulator(0, "My Accumulator")
		sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)

		assert(accum.value == 10)
	}

	override def afterAll() {
		sc.stop()
	}
}

// Spark vs Hadoop
//
// Performance:
// 							Spark performs better when all the data fits in the memory
//
// Ease of use:
// 							Spark is easier to program and includes an interactive mode;
// 							Hadoop MapReduce is more difficult to program but many tools are available to make it easier.
//
// Cost:
// 							Spark is more cost-effective according to the benchmarks, though staffing could be more costly;
// 							Hadoop MapReduce could be cheaper because more personnel are available and because of Hadoop-as-a-service offerings.
//
// Compatibility:
//							Spark’s compatibility to data types and data sources is the same as Hadoop MapReduce.
//
// Security:
//							Spark security is still in its infancy; Hadoop MapReduce has more security features and projects.


// more:
// spark streaming 						https://spark.apache.org/streaming/
// data frames: 							https://databricks.com/blog/2015/02/17/introducing-dataframes-in-spark-for-large-scale-data-science.html
// petabyte sort benchmark: 	https://databricks.com/blog/2014/10/10/spark-petabyte-sort.html