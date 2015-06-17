import org.apache.hadoop.io.compress.{GzipCodec, CompressionCodec}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._


class SparkS3PlaygroundSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

	val logFilePath: String = "s3n://kanicz-spark-demo/access_log"
	val modifiedLogFilePath: String = "s3n://kanicz-spark-demo/access_log_modified"

	val conf: SparkConf = new SparkConf().setAppName("foo").setMaster("local")
	val sc: SparkContext = new SparkContext(conf)
	var logLines: RDD[String] = null

	it should "download log from S3, process and upload gzipped result back to S3" in {

		//download from S3
		logLines = sc.textFile(logFilePath, 2)

		// modify each line somehow
		val modifiedLogs = logLines.map(line => (line + "foo", ""))

		modifiedLogs
			.coalesce(1)
			.saveAsHadoopFile(modifiedLogFilePath,
				classOf[String],
				classOf[String],
				classOf[TextOutputFormat[String, String]],
				classOf[GzipCodec])

		// check merged and gzipped output on s3
	}

	override def beforeAll() {
		//TODO: set aws credentials
		sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
		sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")
	}

	override def afterAll() {
		sc.stop()
	}
}