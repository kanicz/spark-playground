import java.util.Optional

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._


class SparkPlaygroundSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val logFilePath: String = getClass().getResource("sample_logs/access_log").getPath()
  val conf: SparkConf = new SparkConf().setAppName("foo").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)
  var logLines: RDD[String] = null

  it should "load sample log into RDD" in {
    logLines = sc.textFile(logFilePath)
  }

  it should "calculate average, minimum and maximum content size of the response returned" in {
    val sumReducer: (Long, Long) => Long = (a, b) => a + b
    val parsedLogs: RDD[ApacheAccessLog] = logLines.map(ApacheAccessLog.parseFromLogLine).cache()

    val contentSizes: RDD[Long] = parsedLogs.map(_.getContentSize).cache()
    val averageContentSize: Long = contentSizes.reduce(sumReducer) / contentSizes.count()
    val minContentSize = contentSizes.min()
    val maxContentSize = contentSizes.max()

    assert(averageContentSize == 7079)
    assert(minContentSize == 0)
    assert(maxContentSize == 138789)
  }

  override def afterAll() {
    sc.stop()
  }
}