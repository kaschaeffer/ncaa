package ncaa.splitters

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec

class RandomRowSplitterSpec extends FlatSpec with BeforeAndAfter {
  val sc = new SparkContext("local", "NCAA", "/usr/local/spark", Nil, Map(), Map())
  val sqlContext = new SQLContext(sc) 
  val tolerance = 0.01

  behavior of "RandomRowSplitter"

  it should "split into test and control in proportion to trainingFraction" in {
    import sqlContext.implicits._
    val seed = 1
    val trainingFraction = 0.3
    val splitter = RandomRowSplitter(trainingFraction, seed)

    val exampleDf = sc.parallelize(0 until 100000).toDF
    val actualTrainingFraction = splitter
      .split(exampleDf)
      .training.count * 1.0 / exampleDf.count

    assert((actualTrainingFraction - trainingFraction).abs < tolerance)
  }
}
