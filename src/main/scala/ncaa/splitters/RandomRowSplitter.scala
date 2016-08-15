package ncaa.splitters

import org.apache.spark.sql.DataFrame

object RandomRowSplitter {
  def apply(trainingFraction: Double, seed: Long): RandomRowSplitter =
    new RandomRowSplitter(trainingFraction, Some(seed))

  def apply(trainingFraction: Double): RandomRowSplitter =
    new RandomRowSplitter(trainingFraction, None)
}

/** Randomly assigns each row independently to training or test dataset.
  *
  * @param trainingFraction probability that a given row should be placed
  * in the training dataset.  Must be in range [0.0, 1.0].
  */
class RandomRowSplitter(trainingFraction: Double, seed: Option[Long]) extends Splitter {
  require(
    trainingFraction >= 0.0 && trainingFraction <= 1.0,
    "trainingFraction must in range [0.0, 1.0]")

  override def split(df: DataFrame): SplitData = {
    val splitFractions = Array(trainingFraction, 1 - trainingFraction)
    val trainingAndTest = seed match {
      case Some(s) => df.randomSplit(splitFractions, s)
      case None    => df.randomSplit(splitFractions)
    }
    SplitData(trainingAndTest(0), trainingAndTest(1))
  }
}
