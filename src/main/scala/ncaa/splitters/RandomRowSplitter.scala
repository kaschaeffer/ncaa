package ncaa.splitters

import org.apache.spark.sql.DataFrame

object RandomRowSplitter {
  def apply(trainingFraction: Double): RandomRowSplitter =
    new RandomRowSplitter(trainingFraction)
}

class RandomRowSplitter(trainingFraction: Double) extends Splitter {
  require(
    trainingFraction >= 0.0 && trainingFraction <= 1.0,
    "trainingFraction must in range [0.0, 1.0]")

  def split(df: DataFrame): SplitData = {
    val trainingAndTest = df.randomSplit(Array(trainingFraction, 1 - trainingFraction))
    SplitData(trainingAndTest(0), trainingAndTest(1))
  }
}
