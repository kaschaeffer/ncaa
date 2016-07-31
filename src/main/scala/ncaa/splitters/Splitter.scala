package ncaa.splitters

import org.apache.spark.sql.DataFrame

/** Disjoint data split into training and test DataFrames.
  *
  * @param training training data
  * @param test     test data
  */
case class SplitData(training: DataFrame, test: DataFrame)

/** Splits data into two disjoint sets.
  *
  * The split may either be deterministic or random, but each
  * row of the original dataset must appear exactly once
  * in either the test or training datasets.
  */
trait Splitter {
  def split(df: DataFrame): SplitData
}
