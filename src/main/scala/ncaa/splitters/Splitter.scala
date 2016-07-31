package ncaa.splitters

import org.apache.spark.sql.DataFrame

case class SplitData(training: DataFrame, test: DataFrame)

trait Splitter {
  def split(df: DataFrame): SplitData
}
