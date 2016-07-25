import scala.annotation.tailrec

import org.apache.spark.SparkContext

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.rowNumber
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler

object Main {

  /** Transforms one-row-per-game data to one-row-per-team-game data.
   *
   * @param games the one-row-per-game dataset
   * @return a new DataFrame with two rows per game (one for each team)
   */
  def getTeamGameResults(games: DataFrame): DataFrame = {
    val winners = games
      .withColumnRenamed("Wteam", "team")
      .withColumnRenamed("Wscore", "score")
      .withColumnRenamed("Wloc", "loc")
      .withColumnRenamed("Lteam", "otherteam")
      .withColumnRenamed("Lscore", "otherscore")
      .withColumn("label", lit(1.0))

    def swapLocation(loc: String): String = loc match {
      case "H" => "A"
      case "A" => "H"
      case l => l
    }
    val swapLocationUdf = udf(swapLocation _)

    val losers = games
      .withColumnRenamed("Lteam", "team")
      .withColumnRenamed("Lscore", "score")
      .withColumn("loc", swapLocationUdf(games("Wloc")))
      .drop("Wloc")
      .withColumnRenamed("Wteam", "otherteam")
      .withColumnRenamed("Wscore", "otherscore")
      .withColumn("label", lit(0.0))

    winners.unionAll(losers)
  }

  /** Split into training and testing datasets
   */
  def splitData(teamGameResults: DataFrame): (DataFrame, DataFrame) = {
    val TRAINING_FRACTION = 0.7
    val split = teamGameResults.randomSplit(Array(TRAINING_FRACTION, 1 - TRAINING_FRACTION))
    (split(0), split(1))
  }

  def getLabelAndFeatures(teamGameResults: DataFrame): DataFrame = {
    val featureBundles: List[FeatureBundle] = List(GameFeatures, SeasonHistoryFeatures)
    val featureNames: Array[String] = featureBundles flatMap { b => b.featureNames } toArray

    val assembler = new VectorAssembler()
      .setInputCols(featureNames)
      .setOutputCol("features")

    val computedFeatures = featureBundles.map( b => b.compute(teamGameResults) )

    // join results together
    val labelAndFeatures = computedFeatures.reduce {
      (a, b) => a.as("df1").join(b.as("df2"), Seq("Season", "team", "Daynum"))
    }
    
    assembler
      .transform(labelAndFeatures.na.fill(0.0))
      .select("label", "features")
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "NCAA", "/usr/local/spark", Nil, Map(), Map())
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/RegularSeasonCompactResults.csv")

    // transform this into a dataset that has
    // two rows per game (one for winner and one for loser)
    val teamGameResults = getTeamGameResults(df)
    val teamGameLabelAndFeatures = getLabelAndFeatures(teamGameResults)
    val (training, test) = splitData(teamGameLabelAndFeatures)

    training.show()
    test.show()
    
    val lr = new LogisticRegression()
      .setLabelCol("label")

    val fitModel = lr.fit(training)
    // teamGameResults.show()
    val predictions = fitModel.transform(test)
    val evaluator = new BinaryClassificationEvaluator()
    println("MODEL SUMMARY")
    println("=============")
    println("   AUC: " + evaluator.evaluate(predictions))

    sc.stop
  }
}
