import scala.annotation.tailrec

import org.apache.spark.SparkContext

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.rowNumber
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
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
      .withColumn("Wloc", swapLocationUdf(games("Wloc")))
      .withColumnRenamed("Wloc", "loc")
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

  def getLabelAndFeatures(teamGameResults: DataFrame): (Array[String], DataFrame) = {
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
    
    val transformedLabelAndFeatures = assembler
      .transform(labelAndFeatures.na.fill(0.0))
      .select("label", "features")
    (featureNames, transformedLabelAndFeatures)
  }

  case class Options(isTestRun: Boolean)

  def parseArgs(args: Array[String]): Option[Options] = {
    val usage = """
      |Usage: run [--is-test-run]
    """.stripMargin
    args.toList match {
      case Nil => Some(Options(false))
      case "--is-test-run" :: Nil => Some(Options(true))
      case _ => {
        println(usage)
        None
      }
    }
  }



  def main(args: Array[String]) {
    val options = parseArgs(args)
    if (options == None) {
      sys.exit(1)
    }

    val sc = new SparkContext("local[4]", "NCAA", "/usr/local/spark", Nil, Map(), Map())
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/raw/RegularSeasonCompactResults.csv")

    // transform this into a dataset that has
    // two rows per game (one for winner and one for loser)
    val finalDf = if (options.get.isTestRun) df.limit(100) else df
    val teamGameResults = getTeamGameResults(finalDf)
    // teamGameResults.write
    //   .format("com.databricks.spark.csv")
    //   .option("header", "true")
    //   .save("data/derived/RegularSeasonCompactResultsTeamGame.csv")

    val (featureNames, teamGameLabelAndFeatures) = getLabelAndFeatures(teamGameResults)
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(teamGameLabelAndFeatures)
    val teamGameIndexedLabelAndFeatures = labelIndexer.transform(teamGameLabelAndFeatures)

    val (training, test) = splitData(teamGameIndexedLabelAndFeatures)

    training.show()
    test.show()
    
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setNumTrees(100)

    // RandomForestClassificationModel has no support for feature importance (!)

    // val fitModel: RandomForestClassificationModel = rf.fit(training)
    val fitModel = rf.fit(training)
    // teamGameResults.show()
    val predictions = fitModel.transform(test)
    val evaluator = new BinaryClassificationEvaluator()
    println("MODEL SUMMARY")
    println("=============")
    println("\tAUC: " + evaluator.evaluate(predictions))

    val featureImportances = fitModel.featureImportances
    println("FEATURE IMPORTANCES")
    println("===================")
    for ((name, importance) <- featureNames zip featureImportances.toArray) {
      println(f"\t$name%-40s => $importance%1.4f")
    }

    sc.stop
  }
}
