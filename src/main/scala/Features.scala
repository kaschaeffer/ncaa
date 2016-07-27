import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.when

object Utils {
  /* TODO put this in its own class */
  def aliasedCol(alias: String, columnName: String): Column = {
    col(alias + "." + columnName)
  }
}

trait Feature {
  val name: String
  def transformation(df: DataFrame): Column
}

trait FeatureBundle {
  val features: List[Feature] // apparently this can't be private (not sure why)
  def compute(df: DataFrame): DataFrame
  lazy val featureNames = features.map(_.name)
}

object GameFeatures extends FeatureBundle {
  override def compute(df: DataFrame): DataFrame = {
    var dfWithFeatures = df
    for (feature <- features) {
      dfWithFeatures = dfWithFeatures
        .withColumn(feature.name, feature.transformation(dfWithFeatures))
    }
    dfWithFeatures
  }


  object LocationNumeric extends Feature {
    def locationToNumeric(loc: String): Double = loc match {
        case "H" => 1
        case "N" => 0
        case _ => -1
      }
    val locationToNumericUdf = udf(locationToNumeric _)

    override val name = "loc-num"
    override def transformation(df: DataFrame): Column = locationToNumericUdf(df("loc"))
  }

  override val features = List(
    LocationNumeric
  )

}

object SeasonHistoryFeatures extends FeatureBundle {
  val alias1 = "df1"
  val alias2 = "df2"


  override def compute(df: DataFrame): DataFrame = {
    var df1 = df.as(alias1)
    var df2 = df.as(alias2)
    var oldColumns = df.columns
    var dfJoined = df1.join(
      df2, (
        Utils.aliasedCol("df1", "Season") === Utils.aliasedCol("df2", "Season") &&
        Utils.aliasedCol("df1", "team") === Utils.aliasedCol("df2", "team") && 
        Utils.aliasedCol("df1", "Daynum") > Utils.aliasedCol("df2", "Daynum")
      )
    )
    var aggregators = features.map(feature => feature.transformation(df).alias(feature.name))
    dfJoined = dfJoined
      .groupBy(
        Utils.aliasedCol(alias1, "Season"),
        Utils.aliasedCol(alias2, "Daynum"),
        Utils.aliasedCol(alias2, "team")
      )
      .agg(aggregators.head, aggregators.tail:_*)

    dfJoined
  }

  object WinFraction extends Feature {
    override val name = "win-fraction"
    override def transformation(df: DataFrame): Column =
      avg(when(Utils.aliasedCol(alias2, "score") > Utils.aliasedCol(alias2, "otherscore"), 1.0).otherwise(0.0))
  }

  object AvgPointDifference extends Feature {
    override val name = "avg-point-difference"
    override def transformation(df: DataFrame): Column =
      avg(Utils.aliasedCol(alias2, "score") - Utils.aliasedCol(alias2, "otherscore"))
  }

  override val features = List(
    WinFraction,
    AvgPointDifference   
  )
}

object AllHistoryFeatures


