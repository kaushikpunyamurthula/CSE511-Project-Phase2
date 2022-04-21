package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.createOrReplaceTempView("pickupInfo")
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    spark.udf.register("countNeighbors", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int) => ((HotcellUtils.getNeighbors(minX, minY, minZ, maxX, maxY, maxZ, inputX, inputY, inputZ))))
    spark.udf.register("checkGScore", (x: Int, y: Int, z: Int, mean:Double, std: Double, neighbors: Int, pointSum: Int, numCells: Int) => ((HotcellUtils.getGScore(x, y, z, mean, std, neighbors, pointSum, numCells))))

    val rangeQuery: String = "select x,y,z from pickupInfo where (x between "+minX+" and "+maxX+") and (y between "+minY+" and "+maxY+") and (z between "+minZ+" and "+maxZ+") order by z,y,x"
    val rangeCells = spark.sql(rangeQuery)
    rangeCells.createOrReplaceTempView("rangeResult")

    val pointQuery: String = "select x, y, z, count(*) as pointsCount from rangeResult group by z, y, x order by z, y, x"
    val pointData = spark.sql(pointQuery)
    pointData.createOrReplaceTempView("pointResult")

    val pointSumQuery: String = "select count(*) as countVal, sum(pointsCount) as pointsSum, sum(pow(pointsCount, 2)) as pointsSquaredSum from pointResult"
    val pointsSumData = spark.sql(pointSumQuery)

    val neighborQuery: String = "select countNeighbors(" + minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + " pr1.x, pr1.y, pr1.z) as neighbors, pr1.x as x, pr1.y as y, pr1.z as z, sum(pr2.pointsCount) as pointsSum from pointResult as pr1, pointResult as pr2 where pr2.x in(pr1.x-1, pr1.x, pr1.x+1) and pr2.y in(pr1.y-1, pr1.y, pr1.y+1) and pr2.z in(pr1.z-1, pr1.z, pr1.z+1) group by pr1.z, pr1.y, pr1.x order by pr1.z, pr1.y, pr1.x"
    val neighborData = spark.sql(neighborQuery)
    neighborData.createOrReplaceTempView("neighborResult");

    val mean = (pointsSumData.first().getLong(1).toDouble / numCells.toDouble).toDouble
    val std = math.sqrt((pointsSumData.first().getDouble(2).toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble)).toDouble

    val gScoreQuery: String = "select checkGScore(x, y, z, " + mean + ", " + std + ", neighbors, pointsSum," +numCells+ ") as getisOrdStat, x, y, z from neighborResult order by getisOrdStat desc limit 50"
    val gScoreData = spark.sql(gScoreQuery)
    val result = gScoreData.select(col("x"), col("y"), col("z"))
    return result
  }
}
