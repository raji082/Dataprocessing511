package cse512

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

    pickupInfo.createOrReplaceTempView("pickupInfoView")

    spark.udf.register("isCube", (x: Double, y:Double, z:Int) => ( 
      HotcellUtils.isCube(x,y,z,minX,maxX,minY,maxY,minZ,maxZ)
    ))

    val pointsFilteredCube = spark.sql("select x,y,z from pickupInfoView where isCube(x,y,z) order by z,y,x").persist()
    pointsFilteredCube.createOrReplaceTempView("pointsFilteredCubeView")

    val pointsFilteredCubeCount = spark.sql("select x,y,z,count(*) as countPoints from pointsFilteredCubeView group by z,y,x order by z,y,x").persist()
    pointsFilteredCubeCount.createOrReplaceTempView("pointsFilteredCubeCountView")

    spark.udf.register("square", (inputX: Int) => (HotcellUtils.square(inputX)))

    val pointsSum = spark.sql("select count(*), sum(countPoints) , sum(square(countPoints)) from pointsFilteredCubeCountView")
    pointsSum.createOrReplaceTempView("sumofPoints")
      
    val xmean = HotcellUtils.calMean(pointsSum.first().getLong(1), numCells) // mean
    val sd = math.sqrt((pointsSum.first().getDouble(2) / numCells) - (xmean * xmean)) //standard deviation

    spark.udf.register("neighbourCount", (xIn: Int, yIn: Int, zIn: Int)=> 
    (
      (HotcellUtils.NeighbourCount(minX.toInt, minY.toInt, minZ.toInt, maxX.toInt, maxY.toInt, maxZ.toInt, xIn, yIn, zIn))
    ))

    val neighbours = spark.sql("select " + "temp1.x as x, " + "temp1.y as y," + "temp1.z as z,"+
      "neighbourCount(temp1.x,temp1.y,temp1.z) as tNeighbours, " +
      "count(*), " +
      "sum(temp2.countPoints) as sumNeighbourPoints " +
      "from pointsFilteredCubeCountView as temp1, pointsFilteredCubeCountView as temp2 " +
      "where (temp2.x = temp1.x+1 or temp2.x = temp1.x or temp2.x = temp1.x-1) and (temp2.y = temp1.y+1 or temp2.y = temp1.y or temp2.y = temp1.y-1) and (temp2.z = temp1.z+1 or temp2.z = temp1.z or temp2.z = temp1.z-1) " +
      "group by temp1.z, temp1.y, temp1.x order by temp1.z, temp1.y, temp1.x").persist()

    neighbours.createOrReplaceTempView("neighboursView")

    spark.udf.register("GScore", (tNeighbours: Int, sumNeighbourPoints: Int) => (
    (
      HotcellUtils.GScore(numCells.toInt, xmean, sd, tNeighbours, sumNeighbourPoints)
    )))

    val finalNeighbours = spark.sql("select x, y, z, " +
      "GScore(tNeighbours, sumNeighbourPoints) as gi_stats " +
      "from neighboursView " +
      "order by gi_stats desc")
      
    finalNeighbours.createOrReplaceTempView("neighboursFinalView")
    finalNeighbours.show()
    val result = spark.sql("select x,y,z from neighboursFinalView")

    return result
  }
}
