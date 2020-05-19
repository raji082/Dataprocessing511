package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  
  def userContains(pointString:String, queryRectangle:String):Boolean={
    try {
      var point: Array[String] = new Array[String](2)
      var rectangle: Array[String] = new Array[String](4)

      point = pointString.split(",")
      point(0) = point(0).trim()
      point(1) = point(1).trim()
      var pointX: Double = point(0).toDouble
      var pointY: Double = point(1).toDouble

      rectangle = queryRectangle.split(",")
      rectangle(0) = rectangle(0).trim()
      rectangle(1) = rectangle(1).trim()
      rectangle(2) = rectangle(2).trim()
      rectangle(3) = rectangle(3).trim()
      var rectangleX1: Double = rectangle(0).toDouble
      var rectangleY1: Double = rectangle(1).toDouble
      var rectangleX2: Double = rectangle(2).toDouble
      var rectangleY2: Double = rectangle(3).toDouble

      var min_x = Math.min(rectangleX1, rectangleX2)
      var max_x = Math.max(rectangleX1, rectangleX2)
      var min_y = Math.min(rectangleY1, rectangleY2)
      var max_y = Math.max(rectangleY1, rectangleY2)

       if(pointX>= min_x && pointX<= max_x && pointY>=min_y && pointY<=max_y)
         return true
      else
         return false
    }
    catch {
      case _: Throwable => return false
    }
  }

  def userWithin(pointString1:String, pointString2:String, distance:Double):Boolean={
    try {
          var point1:Array[String] = new Array[String](2)
          var point2:Array[String] = new Array[String](2)

          point1 = pointString1.split(",")
          point1(0) = point1(0).trim()
          point1(1) = point1(1).trim()
          var point1X:Double= point1(0).toDouble
          var point1Y:Double= point1(1).toDouble
        
          point2 = pointString2.split(",")
          point2(0) = point2(0).trim()
          point2(1) = point2(1).trim()
          var point2X:Double= point2(0).toDouble
          var point2Y:Double= point2(1).toDouble

          var pDistance:Double = Math.sqrt(Math.pow((point1X - point2X), 2) + Math.pow((point1Y - point2Y), 2))
          
          if(pDistance <= distance)
            return true 
          else
            return false
        }
        catch {
            case _: Throwable => return false
        }
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((userContains(pointString,queryRectangle))))
    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((userContains(pointString,queryRectangle))))
    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    //spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((userWithin(pointString1,pointString2,distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    //spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((userWithin(pointString1,pointString2,distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
