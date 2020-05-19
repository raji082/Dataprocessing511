package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def isCube(x:Double, y:Double, z:Int, minX:Double, maxX:Double, minY:Double, maxY:Double, minZ:Int, maxZ:Int): Boolean =
  {
    if ( (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY) && (z >= minZ) && (z <= maxZ) ){
      return true
    }
    return false
  }

  def square(inputX: Int) : Double={
    return (inputX*inputX).toDouble
  }

  def CheckBoundary(point: Int, minVal: Int, maxVal: Int) : Int={
    if (point == minVal || point == maxVal){
      return 1
    }
    else {
      return 0
    }
  }

  def calMean(xiIn: Long, yiIn: Double): Double={
    return (xiIn/yiIn).toDouble
  }

  def getLocationPoint(intialState: Int): Int={
    val pointLocationInCube: Map[Int, Int] = Map(0->26, 1 -> 17, 2-> 11, 3-> 7)
    var location = pointLocationInCube.get(intialState).get.toInt
    return location
  }

  def NeighbourCount(minX:Int, minY:Int, minZ:Int, maxX:Int, maxY:Int, maxZ:Int, xIn:Int, yIn:Int, zIn:Int): Int ={
    var intialState = 0;
    intialState += CheckBoundary(xIn, minX, maxX)
    intialState += CheckBoundary(yIn, minY, maxY)
    intialState += CheckBoundary(zIn, minZ, maxZ)
    var location = getLocationPoint(intialState)
    return location
  }

  def GScore(numcells: Int, mean:Double, sd: Double, tNeighbours: Int, sumNeighbourPoints: Int): Double ={
    val dividend = ((sumNeighbourPoints.toDouble) - (mean*tNeighbours).toDouble)
    val x = (numcells * tNeighbours).toDouble
    val y = (tNeighbours * tNeighbours).toDouble
    val z = (numcells-1).toDouble
    val divisor = (sd * math.sqrt((x - y) / z)).toDouble
    val gscore = (dividend/divisor).toDouble
    return gscore
  }
}
