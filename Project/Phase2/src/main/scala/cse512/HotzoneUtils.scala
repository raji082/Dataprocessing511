package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
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
}
