package cse511

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

  //Finding the number of neighbors based on the external(outside facing) edge count
  def getNeighbors (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, pointX: Int, pointY: Int, pointZ: Int): Int =
  {
    var externalEdgeCount = 0

    if (pointX == minX || pointX == maxX)
      externalEdgeCount += 1

    if (pointY == minY || pointY == maxY)
      externalEdgeCount += 1

    if (pointZ == minZ || pointZ == maxZ)
      externalEdgeCount += 1

    if (externalEdgeCount == 1)
      return 17
    else if (externalEdgeCount == 2)
      return 11
    else if (externalEdgeCount == 3)
      return 7
    else
      return 26
  }

  // Finding the Getis-ord z-score of the points data
  def getGScore (x: Int, y: Int, z: Int, mean: Double, std: Double, neighbors: Int, pointSum: Int, numCells: Int): Double =
  {
    val num = pointSum.toDouble - (mean * neighbors.toDouble)
    val den = std * math.sqrt((((numCells.toDouble * neighbors.toDouble) - (neighbors.toDouble * neighbors.toDouble)) / (numCells.toDouble - 1.0).toDouble).toDouble).toDouble
    return (num / den).toDouble
  }
}
