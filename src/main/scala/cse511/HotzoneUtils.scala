package cse511

object HotzoneUtils {

  // YOU NEED TO CHANGE THIS PART
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    if(queryRectangle.isEmpty || pointString.isEmpty)
      return false

    val x1 :: y1 :: x2 :: y2 :: _ = queryRectangle.split(",").map(_.trim().toDouble).toList
    val p_x :: p_y :: _ = pointString.split(",").map(_.trim().toDouble).toList

    return ((p_x >= x1 && p_x <= x2 || p_x <= x1 && p_x >= x2) && (p_y >= y1 && p_y <= y2 || p_y <= y1 && p_y >= y2))
  }

}
