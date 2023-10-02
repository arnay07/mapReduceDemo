package NDD_mapreduce

/**
 * abstract class representing a couple of functions (map,reduce)
 * @author Anne-Cecile Caron
 */
abstract class MapReduceFunctions {
  /**
   * map : input row => list of (Key, value) pairs
   * This list can have 0, 1 or several elements
   */
  def map(elt: Row): List[(String, Row)]

  /**
   * reduce : (key, list of values) => one (key,value) pair. So it "reduces" (or aggregates) the list of values
   */
  def reduce(key: String, results: List[Row]) : (String, Row)
}
