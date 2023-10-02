package NDD_mapreduce

/** A simple result of map or reduce phase
 *
 * @constructor create a new result with 2 fields, a string and a number
 * @param str
 * @param num
 * @author Anne-Cecile Caron
 */
class StringIntCouple(var str: String, var num: Int)  extends Row{

    override def toString: String = "(" + str + ", " + num + ")"

}
