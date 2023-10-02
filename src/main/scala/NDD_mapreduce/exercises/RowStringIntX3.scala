package NDD_mapreduce.exercises

import NDD_mapreduce.Row



/** A simple result of map or reduce phase
 *
 * @constructor create a new result with 4 fields, a string and 3 numbers
 * @param str
 * @param num1
 * @param num2
 * @param num3
 * @author Arnaud Kaderi
 */
class RowStringIntX3(var str: String, var num1: Double, var num2: Int, var num3: Int) extends Row{

  override def toString: String = "(" + str + ", " + num1 + ", " + num2 + ", " + num3 + ")"
}
