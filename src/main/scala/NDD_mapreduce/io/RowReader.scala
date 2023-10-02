package NDD_mapreduce.io

import NDD_mapreduce.Row

import java.io.{FileInputStream, ObjectInputStream}

/**
 * Build a Row iterator, either from a CSV or from a serialization.
 * The input format of a RowReader corresponds to the output format of a RowWriter.
 * @author Anne-Cécile Caron
 */
trait RowReader  {
  /**
   * Build a Row iterator from a CSV.
   * @param pathToFile the path to the input CSV file, for instance '/home/toto/myrows.csv
   * @param delimiter the delimiter between CSV columns
   * @return a Row iterator, each Row object corresponds to a CSV row
   */
  def readCSV(pathToFile: String, delimiter: String) : Iterator[Row]

  /**
   * Build a Row iterator from a serialization (so it is a deserialization)
   * @param pathToFile the path to the file, for instance '/home/toto/myrows
   * @return a Row iterator
   */
  def readSerializedRows(pathToFile: String) : Iterator[Row] = {
    var rowList = List[Row]()
    val ois = new ObjectInputStream(new FileInputStream(pathToFile))
    var endOfFile = false

    while (!endOfFile) {
      val row =
      try{
        Some(ois.readObject.asInstanceOf[Row])
      }catch{
        case e:java.io.IOException =>
          if (e.getMessage != null) println(e.getMessage)
          None
      }
      row match {
        case Some(r) => rowList = r::rowList
        case None =>
          ois.close
          endOfFile = true
      }
    }
    rowList.iterator
  }

}
