package NDD_mapreduce.io

import NDD_mapreduce.Row

import java.io.{FileOutputStream, ObjectOutputStream}

/**
 * Serialize a Row iterator, in the form of a CSV file or using scala serialization
 */
trait RowWriter {
  /**
   * Serialize a Row iterator, in the form of a CSV file, 1 CSV row corresponds to 1 Row object
   * @param rows the Row iterator to serialize
   * @param pathToFile the path to the output CSV file, for instance '/home/toto/myrows.csv'
   * @param delimiter the delimiter to use (for instance ",")
   * @return true iff everything is ok
   */
  def writeCSV(rows : Iterator[Row], pathToFile: String, delimiter: String) : Boolean

  /**
   * Serialize a Row iterator, using scala serialization
   * @param rows the Row iterator to serialize
   * @param pathToFile the path to the output file, for instance '/home/toto/myrows'
   * @return true iff everything is ok
   */
  def writeSerializedRows(rows : Iterator[Row], pathToFile: String): Boolean = {
    val oos = new ObjectOutputStream(new FileOutputStream(pathToFile))

    try {
        while (rows.hasNext) {
          oos.writeObject(rows.next())
        }
        true
    } catch {
        case e: Exception => false
    }
  }
}
