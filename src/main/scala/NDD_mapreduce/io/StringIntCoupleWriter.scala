package NDD_mapreduce.io

import NDD_mapreduce.{Row, StringIntCouple}

import java.io.{BufferedWriter, FileWriter}

/**
 * Serializes StringIntCouple objects
 */
object StringIntCoupleWriter extends RowWriter {
  def writeCSV(rows : Iterator[Row], pathToFile: String, delimiter: String) : Boolean = {
    val outputFile = new BufferedWriter(new FileWriter(pathToFile))
    outputFile.write("str"+delimiter+"num")
    outputFile.newLine()
    for (row <- rows){
      outputFile.write(row.asInstanceOf[StringIntCouple].str + delimiter + row.asInstanceOf[StringIntCouple].num)
      outputFile.newLine()
    }
    outputFile.close()
    true
  }

}


