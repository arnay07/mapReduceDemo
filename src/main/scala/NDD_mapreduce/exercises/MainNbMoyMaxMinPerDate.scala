package NDD_mapreduce.exercises


import NDD_mapreduce.MainProcess
import NDD_mapreduce.io.StringIntCoupleWriter

/**
 * execution  of a MapReduce job with map and reduce given by the [[NbMoyMaxMinStreamsPerDate]] class
 * @author Arnaud Kaderi
 */
object MainNbMoyMaxMinPerDate extends MainProcess(NbMoyMaxMinStreamsPerDate, StringIntCoupleWriter){}