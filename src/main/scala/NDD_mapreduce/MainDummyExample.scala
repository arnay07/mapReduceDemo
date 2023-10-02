package NDD_mapreduce

import NDD_mapreduce.io.StringIntCoupleWriter

/**
 * execution  of a MapReduce job with map and reduce given by the [[DummyExample]] class
 * @author Anne-Cecile Caron
 */
object MainDummyExample extends MainProcess(DummyExample, StringIntCoupleWriter){}
