package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.misc.SparkFunSuite

/**
  * Created by DevinPetersohn on 10/8/16.
  */
class PartitionAndSort extends SparkFunSuite {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  sparkTest("testing partitioner") {
    time {
      //val x = sc.loadBam("/data/recompute/alignments/NA12878.bam.aln.bam")
      val x = sc.loadBam("/Users/DevinPetersohn/Desktop/adam/adam-core/src/test/resources/bqsr1.sam")
      println(x.rdd.first)
      x.wellBalancedRepartitionByGenomicCoordinate()
    }
  }
}
