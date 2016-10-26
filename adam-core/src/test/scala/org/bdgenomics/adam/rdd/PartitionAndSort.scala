package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.misc.SparkFunSuite

/**
 * Created by DevinPetersohn on 10/8/16.
 */
class PartitionAndSort extends SparkFunSuite {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  sparkTest("testing partitioner") {
    time {
      //val x = sc.loadBam("/data/recompute/alignments/NA12878.bam.aln.bam")
      val x = sc.loadBam("/Users/DevinPetersohn/software_builds/adam/adam-core/src/test/resources/bqsr1.sam")
      println(x.rdd.first)
      val y = x.repartitionAndSortByGenomicCoordinate(16)
      val z = x.wellBalancedRepartitionByGenomicCoordinate(16)
      println("Printing class:")
      println(z.getClass)
      println(x.rdd.first)
      println(x.toCoverage(true).rdd.first)

//      x.shuffleRegionJoin(y)
//      z.shuffleRegionJoin(y)
//      y.shuffleRegionJoin(z)

      val partitionTupleCounts: Array[Int] = z.rdd.mapPartitions(f => Iterator(f.size)).collect
      println(partitionTupleCounts.mkString(","))

      val a = z.evenlyRepartition(200)
      val partitionTupleCounts2: Array[Int] = a.rdd.mapPartitions(f => Iterator(f.size)).collect
      println(partitionTupleCounts2.mkString(","))

      assert(partitionTupleCounts.sum == partitionTupleCounts2.sum)

      val average = partitionTupleCounts.sum.asInstanceOf[Double] / partitionTupleCounts.length.asInstanceOf[Double]
      for (i <- partitionTupleCounts.indices) {
        if (partitionTupleCounts(i) > 1.4 * average) {
          println("Partition " + i + " contains > 140% of the average -> " + partitionTupleCounts(i) / average)
        } else if (partitionTupleCounts(i) < 0.6 * average) {
          println("Partition " + i + " contains < 60% of the average -> " + partitionTupleCounts(i) / average)
        }
      }
    }
  }
}
