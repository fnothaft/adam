package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.misc.SparkFunSuite

/**
 * Created by DevinPetersohn on 10/8/16.
 */
class SortedGenomicRDDSuite extends SparkFunSuite {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def isSorted(list: Seq[(ReferenceRegion, ReferenceRegion)]): Boolean = {
    val test = list.drop(1).map(_._1)
    val test2 = list.dropRight(1).map(_._2)
    !test2.zip(test).exists(f => f._1.start > f._2.start && f._1.end > f._2.end && f._1.referenceName > f._2.referenceName)
  }

  sparkTest("testing partitioner") {
    time {
      //val x = sc.loadBam("/data/recompute/alignments/NA12878.bam.aln.bam")
      val x = sc.loadBam(ClassLoader.getSystemClassLoader.getResource("bqsr1.sam").getFile)
      println(x.rdd.partitions.length)
      val y = x.repartitionAndSortByGenomicCoordinate(16)
      assert(isSorted(y.partitionMap))
      println("Uneven: " + y.rdd.mapPartitions(f => Iterator(f.size)).collect.mkString(","))
      println("Partitions of Uneven: " + y.rdd.partitions.length)
      val z = x.wellBalancedRepartitionByGenomicCoordinate(16)
      assert(isSorted(z.partitionMap))
      val arrayRepresentationOfZ = z.rdd.collect
      //verify sort worked
      for (currentArray <- List(y.rdd.collect, z.rdd.collect)) {
        for (i <- currentArray.indices) {
          if (i != 0) assert(arrayRepresentationOfZ(i).getStart > arrayRepresentationOfZ(i - 1).getStart ||
            (arrayRepresentationOfZ(i).getStart == arrayRepresentationOfZ(i - 1).getStart && arrayRepresentationOfZ(i).getEnd >= arrayRepresentationOfZ(i - 1).getEnd))
        }
      }

      println("Printing class:")
      println(z.getClass)
      println(x.toCoverage(true).rdd.first)

      //      x.shuffleRegionJoin(y)
      //      z.shuffleRegionJoin(y)
      //      y.shuffleRegionJoin(z)

      val partitionTupleCounts: Array[Int] = z.rdd.mapPartitions(f => Iterator(f.size)).collect
      println(partitionTupleCounts.mkString(","))
//      val d = x.shuffleRegionJoinAndGroupByLeft(y)
//      d.rdd.count
//      println(d.rdd.first)
      val a = z.evenlyRepartition(200)
      val partitionTupleCounts2: Array[Int] = a.rdd.mapPartitions(f => Iterator(f.size)).collect
      println(partitionTupleCounts2.mkString(","))

      assert(partitionTupleCounts.sum == partitionTupleCounts2.sum)

      val b = z.shuffleRegionJoin(x, Some(1)).rdd.collect
      val c = x.shuffleRegionJoin(z).rdd.collect
      println("B length: " + b.length + "\t" + "C length: " + c.length)
      //      assert(b.length == c.length)

      //      for (i <- b.indices) {
      //        assert(b(i) == c(i))
      //      }

    }
  }
}
