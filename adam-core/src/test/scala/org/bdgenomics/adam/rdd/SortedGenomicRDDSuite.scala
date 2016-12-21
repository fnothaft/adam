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
      val y = x.repartitionAndSortByGenomicCoordinate(16)
      assert(isSorted(y.SortedTrait.partitionMap))
      val z = x.repartitionAndSortByGenomicCoordinate(16)
      assert(isSorted(z.SortedTrait.partitionMap))
      val arrayRepresentationOfZ = z.rdd.collect
      //verify sort worked
      for (currentArray <- List(y.rdd.collect, z.rdd.collect)) {
        for (i <- currentArray.indices) {
          if (i != 0) assert(arrayRepresentationOfZ(i).getStart > arrayRepresentationOfZ(i - 1).getStart ||
            (arrayRepresentationOfZ(i).getStart == arrayRepresentationOfZ(i - 1).getStart && arrayRepresentationOfZ(i).getEnd >= arrayRepresentationOfZ(i - 1).getEnd))
        }
      }

      val partitionTupleCounts: Array[Int] = z.rdd.mapPartitions(f => Iterator(f.size)).collect

      val partitionTupleCounts2: Array[Int] = y.rdd.mapPartitions(f => Iterator(f.size)).collect

      assert(partitionTupleCounts.sum == partitionTupleCounts2.sum)

      val b = z.shuffleRegionJoin(x, Some(1)).rdd.collect
      val c = x.shuffleRegionJoin(z).rdd.collect
      assert(b.length == c.length)

      val d = z.fullOuterShuffleRegionJoin(x, Some(1)).rdd.collect
      val e = x.fullOuterShuffleRegionJoin(z).rdd.collect
      assert(d.length == e.length)

      val f = z.rightOuterShuffleRegionJoin(x, Some(1)).rdd.collect
      val g = z.rightOuterShuffleRegionJoin(x).rdd.collect
      assert(f.length == g.length)

      val h = z.leftOuterShuffleRegionJoin(x, Some(1)).rdd.collect
      val i = z.leftOuterShuffleRegionJoin(x).rdd.collect
      assert(h.length == i.length)

      z.save("/Users/DevinPetersohn/Downloads/testOut1.txt", isSorted = true)
      val t = sc.loadParquetAlignments("/Users/DevinPetersohn/Downloads/testOut1.txt")
      println(t.SortedTrait.isSorted)
      val j = t.shuffleRegionJoin(x, Some(1))
      val k = x.shuffleRegionJoin(t, Some(16))
      assert(j.rdd.collect.length == k.rdd.collect.length)
    }
  }
}
