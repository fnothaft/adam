package org.bdgenomics.adam.rdd

import org.apache.spark.{SparkContext, Partitioner}
import org.bdgenomics.adam.models.ReferenceRegion

import scala.reflect.ClassTag

import scala.reflect.ClassTag

import org.bdgenomics.adam.rdd.ADAMContext

import org.bdgenomics.adam.rdd.ADAMContext._

import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}



/**
 * Created by DevinPetersohn on 10/5/16.
 */
trait SortedGenomicRDD[T, U <: SortedGenomicRDD[T, U]] extends GenomicRDD[T, U] {

  val ac = new ADAMContext(new SparkContext())
  val x = ac.loadBam("/data/recompute/alignments/NA12878.bam.aln.bam")


  val elements: Long = rdd.count

  val partitions: Int = 16

  def repartitionByGenomicCoordinate()(implicit c: ClassTag[T]) = {
    val partitionedRDD = rdd.map(f => (getReferenceRegions(f), f))
      .partitionBy(new GenomicPositionRangePartitioner(partitions, elements.toInt))
      .map(f => f._2)
    replaceRdd(partitionedRDD)
  }

  def wellBalancedRepartitionByGenomicCoordinate()(implicit c: ClassTag[T]) = {
    val partitionedRDD = rdd.map(f => (getReferenceRegions(f), f))
      .partitionBy(new GenomicPositionRangePartitioner(partitions, elements.toInt))
      .map(f => f._2)
    val average = partitionTupleCounts.sum / partitionTupleCounts.size
    for(i <- 0 until partitions) {
      if(partitionTupleCounts(i) > 1.5 * average) {
        println("Partition " + i + " contains 50% more than the average -> " + partitionTupleCounts(i) / average)
      } else if(partitionTupleCounts(i) > 1.5 * average) {
        println("Partition " + i + " contains 50% less than the average -> " + partitionTupleCounts(i) / average)
      }
    }
    replaceRdd(partitionedRDD)
  }

  var partitionTupleCounts: Array[Int] = Array.fill[Int](partitions)(0)

  private class GenomicPositionRangePartitioner[V](partitions: Int, elements: Int) extends Partitioner {

    override def numPartitions: Int = partitions

    def getRegionPartition(key: ReferenceRegion): Int = {
      val partitionNumber = key.start.toInt * partitions / elements
      partitionTupleCounts(partitionNumber) += 1
      partitionNumber
    }

    def getPartition(key: Any): Int = {
      key match {
        case f: ReferenceRegion => {
          getRegionPartition(f)
        }
        case _ => throw new Exception("Reference Region Key require to partition on Genomic Position")
      }
    }
  }

}