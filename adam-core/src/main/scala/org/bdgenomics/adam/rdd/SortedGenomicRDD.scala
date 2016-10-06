package org.bdgenomics.adam.rdd

import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.ReferenceRegion

/**
 * Created by DevinPetersohn on 10/5/16.
 */
trait SortedGenomicRDD[T, U <: SortedGenomicRDD[T, U]] extends GenomicRDD {

  val elements: Long = rdd.count

  val partitions: Int = 16

  def repartitionByGenomicCoordinate() = {
    val partitionedRDD = rdd.map(f => (getReferenceRegions(f), f))
      .partitionBy(new GenomicPositionPartitioner(partitions, elements.toInt))
      .map(f => f._2)
    replaceRdd(partitionedRDD)
  }

  def wellBalancedRepartitionByGenomicCoordinate() = {
    val partitionedRDD = rdd.map(f => (getReferenceRegions(f), f))
      .partitionBy(new GenomicPositionPartitioner(partitions, elements.toInt))
      .map(f => f._2)
    replaceRdd(partitionedRDD)
  }
}

private class GenomicPositionPartitioner[V](partitions: Int, elements: Int) extends Partitioner {

  override def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    key match {
      case f: ReferenceRegion => getPartition(f)
      case _                  => throw new Exception("Reference Region Key require to partition on Genomic Position")
    }
  }
}