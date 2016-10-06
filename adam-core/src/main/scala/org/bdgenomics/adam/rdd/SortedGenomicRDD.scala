package org.bdgenomics.adam.rdd

import scala.reflect.ClassTag

/**
 * Created by DevinPetersohn on 10/5/16.
 */
trait SortedGenomicRDD[T, U <: SortedGenomicRDD[T, U]] extends GenomicRDD[T, U] {

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
    replaceRdd(partitionedRDD)
  }
}

