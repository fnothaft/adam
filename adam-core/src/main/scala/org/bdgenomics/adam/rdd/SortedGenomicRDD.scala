package org.bdgenomics.adam.rdd

import org.apache.spark.{SparkContext, Partitioner}
import org.bdgenomics.adam.models.ReferenceRegion
import scala.reflect.ClassTag



/**
 * Created by DevinPetersohn on 10/5/16.
 */
trait SortedGenomicRDD[T, U <: GenomicRDD[T, U]] extends GenomicRDD[T, U] {
  val sorted: Boolean = true


}