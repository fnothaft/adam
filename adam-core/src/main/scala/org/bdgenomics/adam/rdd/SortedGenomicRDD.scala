package org.bdgenomics.adam.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.formats.avro.Sample
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by DevinPetersohn on 10/5/16.
 */

trait SortedGenomicRDD[T, U <: GenomicRDD[T, U]] extends GenomicRDD[T, U] {

  var sorted: Boolean = true

}

object SortedGenomicRDD {
  implicit def innerObj[T, U <: GenomicRDD[T, U]](o: MixTest[T, U]): U = o.obj
  def ::[T, U <: GenomicRDD[T, U]](o: U) = new MixTest[T, U](o)
  final case class MixTest[T, U <: GenomicRDD[T, U]] private[SortedGenomicRDD](val obj: U) extends SortedGenomicRDD[T, U] {
    override val rdd: RDD[T] = {
      println(innerObj(this).rdd.count)
      innerObj(this).rdd
    }
    override val sequences: SequenceDictionary = innerObj(this).sequences
    override protected def replaceRdd(newRdd: RDD[T]): U = innerObj(this)
    override protected def getReferenceRegions(elem: T): Seq[ReferenceRegion] = Seq()
  }
}