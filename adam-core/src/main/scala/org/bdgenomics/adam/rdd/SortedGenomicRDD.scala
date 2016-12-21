package org.bdgenomics.adam.rdd

import org.apache.avro.generic.IndexedRecord
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.formats.avro.Contig
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Created by DevinPetersohn on 10/5/16.
 */

object SortedGenomicRDD {
  /**
   * This function will implicitly split apart the original type from the Mixed-In class
   *
   * @param o The Mixed-In object to get the original type
   * @return The un-mixed object
   */
  implicit def innerObj[T, U <: GenomicRDD[T, U]](o: SortedGenomicRDDMixIn[T, U]): U = o.obj

  /**
   * Creates a Mixed-In object with any U (that extends GenomicRDD).
   *
   * @param o The object to be Mixed-In
   * @return A new Mixed-In object
   */
  def addSortedTrait[T, U <: GenomicRDD[T, U]](o: U, partitionMapRDD: RDD[(ReferenceRegion, ReferenceRegion)]) =
    new SortedGenomicRDDMixIn[T, U](o, partitionMapRDD)

  /**
   * This class is how we tell that a given object (that extends GenomicRDD) is sorted. We can perform multiple
   * optimizations given this information, so many of the methods in this class provide that logic. We use
   * Mix-Ins to create the object.
   *
   * @param obj The object to Mix-In to the SortedGenomicRDD trait.
   */
  class SortedGenomicRDDMixIn[T, U <: GenomicRDD[T, U]] private[SortedGenomicRDD] (private[SortedGenomicRDD] val obj: U,
                                                                                              val partitionMapRDD: RDD[(ReferenceRegion, ReferenceRegion)])
                                                                                              extends SortedGenomicRDD[T, U] with Serializable {
    override val rdd: RDD[T] = innerObj(this).rdd
    override val sequences: SequenceDictionary = innerObj(this).sequences
    override protected[rdd] def replaceRdd(newRdd: RDD[T]): U = innerObj(this).replaceRdd(newRdd)
    override protected[rdd] def getReferenceRegions(elem: T): Seq[ReferenceRegion] = innerObj(this).getReferenceRegions(elem)
    lazy val partitionMap: Seq[(ReferenceRegion, ReferenceRegion)] = partitionMapRDD.collect

  }

}

trait SortedGenomicRDD[T, U <: GenomicRDD[T, U]] extends GenomicRDD[T, U]
