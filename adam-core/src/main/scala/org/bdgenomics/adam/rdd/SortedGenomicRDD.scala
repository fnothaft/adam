package org.bdgenomics.adam.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.rdd.SortedGenomicRDD.SortedGenomicRDDMixIn
import org.bdgenomics.formats.avro.Sample
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
 * Created by DevinPetersohn on 10/5/16.
 */

object SortedGenomicRDD {
  /**
    * This function will implicitly split apart the original type from the Mixed-In class
    * @param o The Mixed-In object to get the original type
    * @return The un-mixed object
    */
  implicit def innerObj[T, U <: GenomicRDD[T, U]](o: SortedGenomicRDDMixIn[T, U]): U = o.obj

  /**
    * Creates a Mixed-In object with any U (that extends GenomicRDD).
    * @param o The object to be Mixed-In
    * @return A new Mixed-In object
    */
  def addSortedTrait[T, U <: GenomicRDD[T, U]](o: U, indexedReferenceRegions: Seq[ReferenceRegion], partitionMap: Array[(Long, Long)]) =
    new SortedGenomicRDDMixIn[T, U](o, indexedReferenceRegions, partitionMap)

  /**
    * This class is how we tell that a given object (that extends GenomicRDD) is sorted. We can perform multiple
    * optimizations given this information, so many of the methods in this class provide that logic. We use
    * Mix-Ins to create the object.
    * @param obj The object to Mix-In to the SortedGenomicRDD trait.
    */
  final case class SortedGenomicRDDMixIn[T, U <: GenomicRDD[T, U]] private[SortedGenomicRDD](private[SortedGenomicRDD] val obj: U,
                                                                                             val indexedReferenceRegions: Seq[ReferenceRegion],
                                                                                             val partitionMap: Array[(Long, Long)])
                                                                                                    extends SortedGenomicRDD[T, U] {
    override val rdd: RDD[T] = innerObj(this).rdd
    override val sequences: SequenceDictionary = innerObj(this).sequences
    override protected[rdd] def replaceRdd(newRdd: RDD[T]): U = innerObj(this).replaceRdd(newRdd)
    override protected[rdd] def getReferenceRegions(elem: T): Seq[ReferenceRegion] = innerObj(this).getReferenceRegions(elem)
//    val partitionMap: Array[(Int, (Long, Long))] = rdd.map(getReferenceRegions).mapPartitionsWithIndex((idx, iter) => {
//                                                      val listRepresentation = iter.toList
//                                                      //this is a bad way of doing things
//                                                      Iterator((idx, (listRepresentation.head.head.start, listRepresentation.last.head.start)))
//                                                    }).collect

    /**
      * This repartition method repartitions all data in this.rdd and distributes it as evenly as possible
      * into the number of partitions provided. The data in this.rdd is already sorted (by ReferenceRegion.start)
      * so we ensure that it is kept that way as we move data around.
      *
      * Data is repartitioned in lists to maintain the sorted order. After the repartition, the lists are placed
      * in the correct order based on their original sorted order.
      * @param partitions the number of partitions to repartition this.rdd into
      * @return a new SortedGenomicRDDMixIn with the RDD partitioned evenly
      */
    def evenlyRepartition(partitions: Int)(implicit tTag: ClassTag[T]): SortedGenomicRDDMixIn[T, U] = {
      //we want to know exactly how much data is on each node
      val partitionTupleCounts: Array[Int] = rdd.mapPartitions(f => Iterator(f.size)).collect
      //the average number of records on each node will help us evenly repartition
      val average: Double = partitionTupleCounts.sum.asInstanceOf[Double] / partitions
      //we already have a sorted rdd, so let's just move the data
      //but we want to move it in blocks to maintain order
      //zipWithIndex seems to be the cheapest way to guarantee this
      val finalPartitionedRDD = rdd.zipWithIndex
        .mapPartitions(iter => {
          getBalancedPartitionNumber(iter.map(_.swap), average)
        }).partitionBy(new GenomicPositionRangePartitioner(partitions)) //should we make a new partitioner for this trait?
        .mapPartitions(iter => {
        //trying to avoid iterator access issues
        val listRepresentation = iter.map(_._2).toList
        var sortedList = new ArrayBuffer[List[(Long, T)]]()
        //we are sorting by the zipped index of the head of the list
        //this is how we maintain the sorted order
        for (listBlock <- listRepresentation.sortBy(_.head._1)) {
          sortedList += listBlock
        }
        //and like magic, our partition is sorted
        sortedList.flatten.toIterator
      })
      val partitionMap = finalPartitionedRDD.mapPartitions((iter) => {
        val listRepresentation = iter.toList
        Iterator((listRepresentation.head._1, listRepresentation.last._1))
      }).collect
      addSortedTrait(replaceRdd(finalPartitionedRDD.map(_._2)), indexedReferenceRegions, partitionMap)
    }

    /**
      * This is a function that splits each partition (iter) into lists keyed with the destination
      * partition number. It preserves the order of the data received and thus is good for moving
      * around sorted data. Each partition is given the same amount of data.
      * @param iter The partition data. (k,v) where k is the index in the RDD and v is T
      * @param average the average number of tuples on all partitions
      * @return grouped lists keyed with the destination partition number. These lists maintain
      *         their original order
      */
    private def getBalancedPartitionNumber(iter: Iterator[(Long, T)], average: Double): Iterator[(Int, List[(Long, T)])] = {
      //converting to list so we can package data that is going to the same node with a groupBy
      val listRepresentation = iter.toList
      //simple division to get the partition number
      listRepresentation.map(f => ((f._1 / average).asInstanceOf[Int], (f._1, f._2)))
        //groupBy will preserve our value order, but we don't want a map
        //so we convert it back to an iterator
        .groupBy(_._1).mapValues(f => f.map(_._2)).toIterator
    }

    /**
      * The purpose of this method is to co-partition two RDDs according to their ReferenceRegions.
      * It is useful during ShuffleRegionJoins because it guarantees no extra shuffling to get correct
      * and complete region joins. RDD is already sorted coming in, so we take precaution to keep it
      * that way.
      *
      * The simplest way of sending data to the correct node is to just send everything that has a
      * start position that could fall within the boundaries
      *
      * This is best used under the condition that (repeatedly) repartitioning is more expensive than
      * calculating the proper location of the records of this.rdd. It requires a pass through the
      * co-located RDD to get the correct partition(s) for each record. It will assign a record to
      * multiple partitions if necessary.
      *
      * @param rddToCoPartitionWith the rdd that this.rdd will be copartitioned with
      * @return returns the newly repartitioned rdd that is co-partitioned with rddToCoPartitionWith
      */
    def coPartitionByGenomicRegion(rddToCoPartitionWith: SortedGenomicRDDMixIn[T, U]): SortedGenomicRDDMixIn[T, U] = {
      val destinationPartitionIndexRange = rddToCoPartitionWith.partitionMap.map(f =>
        (indexedReferenceRegions(f._1.toInt).start, indexedReferenceRegions.slice(f._1.toInt, f._2.toInt).map(_.end).max))
      val partitions = rddToCoPartitionWith.rdd.partitions.length

      val finalPartitionedRDD = rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
        destinationPartitionIndexRange.zipWithIndex.flatMap {
          case (((start: Long, end: Long), index: Int)) => {
            Iterator((index, iter.map(f => (indexedReferenceRegions(f._2.toInt).start, f._1))
              .filter(f => f._1 >= start && f._1 <= end)
              ))
            }
          }.toIterator
        }, preservesPartitioning = true)
        .partitionBy(new GenomicPositionRangePartitioner(partitions))
        .mapPartitions(iter => {
          val listRepresentation = iter.map(_._2.toArray).toList
          var sortedList = new ArrayBuffer[Array[(Long, T)]]()
          //we are sorting by the zipped index of the head of the list
          //this is how we maintain the sorted order
          for (listBlock <- listRepresentation.sortBy(_.head._1)) {
            sortedList += listBlock
          }
          //and like magic, our partition is sorted
          sortedList.flatten.toIterator
        }, preservesPartitioning = true)

      val partitionMap = finalPartitionedRDD.mapPartitions((iter) => {
        val listRepresentation = iter.toList
        Iterator((listRepresentation.head._1, listRepresentation.last._1))
      }).collect
      addSortedTrait(replaceRdd(finalPartitionedRDD.map(_._2)), indexedReferenceRegions, partitionMap)
    }

    /**
      * Performs a sort-merge inner join between this RDD and another RDD.
      *
      * In a sort-merge join, both RDDs are co-partitioned and sorted. The
      * partitions are then zipped, and we do a merge join on each partition.
      * The key equality function used for this join is the reference region
      * overlap function. Since this is an inner join, all values who do not
      * overlap a value from the other RDD are dropped.
      *
      * @param genomicRdd The right RDD in the join.
      * @param optPartitions An Option to give the number of partitions you would like
      * @return Returns a new genomic RDD containing all pairs of keys that
      *   overlapped in the genomic coordinate space.
      */
    override def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                          optPartitions: Option[Int] = None)(
                                                                                          implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                          GenomicRDD[(T, X), Z] = {
      // did the user provide a set partition count?
      // if no, we will avoid repartitioning this one
      val partitions = optPartitions.getOrElse(rdd.partitions.length)
      val leftSidePartitionCount = rdd.partitions.length
      //if the user asked for a different number of partitions than we
      //originally had we need to do a repartition first
      val leftRdd: SortedGenomicRDDMixIn[T, U] = {
        if(optPartitions.getOrElse(leftSidePartitionCount) != leftSidePartitionCount) evenlyRepartition(partitions)
        else this
      }

      val rightRdd: SortedGenomicRDDMixIn[X, Y] =
        genomicRdd match {
          //if both are sorted, great! let's just co-partition the data properly
          case in: SortedGenomicRDDMixIn[X, Y] =>
            println("Both sorted")
            in.coPartitionByGenomicRegion(this.asInstanceOf[SortedGenomicRDDMixIn[X, Y]])
          //otherwise we will just do a generic range partition and sort first
          // then we can perform our co-partition
          case _ =>
            println("This is sorted, but that is not")
            genomicRdd.repartitionAndSortByGenomicCoordinate(partitions).coPartitionByGenomicRegion(this.asInstanceOf[SortedGenomicRDDMixIn[X, Y]])
        }

      val leftRddReadyToJoin = leftRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
        iter.map(f => ((indexedReferenceRegions(f._2.toInt), idx), f._1))
      })
      val rightRddReadytoJoin = rightRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
        iter.map(f => ((indexedReferenceRegions(f._2.toInt), idx), f._1))
      })
      // what sequences do we wind up with at the end?
      val endSequences = sequences ++ genomicRdd.sequences

      // key the RDDs and join
      GenericGenomicRDD[(T, X)](
        InnerShuffleRegionJoin[T, X](endSequences, partitions, rdd.context).joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadytoJoin),
        endSequences,
        kv => { getReferenceRegions(kv._1) ++ rightRdd.getReferenceRegions(kv._2) })
        .asInstanceOf[GenomicRDD[(T, X), Z]]
    }
  }
}

trait SortedGenomicRDD[T, U <: GenomicRDD[T, U]] extends GenomicRDD[T, U] {

  var sorted: Boolean = true

}
