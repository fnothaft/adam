package org.bdgenomics.adam.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.rdd.SortedGenomicRDD.SortedGenomicRDDMixIn
import org.bdgenomics.formats.avro.Sample
import scala.collection.mutable.{ ListBuffer, ArrayBuffer }
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
  def addSortedTrait[T, U <: GenomicRDD[T, U]](o: U, partitionMap: Array[(ReferenceRegion, ReferenceRegion)]) =
    new SortedGenomicRDDMixIn[T, U](o, partitionMap)

  /**
   * This class is how we tell that a given object (that extends GenomicRDD) is sorted. We can perform multiple
   * optimizations given this information, so many of the methods in this class provide that logic. We use
   * Mix-Ins to create the object.
   *
   * @param obj The object to Mix-In to the SortedGenomicRDD trait.
   */
  final case class SortedGenomicRDDMixIn[T, U <: GenomicRDD[T, U]] private[SortedGenomicRDD] (private[SortedGenomicRDD] val obj: U,
                                                                                              val partitionMap: Array[(ReferenceRegion, ReferenceRegion)])
      extends SortedGenomicRDD[T, U] {
    override val rdd: RDD[T] = innerObj(this).rdd
    override val sequences: SequenceDictionary = innerObj(this).sequences
    override protected[rdd] def replaceRdd(newRdd: RDD[T]): U = innerObj(this).replaceRdd(newRdd)
    override protected[rdd] def getReferenceRegions(elem: T): Seq[ReferenceRegion] = innerObj(this).getReferenceRegions(elem)

    /**
     * This repartition method repartitions all data in this.rdd and distributes it as evenly as possible
     * into the number of partitions provided. The data in this.rdd is already sorted (by ReferenceRegion.start)
     * so we ensure that it is kept that way as we move data around.
     *
     * Data is repartitioned in lists to maintain the sorted order. After the repartition, the lists are placed
     * in the correct order based on their original sorted order.
     *
     * @param partitions the number of partitions to repartition this.rdd into
     * @return a new SortedGenomicRDDMixIn with the RDD partitioned evenly
     */
    def evenlyRepartition(partitions: Int)(implicit tTag: ClassTag[T]): SortedGenomicRDDMixIn[T, U] = {
      // we want to know exactly how much data is on each node
      val partitionTupleCounts: Array[Int] = rdd.mapPartitions(f => Iterator(f.size)).collect
      // the average number of records on each node will help us evenly repartition
      val average: Double = partitionTupleCounts.sum.asInstanceOf[Double] / partitions
      // we already have a sorted rdd, so let's just move the data
      // but we want to move it in blocks to maintain order
      // zipWithIndex seems to be the cheapest way to guarantee this
      val finalPartitionedRDD = rdd.zipWithIndex
        .mapPartitionsWithIndex((idx, iter) => {
          getBalancedPartitionNumber(iter.map(_.swap), partitionMap(idx), average)
        }, preservesPartitioning = true)
        .partitionBy(new GenomicPositionRangePartitioner(partitions)) //should we make a new partitioner for this trait?
        .mapPartitions(iter => {
          // trying to avoid iterator access issues
          val listRepresentation = iter.map(_._2).toList.sortBy(_._2.head._1)
          Iterator(((listRepresentation.head._1._1, listRepresentation.last._1._2), listRepresentation.map(f => f._2)))
        }, preservesPartitioning = true)
      val updatedPartitionMap = finalPartitionedRDD.keys.collect
      addSortedTrait(replaceRdd(finalPartitionedRDD.flatMap(f => f._2.flatten.map(f => f._2))), updatedPartitionMap)
    }

    def inferCorrectReferenceRegionsForPartition(iter: Iterator[(Long, T)], partitionMap: (ReferenceRegion, ReferenceRegion)): Iterator[(Long, (ReferenceRegion, T))] = {
      val listRepresentation = iter.toList.map(f => (f._1, (getReferenceRegions(f._2) /*.filter(g => List(g, partitionMap._1).sorted.head == partitionMap._1 && List(g, partitionMap._2).sorted.last == g)*/ .sorted, f._2)))
      val listBuffer = new ListBuffer[(Long, (ReferenceRegion, T))]
      for (i <- listRepresentation.indices) {
        try {
          if (i == 0) listBuffer += ((listRepresentation(i)._1, (listRepresentation(i)._2._1.head, listRepresentation(i)._2._2)))
          else {
            val previousReferenceRegion = listBuffer.last._2._1
            var j = 0
            while (List(listRepresentation(i)._2._1(j), previousReferenceRegion).sorted.head != previousReferenceRegion) {
              j += 1
            }
            listBuffer += ((listRepresentation(i)._1, (listRepresentation(i)._2._1(j), listRepresentation(i)._2._2)))
          }
        } catch {
          case e: Exception =>
        }
      }
      listBuffer.toIterator
    }

    /**
     * This is a function that splits each partition (iter) into lists keyed with the destination
     * partition number. It preserves the order of the data received and thus is good for moving
     * around sorted data. Each partition is given the same amount of data.
     *
     * @param iter The partition data. (k,v) where k is the index in the RDD and v is T
     * @param average the average number of tuples on all partitions
     * @return grouped lists keyed with the destination partition number. These lists maintain
     *         their original order
     */
    def getBalancedPartitionNumber(iter: Iterator[(Long, T)], partitionMap: (ReferenceRegion, ReferenceRegion), average: Double): Iterator[(Int, ((ReferenceRegion, ReferenceRegion), List[(Long, T)]))] = {
      // converting to list so we can package data that is going to the same node with a groupBy
      val correctPartition = inferCorrectReferenceRegionsForPartition(iter, partitionMap)
      correctPartition.map(f => ((f._1 / average).toInt, f)).toList.groupBy(_._1).mapValues(f => f.map(_._2)).map(f => (f._1, ((f._2.head._2._1, f._2.last._2._1), f._2.map(g => (g._1, g._2._2))))).toIterator
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
    def coPartitionByGenomicRegion(rddToCoPartitionWith: SortedGenomicRDDMixIn[T, U])(implicit tTag: ClassTag[T]): SortedGenomicRDDMixIn[T, U] = {
      val partitions = rddToCoPartitionWith.partitionMap.length
      val finalPartitionedRDD = rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
        val listRepresentation = inferCorrectReferenceRegionsForPartition(iter.map(_.swap), partitionMap(idx)).toList
        for (partition <- rddToCoPartitionWith.partitionMap.zipWithIndex) yield {
          (partition._2, listRepresentation.filter(f => (f._2._1.start >= partition._1._1.start && f._2._1.start <= partition._1._2.end) ||
            (f._2._1.end >= partition._1._1.start && f._2._1.end <= partition._1._2.end)))
        }
      }.toIterator.filter(_._2.nonEmpty), preservesPartitioning = true)
        .partitionBy(new GenomicPositionRangePartitioner(partitions))
        .mapPartitions(iter => {
          iter.toList.sortBy(_._2.head._1).flatMap(_._2).toIterator
        }, preservesPartitioning = true)

      val newPartitionMap = finalPartitionedRDD.mapPartitions((iter) => {
        if (iter.isEmpty) Iterator()
        else {
          val listRepresentation = iter.toList
          Iterator((listRepresentation.head._2._1, listRepresentation.last._2._1))
        }
      }).collect
      addSortedTrait(replaceRdd(finalPartitionedRDD.values.values), newPartitionMap)
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
                                                                                           implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, X), Z] = {
      // did the user provide a set partition count?
      // if no, we will avoid repartitioning this one
      val partitions = optPartitions.getOrElse(rdd.partitions.length)
      // if the user asked for a different number of partitions than we
      // originally had we need to do a repartition first
      val leftRdd: SortedGenomicRDDMixIn[T, U] = this //evenlyRepartition(partitions)

      val rightRdd: SortedGenomicRDDMixIn[X, Y] =
        genomicRdd match {
          // if both are sorted, great! let's just co-partition the data properly
          case in: SortedGenomicRDDMixIn[X, Y] =>
            println("Both sorted")
            in.coPartitionByGenomicRegion(leftRdd.asInstanceOf[SortedGenomicRDDMixIn[X, Y]])
          // otherwise we will just do a generic range partition and sort first
          // then we can perform our co-partition
          case _ =>
            println("This is sorted, but that is not")
            genomicRdd.repartitionAndSortByGenomicCoordinate(partitions).coPartitionByGenomicRegion(leftRdd.asInstanceOf[SortedGenomicRDDMixIn[X, Y]])
        }
      //    val leftRddReadyToJoin = leftRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
      //      iter.map(f => ((leftRdd.indexedReferenceRegions(f._2.toInt), idx), f._1))
      //    })
      val leftRddReadyToJoin = leftRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
        leftRdd.inferCorrectReferenceRegionsForPartition(iter.map(_.swap), leftRdd.partitionMap(idx)).map(f => ((f._2._1, f._1.toInt), f._2._2))
      })
      //    val rightRddReadytoJoin = rightRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
      //      iter.map(f => ((rightRdd.indexedReferenceRegions(f._2.toInt), idx), f._1))
      //    })
      val rightRddReadyToJoin = rightRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
        rightRdd.inferCorrectReferenceRegionsForPartition(iter.map(_.swap), rightRdd.partitionMap(idx)).map(f => ((f._2._1, f._1.toInt), f._2._2))
      })

      assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

      // what sequences do we wind up with at the end?
      val endSequences = sequences ++ genomicRdd.sequences

      // as sorted
      GenericGenomicRDD[(T, X)](
        InnerShuffleRegionJoin[T, X](endSequences, partitions, rdd.context).joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
        endSequences,
        kv => { getReferenceRegions(kv._1) ++ rightRdd.getReferenceRegions(kv._2) })
        .asInstanceOf[GenomicRDD[(T, X), Z]]
    }
  }
}

trait SortedGenomicRDD[T, U <: GenomicRDD[T, U]] extends GenomicRDD[T, U] {

  var sorted: Boolean = true

}
