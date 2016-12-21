/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd

import java.util.concurrent.Executors

import org.apache.avro.generic.IndexedRecord
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.{Partitioner, SparkFiles}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{RecordGroupDictionary, ReferenceRegion, SequenceDictionary}
import org.bdgenomics.formats.avro.{Contig, RecordGroupMetadata, Sample}
import org.bdgenomics.utils.cli.SaveArgs
import org.json4s.jackson.Json

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private[rdd] class JavaSaveArgs(var outputPath: String,
                                var blockSize: Int = 128 * 1024 * 1024,
                                var pageSize: Int = 1 * 1024 * 1024,
                                var compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                                var disableDictionaryEncoding: Boolean = false,
                                var asSingleFile: Boolean = false) extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
  var deferMerging = false
}

private[rdd] object GenomicRDD {

  /**
   * Replaces file references in a command.
   *
   * @see pipe
   * @param cmd Command to split and replace references in.
   * @param files List of paths to files.
   * @return Returns a split up command string, with file paths subbed in.
   */
  def processCommand(cmd: String,
                     files: Seq[String]): List[String] = {
    val filesWithIndex = files.zipWithIndex
      .map(p => {
        val (file, index) = p
        ("$%d".format(index), file)
      }).reverse

    @tailrec def replaceEscapes(cmd: String,
                                iter: Iterator[(String, String)]): String = {
      if (!iter.hasNext) {
        cmd
      } else {
        val (idx, file) = iter.next
        val newCmd = cmd.replace(idx, file)
        replaceEscapes(newCmd, iter)
      }
    }

    cmd.split(" ")
      .map(s => {
        replaceEscapes(s, filesWithIndex.toIterator)
      }).toList
  }
}

trait GenomicRDD[T, U <: GenomicRDD[T, U]] {

  val rdd: RDD[T]

  val sequences: SequenceDictionary

  lazy val jrdd: JavaRDD[T] = {
    rdd.toJavaRDD()
  }

  def transform(tFn: RDD[T] => RDD[T]): U = {
    replaceRdd(tFn(rdd))
  }
  val self = this

  /**
    *
    */
  private[rdd] object SortedTrait extends Serializable {
    var isSorted: Boolean = false
    var partitionMapRdd: RDD[(ReferenceRegion, ReferenceRegion)] = null
    lazy val partitionMap: Seq[(ReferenceRegion, ReferenceRegion)] = partitionMapRdd.collect

    def apply(newRdd: RDD[T], newPartitionMapRDD: RDD[(ReferenceRegion, ReferenceRegion)]): U = {
      val newObject = replaceRdd(newRdd)
      newObject.SortedTrait.isSorted = true
      newObject.SortedTrait.partitionMapRdd = newPartitionMapRDD
      newObject
    }

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
    private def evenlyRepartition(partitions: Int)(implicit tTag: ClassTag[T]): GenomicRDD[T, U] = {
      if(!isSorted) return self
      val collectedPartitionMap = partitionMap
      // we want to know exactly how much data is on each node
      val partitionTupleCounts: Array[Int] = rdd.mapPartitions(f => Iterator(f.size)).collect
      // the average number of records on each node will help us evenly repartition
      val average: Double = partitionTupleCounts.sum.asInstanceOf[Double] / partitions
      // we already have a sorted rdd, so let's just move the data
      // but we want to move it in blocks to maintain order
      // zipWithIndex seems to be the cheapest way to guarantee this
      val finalPartitionedRDD = rdd.zipWithIndex
        .mapPartitionsWithIndex((idx, iter) => {
          getBalancedPartitionNumber(iter.map(_.swap), collectedPartitionMap(idx), average)
        }, preservesPartitioning = true)
        .partitionBy(new GenomicPositionRangePartitioner(partitions)) //should we make a new partitioner for this trait?
        .mapPartitions(iter => {
        // trying to avoid iterator access issues
        val listRepresentation = iter.map(_._2).toList.sortBy(_._2.head._1)
        Iterator(((listRepresentation.head._1._1, listRepresentation.last._1._2), listRepresentation.map(f => f._2)))
      }, preservesPartitioning = true)

      SortedTrait(finalPartitionedRDD.flatMap(f => f._2.flatten.map(f => f._2)), finalPartitionedRDD.keys)
    }

    /**
      * Infers the correct reference region for each T given a the bounds of the partition (partitionMap)
      * and returns the iterator with each T paired with the ReferenceRegion that it represents in that
      * sorted location. This is important because a T can map to multiple ReferenceRegions.
      *
      * This version of the method works with getBalancedPartitionNumber
      *
      * @param iter The iterator (that partition's data) with each T zipped
      * @param partitionMap the bounds of the partition where _._1 is the lower bound and _._2 is the
      *                     upper bound
      * @return An iterator of all T's keyed with the specific ReferenceRegion that the T represents in
      *         the global sort, then keyed with the globally zipped position
      */
    private def inferCorrectReferenceRegionsForPartitionWithZip(iter: Iterator[(Long, T)], partitionMap: (ReferenceRegion, ReferenceRegion)): Iterator[(Long, (ReferenceRegion, T))] = {
      //converting to a list and making sure that we only look at values that are in our range
      val listRepresentation = iter.toList.map(f => (f._1, (getReferenceRegions(f._2).filter(g => List(g, partitionMap._1).sorted.head == partitionMap._1 || g == partitionMap._1).sorted, f._2)))
      //for now we are using Listbuffers, in the future something else may be better
      val listBuffer = new ListBuffer[(Long, (ReferenceRegion, T))]

      for (i <- listRepresentation.indices) {
        //the first record on the partition is always the first to be added, and contains the lower bound (or next value after lower bound)
        if (i == 0) {
          listBuffer += ((listRepresentation(i)._1, (listRepresentation(i)._2._1.head, listRepresentation(i)._2._2)))
        }
        else {
          val previousReferenceRegion = listBuffer.last._2._1
          var j = 0
          //we should never run off the end of the list because there will always be at least the same number
          //of regions as there are T's in this partition.
          while (List(listRepresentation(i)._2._1(j), previousReferenceRegion).sorted.head != previousReferenceRegion) {
            j += 1
          }
          listBuffer += ((listRepresentation(i)._1, (listRepresentation(i)._2._1(j), listRepresentation(i)._2._2)))
        }
      }
      listBuffer.toIterator
    }

    /**
      * Infers the correct reference region for each T given a the bounds of the partition (partitionMap)
      * and returns the iterator with each T paired with the ReferenceRegion that it represents in that
      * sorted location. This is important because a T can map to multiple ReferenceRegions.
      *
      * @param iter The iterator (that partition's data) of T objects
      * @param partitionMap the bounds of the partition where _._1 is the lower bound and _._2 is the
      *                     upper bound
      * @return An iterator of all T's keyed with the specific ReferenceRegion that the T represents in
      *         the global sort
      */
    def inferCorrectReferenceRegionsForPartition(iter: Iterator[T], partitionMap: (ReferenceRegion, ReferenceRegion)): Iterator[((ReferenceRegion, T))] = {
      //converting to a list and making sure that we only look at values that are in our range
      val listRepresentation = iter.toList.map(f => (getReferenceRegions(f).filter(g => List(g, partitionMap._1).sorted.head == partitionMap._1 || g == partitionMap._1).sorted, f))
      //for now we are using Listbuffers, in the future something else may be better
      val listBuffer = new ListBuffer[(ReferenceRegion, T)]
      for (i <- listRepresentation.indices) {
        //the first record on the partition is always the first to be added, and contains the lower bound (or next value after lower bound)
        if (i == 0) {
          listBuffer += ((listRepresentation(i)._1.head, listRepresentation(i)._2))
        }
        else {
          val previousReferenceRegion = listBuffer.last._1
          var j = 0
          //we should never run off the end of the list because there will always be at least the same number
          //of regions as there are T's in this partition.
          while (List(listRepresentation(i)._1(j), previousReferenceRegion).sorted.head != previousReferenceRegion) {
            j += 1
          }
          listBuffer += ((listRepresentation(i)._1(j), listRepresentation(i)._2))
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
      val correctPartition = inferCorrectReferenceRegionsForPartitionWithZip(iter, partitionMap)
      correctPartition.map(f => ((f._1 / average).toInt, f)).toList.groupBy(_._1).mapValues(f => f.map(_._2))
        .map(f => (f._1, ((f._2.head._2._1, f._2.last._2._1), f._2.map(g => (g._1, g._2._2))))).toIterator
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
    def coPartitionByGenomicRegion[X, Y <: GenomicRDD[X, Y]](rddToCoPartitionWith: GenomicRDD[X, Y])(implicit tTag: ClassTag[T], xTag: ClassTag[X]): U = {
      assert(rddToCoPartitionWith.SortedTrait.isSorted)
      val collectedDestinationPartitionMap = rddToCoPartitionWith.SortedTrait.partitionMap
      val collectedSourcePartitionMap = partitionMap
      val partitions = collectedDestinationPartitionMap.length
      val finalPartitionedRDD = rdd.mapPartitionsWithIndex((idx, iter) => {
        val listRepresentation = inferCorrectReferenceRegionsForPartition(iter, collectedSourcePartitionMap(idx)).toList
        for (partition <- collectedDestinationPartitionMap.zipWithIndex) yield {
          (partition._2, listRepresentation.filter(f => (f._1.start >= partition._1._1.start && f._1.start <= partition._1._2.end) ||
            (f._1.end >= partition._1._1.start && f._1.end <= partition._1._2.end)))
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
          Iterator((listRepresentation.head._1, listRepresentation.last._1))
        }
      })

      SortedTrait(finalPartitionedRDD.values, newPartitionMap)
    }

    /**
      * All shuffleRegionJoins share the same code to prepare, so this is a helper function that prepares all the data
      * for joining. It starts by repartitioning (if needed) the leftRdd, which is <this>. Next, it copartitions the
      * right Rdd with it. Before returning the RDDs, it puts them in the format that the joins need. The ReferenceRegion
      * is inferred for each T/X.
      *
      * @param leftRdd Usually <this>, unless a repartition had to occur. The repartition only happens in cases where
      *                the number of partitions requested for the join differs from the <this.partitions>.
      * @param genomicRdd The RDD to join to.
      * @return A tuple containing both RDDs ready to join. _._1 is the leftRdd and _._2 is the rightRdd
      */
    private def prepareForShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](leftRdd: GenomicRDD[T, U], genomicRdd: GenomicRDD[X, Y])
                                                                                                 (implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                 ((RDD[((ReferenceRegion, Int), T)], RDD[((ReferenceRegion, Int), X)])) = {
      assert(leftRdd.SortedTrait.isSorted)
      val rightRdd: GenomicRDD[X, Y] =
        // if both are sorted, great! let's just co-partition the data properly
        if(genomicRdd.SortedTrait.isSorted) {
            genomicRdd.SortedTrait.coPartitionByGenomicRegion(leftRdd)
        }
        // otherwise we will co-partition then sort
        else {
            genomicRdd.coPartitionByGenomicRegion(leftRdd)
          }
      assert(rightRdd.SortedTrait.isSorted)

      val leftPmap = leftRdd.SortedTrait.partitionMap
      val leftRddReadyToJoin = leftRdd.rdd.mapPartitionsWithIndex((idx, iter) => {
        leftRdd.SortedTrait.inferCorrectReferenceRegionsForPartition(iter, leftPmap(idx)).map(f => ((f._1, idx), f._2))
      })

      val rightPmap = rightRdd.SortedTrait.partitionMap
      val rightRddReadyToJoin = rightRdd.rdd.mapPartitionsWithIndex((idx, iter) => {
        rightRdd.SortedTrait.inferCorrectReferenceRegionsForPartition(iter, rightPmap(idx)).map(f => ((f._1, idx), f._2))
      })
      (leftRddReadyToJoin, rightRddReadyToJoin)
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
    def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                         optPartitions: Option[Int] = None)(
                                                                                         implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                         GenomicRDD[(T, X), Z] = {
      // did the user provide a set partition count?
      // if no, we will avoid repartitioning this one
      val partitions = optPartitions.getOrElse(rdd.partitions.length)
      val leftRdd: GenomicRDD[T, U] = if(partitions != rdd.partitions.length) evenlyRepartition(partitions) else self
      val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
      val leftRddReadyToJoin = preparedRdds._1
      val rightRddReadyToJoin = preparedRdds._2
      assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

      // what sequences do we wind up with at the end?
      val endSequences = sequences ++ genomicRdd.sequences

      GenericGenomicRDD[(T, X)](
        InnerShuffleRegionJoin[T, X](endSequences, partitions, rdd.context)
          .joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
        endSequences,
        kv => {
          getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2)
        }).asInstanceOf[GenomicRDD[(T, X), Z]]
    }

    /**
      * Performs a sort-merge inner join between this RDD and another RDD,
      * followed by a groupBy on the left value.
      *
      * In a sort-merge join, both RDDs are co-partitioned and sorted. The
      * partitions are then zipped, and we do a merge join on each partition.
      * The key equality function used for this join is the reference region
      * overlap function. Since this is an inner join, all values who do not
      * overlap a value from the other RDD are dropped. In the same operation,
      * we group all values by the left item in the RDD.
      *
      * @param genomicRdd The right RDD in the join.
      * @return Returns a new genomic RDD containing all pairs of keys that
      *   overlapped in the genomic coordinate space, grouped together by
      *   the value they overlapped in the left RDD..
      */
    def shuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Iterable[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                                optPartitions: Option[Int] = None)
                                                                                                                (implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                                GenomicRDD[(T, Iterable[X]), Z] = {
      // did the user provide a set partition count?
      // if no, we will avoid repartitioning this one
      val partitions = optPartitions.getOrElse(rdd.partitions.length)
      val leftRdd: GenomicRDD[T, U] = if(partitions != rdd.partitions.length) evenlyRepartition(partitions) else self
      val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
      val leftRddReadyToJoin = preparedRdds._1
      val rightRddReadyToJoin = preparedRdds._2
      assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

      // what sequences do we wind up with at the end?
      val endSequences = sequences ++ genomicRdd.sequences

      assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

      val joinedRDD = InnerShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin).mapPartitionsWithIndex((idx, iter) => {
        if (iter.isEmpty) Iterator()
        else {
          val listRepresentation = iter.toList
          listRepresentation.groupBy(_._1).mapValues(_.map(_._2).toIterable).toIterator
        }
      }, preservesPartitioning = true)

      GenericGenomicRDD[(T, Iterable[X])](
        joinedRDD,
        endSequences,
        kv => {
          (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
            getReferenceRegions(kv._1)).toSeq
        }).asInstanceOf[GenomicRDD[(T, Iterable[X]), Z]]
    }

    /**
      * Performs a sort-merge right outer join between this RDD and another RDD,
      * followed by a groupBy on the left value, if not null.
      *
      * In a sort-merge join, both RDDs are co-partitioned and sorted. The
      * partitions are then zipped, and we do a merge join on each partition.
      * The key equality function used for this join is the reference region
      * overlap function. In the same operation, we group all values by the left
      * item in the RDD. Since this is a right outer join, all values from the
      * right RDD who did not overlap a value from the left RDD are placed into
      * a length-1 Iterable with a `None` key.
      *
      * @param genomicRdd The right RDD in the join.
      * @return Returns a new genomic RDD containing all pairs of keys that
      *   overlapped in the genomic coordinate space, grouped together by
      *   the value they overlapped in the left RDD, and all values from the
      *   right RDD that did not overlap an item in the left RDD.
      */
    def rightOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Iterable[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                                                   optPartitions: Option[Int] = None)(
                                                                                                                                   implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                                                   GenomicRDD[(Option[T], Iterable[X]), Z] = {
      // did the user provide a set partition count?
      // if no, we will avoid repartitioning this one
      val partitions = optPartitions.getOrElse(rdd.partitions.length)
      val leftRdd: GenomicRDD[T, U] = if(partitions != rdd.partitions.length) evenlyRepartition(partitions) else self
      val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
      val leftRddReadyToJoin = preparedRdds._1
      val rightRddReadyToJoin = preparedRdds._2
      assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

      // what sequences do we wind up with at the end?
      val endSequences = sequences ++ genomicRdd.sequences

      GenericGenomicRDD[(Option[T], Iterable[X])](RightOuterShuffleRegionJoinAndGroupByLeft[T, X](endSequences,
        partitions,
        rdd.context).joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
        endSequences,
        kv => {
          (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
            kv._1.toSeq.flatMap(v => getReferenceRegions(v))).toSeq
        }).asInstanceOf[GenomicRDD[(Option[T], Iterable[X]), Z]]
    }

    /**
      * Performs a sort-merge full outer join between this RDD and another RDD.
      *
      * In a sort-merge join, both RDDs are co-partitioned and sorted. The
      * partitions are then zipped, and we do a merge join on each partition.
      * The key equality function used for this join is the reference region
      * overlap function. Since this is a full outer join, if a value from either
      * RDD does not overlap any values in the other RDD, it will be paired with
      * a `None` in the product of the join.
      *
      * @param genomicRdd The right RDD in the join.
      * @return Returns a new genomic RDD containing all pairs of keys that
      *   overlapped in the genomic coordinate space, and values that did not
      *   overlap will be paired with a `None`.
      */
    def fullOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Option[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                                  optPartitions: Option[Int] = None)(
                                                                                                                  implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                                  GenomicRDD[(Option[T], Option[X]), Z] = {
      // did the user provide a set partition count?
      // if no, we will avoid repartitioning this one
      val partitions = optPartitions.getOrElse(rdd.partitions.length)
      val leftRdd: GenomicRDD[T, U] = if(partitions != rdd.partitions.length) evenlyRepartition(partitions) else self
      val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
      val leftRddReadyToJoin = preparedRdds._1
      val rightRddReadyToJoin = preparedRdds._2
      assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

      // what sequences do we wind up with at the end?
      val endSequences = sequences ++ genomicRdd.sequences

      GenericGenomicRDD[(Option[T], Option[X])](FullOuterShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
        endSequences,
        kv => {
          Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v)),
            kv._1.map(v => getReferenceRegions(v))).flatten.flatten
        }).asInstanceOf[GenomicRDD[(Option[T], Option[X]), Z]]
    }

    /**
      * Performs a sort-merge left outer join between this RDD and another RDD.
      *
      * In a sort-merge join, both RDDs are co-partitioned and sorted. The
      * partitions are then zipped, and we do a merge join on each partition.
      * The key equality function used for this join is the reference region
      * overlap function. Since this is a left outer join, all values in the
      * right RDD that do not overlap a value from the left RDD are dropped.
      * If a value from the left RDD does not overlap any values in the right
      * RDD, it will be paired with a `None` in the product of the join.
      *
      * @param genomicRdd The right RDD in the join.
      * @return Returns a new genomic RDD containing all pairs of keys that
      *   overlapped in the genomic coordinate space, and all keys from the
      *   left RDD that did not overlap a key in the right RDD.
      */
    def leftOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Option[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                          optPartitions: Option[Int] = None)(
                                                                                                          implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                          GenomicRDD[(T, Option[X]), Z] = {
      // did the user provide a set partition count?
      // if no, we will avoid repartitioning this one
      val partitions = optPartitions.getOrElse(rdd.partitions.length)
      val leftRdd: GenomicRDD[T, U] = if(partitions != rdd.partitions.length) evenlyRepartition(partitions) else self
      val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
      val leftRddReadyToJoin = preparedRdds._1
      val rightRddReadyToJoin = preparedRdds._2
      assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

      // what sequences do we wind up with at the end?
      val endSequences = sequences ++ genomicRdd.sequences

      GenericGenomicRDD[(T, Option[X])](LeftOuterShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
        endSequences,
        kv => {
          Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v))).flatten.flatten ++
            getReferenceRegions(kv._1)
        }).asInstanceOf[GenomicRDD[(T, Option[X]), Z]]
    }

    /**
      * Performs a sort-merge right outer join between this RDD and another RDD.
      *
      * In a sort-merge join, both RDDs are co-partitioned and sorted. The
      * partitions are then zipped, and we do a merge join on each partition.
      * The key equality function used for this join is the reference region
      * overlap function. Since this is a right outer join, all values in the
      * left RDD that do not overlap a value from the right RDD are dropped.
      * If a value from the right RDD does not overlap any values in the left
      * RDD, it will be paired with a `None` in the product of the join.
      *
      * @param genomicRdd The right RDD in the join.
      * @return Returns a new genomic RDD containing all pairs of keys that
      *   overlapped in the genomic coordinate space, and all keys from the
      *   right RDD that did not overlap a key in the left RDD.
      */
    def rightOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], X), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                           optPartitions: Option[Int] = None)(
                                                                                                           implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                           GenomicRDD[(Option[T], X), Z] = {
      // did the user provide a set partition count?
      // if no, we will avoid repartitioning this one
      val partitions = optPartitions.getOrElse(rdd.partitions.length)
      val leftRdd: GenomicRDD[T, U] = if(partitions != rdd.partitions.length) evenlyRepartition(partitions) else self
      val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
      val leftRddReadyToJoin = preparedRdds._1
      val rightRddReadyToJoin = preparedRdds._2
      assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

      // what sequences do we wind up with at the end?
      val endSequences = sequences ++ genomicRdd.sequences

      GenericGenomicRDD[(Option[T], X)](RightOuterShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
        endSequences,
        kv => {
          Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
            genomicRdd.getReferenceRegions(kv._2)
        }).asInstanceOf[GenomicRDD[(Option[T], X), Z]]
    }
  }

  /**
   * Pipes genomic data to a subprocess that runs in parallel using Spark.
   *
   * Files are substituted in to the command with a $x syntax. E.g., to invoke
   * a command that uses the first file from the files Seq, use $0.
   *
   * Pipes require the presence of an InFormatterCompanion and an OutFormatter
   * as implicit values. The InFormatterCompanion should be a singleton whose
   * apply method builds an InFormatter given a specific type of GenomicRDD.
   * The implicit InFormatterCompanion yields an InFormatter which is used to
   * format the input to the pipe, and the implicit OutFormatter is used to
   * parse the output from the pipe.
   *
   * @param cmd Command to run.
   * @param files Files to make locally available to the commands being run.
   *   Default is empty.
   * @param environment A map containing environment variable/value pairs to set
   *   in the environment for the newly created process. Default is empty.
   * @param flankSize Number of bases to flank each command invocation by.
   * @return Returns a new GenomicRDD of type Y.
   * @tparam X The type of the record created by the piped command.
   * @tparam Y A GenomicRDD containing X's.
   * @tparam V The InFormatter to use for formatting the data being piped to the
   *   command.
   */
  def pipe[X, Y <: GenomicRDD[X, Y], V <: InFormatter[T, U, V]](cmd: String,
                                                                files: Seq[String] = Seq.empty,
                                                                environment: Map[String, String] = Map.empty,
                                                                flankSize: Int = 0)(implicit tFormatterCompanion: InFormatterCompanion[T, U, V],
                                                                                    xFormatter: OutFormatter[X],
                                                                                    convFn: (U, RDD[X]) => Y,
                                                                                    tManifest: ClassTag[T],
                                                                                    xManifest: ClassTag[X]): Y = {

    // TODO: support broadcasting files
    files.foreach(f => {
      rdd.context.addFile(f)
    })

    // make formatter
    val tFormatter: V = tFormatterCompanion.apply(this.asInstanceOf[U])

    // make bins
    val seqLengths = sequences.records.toSeq.map(rec => (rec.name, rec.length)).toMap
    val totalLength = seqLengths.values.sum
    val bins = GenomeBins(totalLength / rdd.partitions.size, seqLengths)

    // if the input rdd is mapped, then we need to repartition
    val partitionedRdd = if (sequences.records.size > 0) {
      // get region covered, expand region by flank size, and tag with bins
      val binKeyedRdd = rdd.flatMap(r => {

        // get regions and expand
        val regions = getReferenceRegions(r).map(_.pad(flankSize))

        // get all the bins this record falls into
        val recordBins = regions.flatMap(rr => {
          (bins.getStartBin(rr) to bins.getEndBin(rr)).map(b => (rr, b))
        })

        // key the record by those bins and return
        // TODO: this should key with the reference region corresponding to a bin
        recordBins.map(b => (b, r))
      })

      // repartition yonder our data
      // TODO: this should repartition and sort within the partition
      binKeyedRdd.repartitionAndSortWithinPartitions(ManualRegionPartitioner(bins.numBins))
        .values
    } else {
      rdd
    }

    // are we in local mode?
    val isLocal = partitionedRdd.context.isLocal

    // call map partitions and pipe
    val pipedRdd = partitionedRdd.mapPartitions(iter => {

      // get files
      // from SPARK-3311, SparkFiles doesn't work in local mode.
      // so... we'll bypass that by checking if we're running in local mode.
      // sigh!
      val locs = if (isLocal) {
        files
      } else {
        files.map(f => {
          SparkFiles.get(f)
        })
      }

      // split command and create process builder
      val finalCmd = GenomicRDD.processCommand(cmd, locs)
      val pb = new ProcessBuilder(finalCmd)
      pb.redirectError(ProcessBuilder.Redirect.INHERIT)

      // add environment variables to the process builder
      val pEnv = pb.environment()
      environment.foreach(kv => {
        val (k, v) = kv
        pEnv.put(k, v)
      })

      // start underlying piped command
      val process = pb.start()
      val os = process.getOutputStream()
      val is = process.getInputStream()

      // wrap in and out formatters
      val ifr = new InFormatterRunner[T, U, V](iter, tFormatter, os)
      val ofr = new OutFormatterRunner[X, OutFormatter[X]](xFormatter, is)

      // launch thread pool and submit formatters
      val pool = Executors.newFixedThreadPool(2)
      pool.submit(ifr)
      val futureIter = pool.submit(ofr)

      // wait for process to finish
      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw new RuntimeException("Piped command %s exited with error code %d.".format(
          finalCmd, exitCode))
      }

      // shut thread pool
      pool.shutdown()

      futureIter.get
    })

    // build the new GenomicRDD
    val newRdd = convFn(this.asInstanceOf[U], pipedRdd)

    // if the original rdd was aligned and the final rdd is aligned, then we must filter
    if (newRdd.sequences.isEmpty ||
      sequences.isEmpty) {
      newRdd
    } else {
      def filterPartition(idx: Int, iter: Iterator[X]): Iterator[X] = {

        // get the region for this partition
        val region = bins.invert(idx)

        // map over the iterator and filter out any items that don't belong
        iter.filter(x => {

          // get the regions for x
          val regions = newRdd.getReferenceRegions(x)

          // are there any regions that overlap our current region
          !regions.forall(!_.overlaps(region))
        })
      }

      // run a map partitions with index and discard all items that fall outside of their
      // own partition's region bound
      newRdd.transform(_.mapPartitionsWithIndex(filterPartition))
    }
  }

  protected[rdd] def replaceRdd(newRdd: RDD[T]): U

  protected[rdd] def getReferenceRegions(elem: T): Seq[ReferenceRegion]

  protected[rdd] def flattenRddByRegions(): RDD[(ReferenceRegion, T)] = {
    rdd.flatMap(elem => {
      getReferenceRegions(elem).map(r => (r, elem))
    })
  }

  def filterByOverlappingRegion(query: ReferenceRegion): U = {
    replaceRdd(rdd.filter(elem => {

      // where can this item sit?
      val regions = getReferenceRegions(elem)

      // do any of these overlap with our query region?
      regions.exists(_.overlaps(query))
    }))
  }

  /**
   * Performs a broadcast inner join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is an inner join, all values who do not overlap a value from the other
   * RDD are dropped.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def broadcastRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](genomicRdd: GenomicRDD[X, Y])(
    implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, X), Z] = {

    // key the RDDs and join
    GenericGenomicRDD[(T, X)](InnerBroadcastRegionJoin[T, X]().partitionAndJoin(flattenRddByRegions(),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => { getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2) })
      .asInstanceOf[GenomicRDD[(T, X), Z]]
  }

  /**
   * Performs a broadcast right outer join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is a right outer join, all values in the left RDD that do not overlap a
   * value from the right RDD are dropped. If a value from the right RDD does
   * not overlap any values in the left RDD, it will be paired with a `None`
   * in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterBroadcastRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], X), Z]](genomicRdd: GenomicRDD[X, Y])(
    implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], X), Z] = {

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], X)](RightOuterBroadcastRegionJoin[T, X]().partitionAndJoin(flattenRddByRegions(),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
      .asInstanceOf[GenomicRDD[(Option[T], X), Z]]
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
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                              optPartitions: Option[Int] = None)(
                                                                              implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                              GenomicRDD[(T, X), Z] = {
    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)
    val leftRdd = repartitionAndSortByGenomicCoordinate(partitions)
    leftRdd.SortedTrait.shuffleRegionJoin(genomicRdd, optPartitions)
  }

  /**
   * Performs a sort-merge right outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a right outer join, all values in the
   * left RDD that do not overlap a value from the right RDD are dropped.
   * If a value from the right RDD does not overlap any values in the left
   * RDD, it will be paired with a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], X), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                optPartitions: Option[Int] = None)(
                                                                                                implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                GenomicRDD[(Option[T], X), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd = repartitionAndSortByGenomicCoordinate(partitions)
    leftRdd.SortedTrait.rightOuterShuffleRegionJoin(genomicRdd, optPartitions)
  }

  /**
   * Performs a sort-merge left outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a left outer join, all values in the
   * right RDD that do not overlap a value from the left RDD are dropped.
   * If a value from the left RDD does not overlap any values in the right
   * RDD, it will be paired with a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  def leftOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Option[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                               optPartitions: Option[Int] = None)(
                                                                                               implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                               GenomicRDD[(T, Option[X]), Z] = {
    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd = repartitionAndSortByGenomicCoordinate(partitions)
    leftRdd.SortedTrait.leftOuterShuffleRegionJoin(genomicRdd, optPartitions)
  }

  /**
   * Performs a sort-merge full outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a full outer join, if a value from either
   * RDD does not overlap any values in the other RDD, it will be paired with
   * a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and values that did not
   *   overlap will be paired with a `None`.
   */
  def fullOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Option[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                       optPartitions: Option[Int] = None)(
                                                                                                       implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                       GenomicRDD[(Option[T], Option[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd = repartitionAndSortByGenomicCoordinate(partitions)
    leftRdd.SortedTrait.fullOuterShuffleRegionJoin(genomicRdd, optPartitions)
  }

  /**
   * Performs a sort-merge inner join between this RDD and another RDD,
   * followed by a groupBy on the left value.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is an inner join, all values who do not
   * overlap a value from the other RDD are dropped. In the same operation,
   * we group all values by the left item in the RDD.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD..
   */
  def shuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Iterable[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                      optPartitions: Option[Int] = None)(
                                                                                                      implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                      GenomicRDD[(T, Iterable[X]), Z] = {
    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd = repartitionAndSortByGenomicCoordinate(partitions)
    leftRdd.SortedTrait.shuffleRegionJoinAndGroupByLeft(genomicRdd, optPartitions)
  }

  /**
   * Performs a sort-merge right outer join between this RDD and another RDD,
   * followed by a groupBy on the left value, if not null.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. In the same operation, we group all values by the left
   * item in the RDD. Since this is a right outer join, all values from the
   * right RDD who did not overlap a value from the left RDD are placed into
   * a length-1 Iterable with a `None` key.
   *
   * @param genomicRdd The right RDD in the join.
   * @param optPartitions Optional parameter for providing number of partitions
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD, and all values from the
   *   right RDD that did not overlap an item in the left RDD.
   */
  def rightOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Iterable[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                      optPartitions: Option[Int] = None)(
                                                                                                      implicit tTag: ClassTag[T], xTag: ClassTag[X]):
                                                                                                      GenomicRDD[(Option[T], Iterable[X]), Z] = {
    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd = repartitionAndSortByGenomicCoordinate(partitions)
    leftRdd.SortedTrait.rightOuterShuffleRegionJoinAndGroupByLeft(genomicRdd, optPartitions)
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
  def coPartitionByGenomicRegion[X, Y <: GenomicRDD[X, Y]](rddToCoPartitionWith: GenomicRDD[X, Y])(implicit tTag: ClassTag[T], xTag: ClassTag[X]): U = {
    assert(rddToCoPartitionWith.SortedTrait.isSorted)
    val collectedDestinationPartitionMap = rddToCoPartitionWith.SortedTrait.partitionMap
    val partitions = collectedDestinationPartitionMap.length
    val finalPartitionedRDD = flattenRddByRegions.mapPartitionsWithIndex((idx, iter) => {
      val listRepresentation = iter.map(_.swap).toList
      for (partition <- collectedDestinationPartitionMap.zipWithIndex) yield {
        (partition._2, listRepresentation.filter(f => (f._2.start >= partition._1._1.start && f._2.start <= partition._1._2.end) ||
          (f._2.end >= partition._1._1.start && f._2.end <= partition._1._2.end)))
      }
    }.toIterator.filter(_._2.nonEmpty), preservesPartitioning = true)
      .partitionBy(new GenomicPositionRangePartitioner(partitions))
      .mapPartitions(iter => {
        iter.flatMap(_._2).toList.sortBy(_._2).toIterator
      }, preservesPartitioning = true)

    val newPartitionMap = finalPartitionedRDD.mapPartitions((iter) => {
      if (iter.isEmpty) Iterator()
      else {
        val listRepresentation = iter.toList
        Iterator((listRepresentation.head._2, listRepresentation.last._2))
      }
    })
    SortedTrait(finalPartitionedRDD.keys, newPartitionMap)
  }


  /**
   * This method is the first method called when there is some need for the
   * data to be sorted. We are assuming no previous knowledge about data
   * distribution or skew, and as such we simply partition based on range.
   * This can present some issues when the data is extremely skewed, but
   * we attempt to do our best in solving this problem.
   *
   * The output from this method is a SortedGenomicRDD, which simply
   * contains the knowledge that this was sorted and overrides many methods
   * that would be improved by sorting the data first.
   *
   * @param partitions The number of partitions to split the data into
   * @return A SortedGenomicRDDMixIn that contains the sorted and partitioned
   *         RDD
   */
  def repartitionAndSortByGenomicCoordinate(partitions: Int = rdd.partitions.length)(implicit c: ClassTag[T]): U = {

    def getPartitionMap(iter: Iterator[(ReferenceRegion, T)]): Iterator[(ReferenceRegion, ReferenceRegion)] = {
      if (iter.isEmpty) Iterator()
      else {
        val head = iter.next._1
        if (iter.hasNext) {
          var tail = iter.next._1
          while (iter.hasNext) tail = iter.next._1
          Iterator((head, tail))
        } else Iterator((head, head))
      }
    }
    val partitionedRDD = flattenRddByRegions().sortBy(f => f._1, ascending = true, partitions)
    //some really strange behavior here where it is only correct the second time
    val partitionMap = partitionedRDD.mapPartitions(iter => getPartitionMap(iter), preservesPartitioning = true)
    SortedTrait(partitionedRDD.values, partitionedRDD.mapPartitions(iter => getPartitionMap(iter), preservesPartitioning = true))
  }

  private[rdd] class GenomicPositionRangePartitioner[V](partitions: Int, elements: Int = 0) extends Partitioner {

    override def numPartitions: Int = partitions

    def getPartition(key: Any): Int = {
      key match {
        case (f1: Int, f2: Boolean) => f1
        case f: Int                 => f
        case _                      => throw new Exception("Reference Region Key require to partition on Genomic Position")
      }
    }

  }
}

private case class GenericGenomicRDD[T](rdd: RDD[T],
                                        sequences: SequenceDictionary,
                                        regionFn: T => Seq[ReferenceRegion]) extends GenomicRDD[T, GenericGenomicRDD[T]] {

  protected[rdd] def replaceRdd(newRdd: RDD[T]): GenericGenomicRDD[T] = {
    copy(rdd = newRdd)
  }

  protected[rdd] def getReferenceRegions(elem: T): Seq[ReferenceRegion] = {
    regionFn(elem)
  }
}

trait MultisampleGenomicRDD[T, U <: MultisampleGenomicRDD[T, U]] extends GenomicRDD[T, U] {

  val samples: Seq[Sample]
}

abstract class AvroReadGroupGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroReadGroupGenomicRDD[T, U]] extends AvroGenomicRDD[T, U] {

  val recordGroups: RecordGroupDictionary

  override protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    val contigSchema = Contig.SCHEMA$
    if (SortedTrait.isSorted) {
      contigSchema.addProp("sorted", "true".asInstanceOf[Any])
      contigSchema.addProp("partitionMap", SortedTrait.partitionMap.mkString(",").asInstanceOf[Any])
    }
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      contigSchema,
      contigs)

    // convert record group to avro and save
    val rgMetadata = recordGroups.recordGroups
      .map(_.toMetadata)

    val recordGroupMetaData = RecordGroupMetadata.SCHEMA$
    if (SortedTrait.isSorted) {
      recordGroupMetaData.addProp("sorted", "true".asInstanceOf[Any])
      recordGroupMetaData.addProp("partitionMap", SortedTrait.partitionMap.mkString(",").asInstanceOf[Any])
    }
    saveAvro("%s/_rgdict.avro".format(filePath),
      rdd.context,
      recordGroupMetaData,
      rgMetadata)
  }
}

abstract class MultisampleAvroGenomicRDD[T <% IndexedRecord: Manifest, U <: MultisampleAvroGenomicRDD[T, U]] extends AvroGenomicRDD[T, U]
    with MultisampleGenomicRDD[T, U] {

  override protected def saveMetadata(filePath: String) {

    val sampleSchema = Sample.SCHEMA$
    if (SortedTrait.isSorted) {
      sampleSchema.addProp("sorted", "true".asInstanceOf[Any])
      sampleSchema.addProp("partitionMap", SortedTrait.partitionMap.mkString(",").asInstanceOf[Any])
    }
    // get file to write to
    saveAvro("%s/_samples.avro".format(filePath),
      rdd.context,
      sampleSchema,
      samples)

    val contigSchema = Contig.SCHEMA$
    if (SortedTrait.isSorted) {
      contigSchema.addProp("sorted", "true".asInstanceOf[Any])
      contigSchema.addProp("partitionMap", SortedTrait.partitionMap.mkString(",").asInstanceOf[Any])
    }

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      contigSchema,
      contigs)
  }
}

abstract class AvroGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroGenomicRDD[T, U]] extends ADAMRDDFunctions[T]
    with GenomicRDD[T, U] {

  /**
   * Called in saveAsParquet after saving RDD to Parquet to save metadata.
   *
   * Writes any necessary metadata to disk. If not overridden, writes the
   * sequence dictionary to disk as Avro.
   *
   * @param args Arguments for saving file to disk.
   */
  protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    val contigSchema = Contig.SCHEMA$
    if (SortedTrait.isSorted) {
      println("Adding sorted to avro")
      contigSchema.addProp("sorted", "true".asInstanceOf[Any])
      contigSchema.addProp("partitionMap", SortedTrait.partitionMap.mkString(",").asInstanceOf[Any])
    }
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      contigSchema,
      contigs)
  }

  /**
   * Saves RDD as a directory of Parquet files.
   *
   * The RDD is written as a directory of Parquet files, with
   * Parquet configuration described by the input param args.
   * The provided sequence dictionary is written at args.outputPath/_seqdict.avro
   * as Avro binary.
   *
   * @param args Save configuration arguments.
   */
  def saveAsParquet(args: SaveArgs) {
    saveAsParquet(
      args.outputPath,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionaryEncoding
    )
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   *   Default is false.
   */
  def saveAsParquet(
    filePath: String,
    blockSize: Int = 128 * 1024 * 1024,
    pageSize: Int = 1 * 1024 * 1024,
    compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
    disableDictionaryEncoding: Boolean = false) {
    saveRddAsParquet(filePath,
      blockSize,
      pageSize,
      compressCodec,
      disableDictionaryEncoding)
    saveMetadata(filePath)
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   */
  def saveAsParquet(
    filePath: java.lang.String,
    blockSize: java.lang.Integer,
    pageSize: java.lang.Integer,
    compressCodec: CompressionCodecName,
    disableDictionaryEncoding: java.lang.Boolean) {
    saveAsParquet(
      new JavaSaveArgs(filePath,
        blockSize = blockSize,
        pageSize = pageSize,
        compressionCodec = compressCodec,
        disableDictionaryEncoding = disableDictionaryEncoding))
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   */
  def saveAsParquet(filePath: java.lang.String) {
    saveAsParquet(new JavaSaveArgs(filePath))
  }
}

trait Unaligned {

  val sequences = SequenceDictionary.empty
}
