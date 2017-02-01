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

import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegion }
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * A trait describing join implementations that are based on a sort-merge join.
 *
 * @tparam T The type of the left RDD.
 * @tparam U The type of the right RDD.
 * @tparam RT The type of data yielded by the left RDD at the output of the
 *   join. This may not match T if the join is an outer join, etc.
 * @tparam RU The type of data yielded by the right RDD at the output of the
 *   join.
 */
sealed abstract class ShuffleRegionJoin[T: ClassTag, U: ClassTag, RT, RU]
    extends RegionJoin[T, U, RT, RU] {

  protected val sd: SequenceDictionary
  protected val partitionSize: Long
  protected val sc: SparkContext
  protected val partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]]

  // Create the set of bins across the genome for parallel processing
  //   partitionSize (in nucleotides) may range from 10000 to 10000000+
  //   depending on cluster and dataset size
  protected val seqLengths = Map(sd.records.map(rec => (rec.name, rec.length)): _*)
  protected val bins = sc.broadcast(GenomeBins(partitionSize, seqLengths))

  /**
   * Performs a region join between two RDDs (shuffle join). All data should be pre-shuffled and
   * copartitioned.
   *
   * @param leftRDD The 'left' side of the join, also contains the partition bounds of each partition
   * @param rightRDD The 'right' side of the join
   * @return An RDD of pairs (x, y), where x is from leftRDD, y is from rightRDD, and the region
   *         corresponding to x overlaps the region corresponding to y.
   */
  def partitionAndJoin(leftRDD: RDD[(ReferenceRegion, T)],
                       rightRDD: RDD[(ReferenceRegion, U)]): RDD[(RT, RU)] = {
    leftRDD.mapPartitionsWithIndex((idx, iter) => {
      if (iter.isEmpty) {
        Iterator((None, iter))
      } else {
        Iterator((partitionMap(idx), iter))
      }
    }).zipPartitions(rightRDD, preservesPartitioning = true)(sweep)
  }

  protected def makeIterator(regionLowerBound: ReferenceRegion,
                             regionUpperBound: ReferenceRegion,
                             left: BufferedIterator[(ReferenceRegion, T)],
                             right: BufferedIterator[(ReferenceRegion, U)]): Iterator[(RT, RU)]

  // this function carries out the sort-merge join inside each Spark partition.
  // It assumes the iterators are sorted.
  def sweep(leftIterWithPartitionBounds: Iterator[(Option[(ReferenceRegion, ReferenceRegion)], Iterator[(ReferenceRegion, T)])],
            rightIter: Iterator[(ReferenceRegion, U)]): Iterator[(RT, RU)] = {
    val (partitionBounds, leftIter) = leftIterWithPartitionBounds.next
    if (leftIter.isEmpty || rightIter.isEmpty || partitionBounds.isEmpty) {
      emptyFn(leftIter, rightIter)
    } else {
      val bufferedLeft = leftIter.buffered
      // return an Iterator[(T, U)]
      makeIterator(partitionBounds.get._1, partitionBounds.get._2, bufferedLeft, rightIter.buffered)
    }
  }

  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(RT, RU)]
}

/**
 * Extends the ShuffleRegionJoin trait to implement an inner join.
 */
case class InnerShuffleRegionJoin[T: ClassTag, U: ClassTag](sd: SequenceDictionary,
                                                            partitionSize: Long,
                                                            @transient sc: SparkContext,
                                                            partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]])
    extends ShuffleRegionJoin[T, U, T, U] {

  protected def makeIterator(regionLowerBound: ReferenceRegion,
                             regionUpperBound: ReferenceRegion,
                             left: BufferedIterator[(ReferenceRegion, T)],
                             right: BufferedIterator[(ReferenceRegion, U)]): Iterator[(T, U)] = {
    InnerSortedIntervalPartitionJoin(regionLowerBound, regionUpperBound, left, right)
  }

  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(T, U)] = {
    Iterator.empty
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement a left outer join.
 */
case class LeftOuterShuffleRegionJoin[T: ClassTag, U: ClassTag](sd: SequenceDictionary,
                                                                partitionSize: Long,
                                                                @transient sc: SparkContext,
                                                                partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]])
    extends ShuffleRegionJoin[T, U, T, Option[U]] {

  protected def makeIterator(regionLowerBound: ReferenceRegion,
                             regionUpperBound: ReferenceRegion,
                             left: BufferedIterator[(ReferenceRegion, T)],
                             right: BufferedIterator[(ReferenceRegion, U)]): Iterator[(T, Option[U])] = {
    LeftOuterSortedIntervalPartitionJoin(regionLowerBound, regionUpperBound, left, right)
  }

  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(T, Option[U])] = {
    left.map(t => (t._2, None))
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement a right outer join.
 */
case class RightOuterShuffleRegionJoin[T: ClassTag, U: ClassTag](sd: SequenceDictionary,
                                                                 partitionSize: Long,
                                                                 @transient sc: SparkContext,
                                                                 partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]])
    extends ShuffleRegionJoin[T, U, Option[T], U] {

  protected def makeIterator(regionLowerBound: ReferenceRegion,
                             regionUpperBound: ReferenceRegion,
                             left: BufferedIterator[(ReferenceRegion, T)],
                             right: BufferedIterator[(ReferenceRegion, U)]): Iterator[(Option[T], U)] = {
    LeftOuterSortedIntervalPartitionJoin(regionLowerBound, regionUpperBound, right, left).map(_.swap)
  }

  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(Option[T], U)] = {
    right.map(u => (None, u._2))
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement a full outer join.
 */
case class FullOuterShuffleRegionJoin[T: ClassTag, U: ClassTag](sd: SequenceDictionary,
                                                                partitionSize: Long,
                                                                @transient sc: SparkContext,
                                                                partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]])
    extends ShuffleRegionJoin[T, U, Option[T], Option[U]] {

  protected def makeIterator(regionLowerBound: ReferenceRegion,
                             regionUpperBound: ReferenceRegion,
                             left: BufferedIterator[(ReferenceRegion, T)],
                             right: BufferedIterator[(ReferenceRegion, U)]): Iterator[(Option[T], Option[U])] = {
    FullOuterSortedIntervalPartitionJoin(regionLowerBound, regionUpperBound, left, right)
  }

  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(Option[T], Option[U])] = {
    left.map(t => (Some(t._2), None)) ++ right.map(u => (None, Some(u._2)))
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement an inner join followed by
 * grouping by the left value.
 */
case class InnerShuffleRegionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](sd: SequenceDictionary,
                                                                          partitionSize: Long,
                                                                          @transient sc: SparkContext,
                                                                          partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]])
    extends ShuffleRegionJoin[T, U, T, Iterable[U]] {

  protected def makeIterator(regionLowerBound: ReferenceRegion,
                             regionUpperBound: ReferenceRegion,
                             left: BufferedIterator[(ReferenceRegion, T)],
                             right: BufferedIterator[(ReferenceRegion, U)]): Iterator[(T, Iterable[U])] = {
    SortedIntervalPartitionJoinAndGroupByLeft(regionLowerBound, regionUpperBound, left, right)
  }

  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(T, Iterable[U])] = {
    Iterator.empty
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement a right outer join followed by
 * grouping by all non-null left values.
 */
case class RightOuterShuffleRegionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](sd: SequenceDictionary,
                                                                               partitionSize: Long,
                                                                               @transient sc: SparkContext,
                                                                               partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]])
    extends ShuffleRegionJoin[T, U, Option[T], Iterable[U]] {

  protected def makeIterator(regionLowerBound: ReferenceRegion,
                             regionUpperBound: ReferenceRegion,
                             left: BufferedIterator[(ReferenceRegion, T)],
                             right: BufferedIterator[(ReferenceRegion, U)]): Iterator[(Option[T], Iterable[U])] = {
    RightOuterSortedIntervalPartitionJoinAndGroupByLeft(regionLowerBound, regionUpperBound, left, right)
  }

  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(Option[T], Iterable[U])] = {
    left.map(v => (Some(v._2), Iterable.empty)) ++ right.map(v => (None, Iterable(v._2)))
  }
}

private trait SortedIntervalPartitionJoin[T, U, RT, RU]
    extends Iterator[(RT, RU)]
    with Serializable {

  val regionLowerBound: ReferenceRegion
  val regionUpperBound: ReferenceRegion
  val left: BufferedIterator[(ReferenceRegion, T)]
  val right: BufferedIterator[(ReferenceRegion, U)]

  protected def advanceCache(until: ReferenceRegion)

  protected def pruneCache(to: ReferenceRegion)

  protected def finalizeHits(): Iterator[(RT, RU)]

  protected val hits: Iterator[(RT, RU)] = getHits() ++ finalizeHits()

  private def getHits(): Iterator[(RT, RU)] = {
    left.flatMap(f => {
      val nextLeftRegion = f._1
      advanceCache(nextLeftRegion)
      pruneCache(nextLeftRegion)
      processHits(f._2, f._1)
    })
  }

  protected def processHits(currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterator[(RT, RU)]

  final def hasNext: Boolean = {
    hits.hasNext
  }

  final def next: (RT, RU) = {
    hits.next
  }
}

private trait VictimlessSortedIntervalPartitionJoin[T, U, RU]
    extends SortedIntervalPartitionJoin[T, U, T, RU]
    with Serializable {

  // stores the rightIter values that might overlap the current value from the leftIter
  private val cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty

  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, RU)]

  protected def advanceCache(until: ReferenceRegion): Unit = {
    while (right.hasNext && (right.head._1.compareTo(until) <= 0 || right.head._1.covers(until))) {
      val x = right.next()
      cache += x._1 -> x._2
    }
  }

  protected def pruneCache(to: ReferenceRegion) {
    cache --= cache.takeWhile(f => f._1.compareTo(to) < 0 && !f._1.covers(to))
  }

  protected def processHits(currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterator[(T, RU)] = {
    postProcessHits(cache
      .filter(y => {
        y._1.overlaps(currentLeftRegion) &&
          (y._1.compareTo(regionLowerBound) >= 0 || currentLeftRegion.compareTo(regionLowerBound) >= 0)
      })
      .map(y => (currentLeft, y._2))
      .toIterator, currentLeft)
  }

  override protected def finalizeHits(): Iterator[(T, RU)] = Iterator.empty
}

private case class InnerSortedIntervalPartitionJoin[T: ClassTag, U: ClassTag](
  regionLowerBound: ReferenceRegion,
  regionUpperBound: ReferenceRegion,
  left: BufferedIterator[(ReferenceRegion, T)],
  right: BufferedIterator[(ReferenceRegion, U)])
    extends VictimlessSortedIntervalPartitionJoin[T, U, U] {

  // no op!
  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, U)] = {
    iter
  }
}

private case class SortedIntervalPartitionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](
  regionLowerBound: ReferenceRegion,
  regionUpperBound: ReferenceRegion,
  left: BufferedIterator[(ReferenceRegion, T)],
  right: BufferedIterator[(ReferenceRegion, U)])
    extends VictimlessSortedIntervalPartitionJoin[T, U, Iterable[U]] {

  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, Iterable[U])] = {
    if (iter.hasNext) {
      Iterator((currentLeft, iter.map(_._2).toIterable))
    } else {
      Iterator.empty
    }
  }
}

private case class LeftOuterSortedIntervalPartitionJoin[T: ClassTag, U: ClassTag](
  regionLowerBound: ReferenceRegion,
  regionUpperBound: ReferenceRegion,
  left: BufferedIterator[(ReferenceRegion, T)],
  right: BufferedIterator[(ReferenceRegion, U)])
    extends VictimlessSortedIntervalPartitionJoin[T, U, Option[U]] {

  // no op!
  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, Option[U])] = {
    if (iter.hasNext) {
      iter.map(kv => (kv._1, Some(kv._2)))
    } else {
      Iterator((currentLeft, None))
    }
  }
}

private trait SortedIntervalPartitionJoinWithVictims[T, U, RT, RU]
    extends SortedIntervalPartitionJoin[T, U, RT, RU]
    with Serializable {

  // stores the rightIter values that might overlap the current value from the leftIter
  private val cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty
  private val victimCache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty
  private val pruned: ListBuffer[(RT, RU)] = ListBuffer.empty

  protected def postProcessHits(iter: Iterator[U],
                                currentLeft: T): Iterator[(RT, RU)]

  protected def processHits(currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterator[(RT, RU)] = {
    postProcessHits(cache
      .filter(y => {
        y._1.overlaps(currentLeftRegion) &&
          (y._1.compareTo(regionLowerBound) >= 0 || currentLeftRegion.compareTo(regionLowerBound) >= 0)
      })
      .map(y => y._2)
      .toIterator, currentLeft)
  }

  protected def advanceCache(until: ReferenceRegion): Unit = {
    while (right.hasNext && (right.head._1.compareTo(until) <= 0 || right.head._1.covers(until))) {
      val x = right.next()
      victimCache += x._1 -> x._2
    }
  }

  protected def pruneCache(to: ReferenceRegion) {

    cache --= cache.takeWhile(f => f._1.compareTo(to) < 0 && !f._1.covers(to))
    cache ++= victimCache.takeWhile(f => f._1.compareTo(to) > 0 || f._1.covers(to))
    victimCache --= victimCache.takeWhile(f => f._1.compareTo(to) > 0 || f._1.covers(to))

    pruned ++= victimCache.takeWhile(f => f._1.compareTo(to) <= 0).map(u => postProcessPruned(u._2))

    victimCache --= victimCache.takeWhile(f => f._1.compareTo(to) <= 0)
  }

  protected def postProcessPruned(pruned: U): (RT, RU)

  override protected def finalizeHits(): Iterator[(RT, RU)] = {
    advanceCache(regionUpperBound)
    pruneCache(regionUpperBound)
    pruned.iterator
  }
}

private case class FullOuterSortedIntervalPartitionJoin[T: ClassTag, U: ClassTag](
  regionLowerBound: ReferenceRegion,
  regionUpperBound: ReferenceRegion,
  left: BufferedIterator[(ReferenceRegion, T)],
  right: BufferedIterator[(ReferenceRegion, U)])
    extends SortedIntervalPartitionJoinWithVictims[T, U, Option[T], Option[U]] {

  protected def postProcessHits(iter: Iterator[U],
                                currentLeft: T): Iterator[(Option[T], Option[U])] = {
    if (iter.hasNext) {
      iter.map(u => (Some(currentLeft), Some(u)))
    } else {
      Iterator((Some(currentLeft), None))
    }
  }

  protected def postProcessPruned(pruned: U): (Option[T], Option[U]) = {
    (None, Some(pruned))
  }
}

private case class RightOuterSortedIntervalPartitionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](
  regionLowerBound: ReferenceRegion,
  regionUpperBound: ReferenceRegion,
  left: BufferedIterator[(ReferenceRegion, T)],
  right: BufferedIterator[(ReferenceRegion, U)])
    extends SortedIntervalPartitionJoinWithVictims[T, U, Option[T], Iterable[U]] {

  protected def postProcessHits(iter: Iterator[U],
                                currentLeft: T): Iterator[(Option[T], Iterable[U])] = {
    Iterator((Some(currentLeft), iter.toIterable))
  }

  protected def postProcessPruned(pruned: U): (Option[T], Iterable[U]) = {
    (None, Iterable(pruned))
  }
}
