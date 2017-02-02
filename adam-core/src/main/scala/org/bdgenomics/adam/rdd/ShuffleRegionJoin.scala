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
  protected val partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]]
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

/**
 * This trait is the source of all shuffleRegionJoins
 */
private trait SortedIntervalPartitionJoin[T, U, RT, RU]
    extends Iterator[(RT, RU)]
    with Serializable {

  // this is the lower bound of the left side of this partition
  val regionLowerBound: ReferenceRegion
  // this is the max of the upperbounds between both the left and
  //right side of this partition
  val regionUpperBound: ReferenceRegion
  // the left side of the join
  val left: BufferedIterator[(ReferenceRegion, T)]
  // the right side of the join
  val right: BufferedIterator[(ReferenceRegion, U)]

  protected def advanceCache(until: ReferenceRegion)

  protected def pruneCache(to: ReferenceRegion)

  protected def finalizeHits(): Iterator[(RT, RU)]

  // this is the joined partition
  private val hits: Iterator[(RT, RU)] = getHits() ++ finalizeHits()

  /**
   * Gets all hits for each element in left.
   *
   * @see finalizeHits()
   *
   * @return the joined partition based on processHits()
   */
  private def getHits(): Iterator[(RT, RU)] = {
    left.flatMap(f => {
      val currentLeftRegion = f._1

      advanceCache(currentLeftRegion)
      pruneCache(currentLeftRegion)

      processHits(f._2, f._1)
    })
  }

  protected def processHits(currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterator[(RT, RU)]

  /**
   * Overrides Iterator.hasNext. Determines if there is at least
   * one additional element in the hits.
   *
   * @return a true value if there is at least one additional
   *         element in the hits, false if there are no more
   *         elements in the hits.
   */
  final def hasNext: Boolean = {
    hits.hasNext
  }

  /**
   * Overrides Iterator.next. Pops the next value off hits.
   *
   * @return the next value in hits.
   */
  final def next: (RT, RU) = {
    hits.next
  }
}

private trait VictimlessSortedIntervalPartitionJoin[T, U, RU]
    extends SortedIntervalPartitionJoin[T, U, T, RU]
    with Serializable {

  // stores join candidate tuples from right
  private val cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty

  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, RU)]

  /**
   * Adds elements from right to cache based on the next region
   * encountered.
   */
  protected def advanceCache(until: ReferenceRegion): Unit = {
    while (right.hasNext && (right.head._1.compareTo(until) <= 0 ||
      right.head._1.covers(until))) {
      val x = right.next()
      cache += ((x._1, x._2))
    }
  }

  /**
   * Removes elements from cache that will not be joined to any remaining tuples
   * in left.
   */
  protected def pruneCache(to: ReferenceRegion) {
    // remove values from cache that are less that the to and that do
    // not overlap to.
    cache --= cache.takeWhile(f => f._1.compareTo(to) < 0 && !f._1.covers(to))
  }

  /**
   * Process hits for a given tuple in left.
   */
  protected def processHits(currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterator[(T, RU)] = {
    // post processing formats the hits for each individual type of join
    postProcessHits(cache
      // everything that overlaps the left region is a hit
      .filter(y => {
        y._1.covers(currentLeftRegion)
      })
      .map(y => (currentLeft, y._2))
      .iterator, currentLeft)
  }
  /**
   * Computes all victims for the partition. NOTE: These are victimless joins
   * so we have no victims.
   *
   * @return an empty iterator.
   */
  override protected def finalizeHits(): Iterator[(T, RU)] = Iterator.empty
}

private case class InnerSortedIntervalPartitionJoin[T: ClassTag, U: ClassTag](
  regionLowerBound: ReferenceRegion,
  regionUpperBound: ReferenceRegion,
  left: BufferedIterator[(ReferenceRegion, T)],
  right: BufferedIterator[(ReferenceRegion, U)])
    extends VictimlessSortedIntervalPartitionJoin[T, U, U] {

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @return the post processed iterator.
   */
  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, U)] = {
    // no post-processing required
    iter
  }
}

private case class SortedIntervalPartitionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](
  regionLowerBound: ReferenceRegion,
  regionUpperBound: ReferenceRegion,
  left: BufferedIterator[(ReferenceRegion, T)],
  right: BufferedIterator[(ReferenceRegion, U)])
    extends VictimlessSortedIntervalPartitionJoin[T, U, Iterable[U]] {

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @return the post processed iterator.
   */
  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, Iterable[U])] = {
    if (iter.hasNext) {
      // group all hits for currentLeft into an iterable
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

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @return the post processed iterator.
   */
  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, Option[U])] = {
    if (iter.hasNext) {
      // left has some hits
      iter.map(kv => (kv._1, Some(kv._2)))
    } else {
      // left has no hits
      Iterator((currentLeft, None))
    }
  }
}

private trait SortedIntervalPartitionJoinWithVictims[T, U, RT, RU]
    extends SortedIntervalPartitionJoin[T, U, RT, RU]
    with Serializable {

  // stores join candidate tuples from right
  private val cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty
  // caches potential pruned values
  private val victimCache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty
  // the pruned values that do not contain any hits from the left
  private val pruned: ListBuffer[(RT, RU)] = ListBuffer.empty

  protected def postProcessHits(iter: Iterator[U],
                                currentLeft: T): Iterator[(RT, RU)]

  /**
   * Process hits for a given tuple in left.
   */
  protected def processHits(currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterator[(RT, RU)] = {
    // post processing formats the hits for each individual type of join
    postProcessHits(cache
      .filter(y => {
        // everything that overlaps the left region is a hit
        y._1.covers(currentLeftRegion)
      })
      .map(y => y._2)
      .toIterator, currentLeft)
  }

  /**
   * Adds elements from right to victimCache based on the next region encountered.
   */
  protected def advanceCache(until: ReferenceRegion): Unit = {
    while (right.hasNext && (right.head._1.compareTo(until) <= 0 || right.head._1.covers(until))) {
      val x = right.next()
      victimCache += x._1 -> x._2
    }
  }

  /**
   * Removes elements from cache that will not be joined to any remaining tuples
   * in left. Also adds the elements that are not hits to the list of pruned.
   */
  protected def pruneCache(to: ReferenceRegion) {
    // remove everything from cache that will never again be joined
    cache --= cache.takeWhile(f => f._1.compareTo(to) < 0 && !f._1.covers(to))
    // add the values from the victimCache that are candidates to be joined
    // the the current left
    cache ++= victimCache.takeWhile(f => f._1.compareTo(to) > 0 || f._1.covers(to))
    // remove the values from the victimCache that were just added to cache
    victimCache --= victimCache.takeWhile(f => f._1.compareTo(to) > 0 || f._1.covers(to))
    // add to pruned any values that do not have any matches to a left
    // and perform post processing to format the new pruned values
    pruned ++= victimCache.takeWhile(f => f._1.compareTo(to) <= 0).map(u => postProcessPruned(u._2))
    // remove the values from victimCache that were added to pruned
    victimCache --= victimCache.takeWhile(f => f._1.compareTo(to) <= 0)
  }

  protected def postProcessPruned(pruned: U): (RT, RU)

  /**
   * Computes all victims for the partition.
   *
   * @return an iterator of pruned hits.
   */
  override protected def finalizeHits(): Iterator[(RT, RU)] = {
    // advance the cache in case there are still values on the end
    // that weren't covered by the left
    advanceCache(regionUpperBound)
    // compute the pruned values
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

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @return the post processed iterator.
   */
  protected def postProcessHits(iter: Iterator[U],
                                currentLeft: T): Iterator[(Option[T], Option[U])] = {
    if (iter.hasNext) {
      // formatting these as options for the full outer join
      iter.map(u => (Some(currentLeft), Some(u)))
    } else {
      // no hits for the currentLeft
      Iterator((Some(currentLeft), None))
    }
  }

  /**
   * Properly formats right values that did not pair with a left
   */
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

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @return the post processed iterator.
   */
  protected def postProcessHits(iter: Iterator[U],
                                currentLeft: T): Iterator[(Option[T], Iterable[U])] = {
    Iterator((Some(currentLeft), iter.toIterable))
  }

  /**
   * Properly formats right values that did not pair with a left
   */
  protected def postProcessPruned(pruned: U): (Option[T], Iterable[U]) = {
    (None, Iterable(pruned))
  }
}
