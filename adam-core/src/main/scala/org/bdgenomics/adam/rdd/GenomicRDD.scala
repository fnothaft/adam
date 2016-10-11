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

import org.apache.avro.generic.IndexedRecord
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  SequenceDictionary
}
import org.bdgenomics.formats.avro.{ Contig, RecordGroupMetadata, Sample }
import org.bdgenomics.utils.cli.SaveArgs
import scala.collection.mutable.ArrayBuffer
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

trait GenomicRDD[T, U <: GenomicRDD[T, U]] {

  val rdd: RDD[T]

  private[this] val starts: RDD[Long] = flattenRddByRegions().map(f => f._1.start)

  private[this] val elements: Long = starts.max

  private[this] val minimum: Long = starts.min

  val sequences: SequenceDictionary

  lazy val jrdd: JavaRDD[T] = {
    rdd.toJavaRDD()
  }

  def transform(tFn: RDD[T] => RDD[T]): U = {
    replaceRdd(tFn(rdd))
  }

  protected def replaceRdd(newRdd: RDD[T]): U

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion]

  protected def flattenRddByRegions(): RDD[(ReferenceRegion, T)] = {
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
                                                                                implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, X), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(T, X)](
      InnerShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => { getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2) })
      .asInstanceOf[GenomicRDD[(T, X), Z]]
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
                                                                                                  implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], X), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], X)](
      RightOuterShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
      .asInstanceOf[GenomicRDD[(Option[T], X), Z]]
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
                                                                                                 implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, Option[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(T, Option[X])](
      LeftOuterShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => {
        Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v))).flatten.flatten ++
          getReferenceRegions(kv._1)
      })
      .asInstanceOf[GenomicRDD[(T, Option[X]), Z]]
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
                                                                                                         implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], Option[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], Option[X])](
      FullOuterShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => {
        Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v)),
          kv._1.map(v => getReferenceRegions(v))).flatten.flatten
      })
      .asInstanceOf[GenomicRDD[(Option[T], Option[X]), Z]]
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
                                                                                                        implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, Iterable[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(T, Iterable[X])](
      InnerShuffleRegionJoinAndGroupByLeft[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => {
        (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
          getReferenceRegions(kv._1)).toSeq
      })
      .asInstanceOf[GenomicRDD[(T, Iterable[X]), Z]]
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
                                                                                                                          implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], Iterable[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], Iterable[X])](
      RightOuterShuffleRegionJoinAndGroupByLeft[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => {
        (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
          kv._1.toSeq.flatMap(v => getReferenceRegions(v))).toSeq
      })
      .asInstanceOf[GenomicRDD[(Option[T], Iterable[X]), Z]]

  }

  def repartitionByGenomicCoordinate(partitions: Int = rdd.partitions.length)(implicit c: ClassTag[T]) = {
    val partitionedRDD = rdd.map(f => (getReferenceRegions(f), f))
      .partitionBy(new GenomicPositionRangePartitioner(partitions, elements.toInt))
      .map(f => f._2)
    replaceRdd(partitionedRDD)
  }

  def wellBalancedRepartitionByGenomicCoordinate(partitions: Int = rdd.partitions.length)(implicit c: ClassTag[T]) = {
    val partitionedRDD: RDD[(ReferenceRegion, T)] = flattenRddByRegions()
      .partitionBy(new GenomicPositionRangePartitioner(partitions, elements.toInt))
    val partitionTupleCounts: Array[Int] = partitionedRDD.mapPartitions(f => Iterator(f.size)).collect
    val average: Double = partitionTupleCounts.sum.asInstanceOf[Double] / partitionTupleCounts.length.asInstanceOf[Double]
    partitionTupleCounts.foreach(println)
    println(average)
  /*
    //val x = partitionedRDD.mapPartitions(f => f.toArray.sortBy(_._1.start).map(_._2).toIterator)
    val x = partitionedRDD.mapPartitions(f => radixSort(f)).map(_._2)
      .mapPartitionsWithIndex((idx, iter) => {
        val tuple = getPartitionData(iter, partitionTupleCounts(idx), average)
        if(idx == partitions-1) Iterator(((idx, true), tuple._1 ++ tuple._2))
        else Iterator(((idx, true), tuple._1),((idx+1, false), tuple._2))
      }).partitionBy(new GenomicPositionRangePartitioner(partitions, 0))
      .mapPartitions(f => {
        val list = f.toList
        if(list.isEmpty) Iterator()
        else if(list.size == 1) list.head._2.toIterator
        else {
          val firstElement = list.head
          val secondElement = list(1)
          if (!firstElement._1._2) Iterator(firstElement._2, secondElement._2).flatten
          else Iterator(secondElement._2, firstElement._2).flatten
        }
      })
    */
    //val y = partitionedRDD.mapPartitions(f => f.toArray.sortBy(_._1.start).toIterator).zipWithIndex
    val y = partitionedRDD.mapPartitions(f => radixSort(f)).zipWithIndex
        .mapPartitions(iter => {
          getBalancedPartitionNumber(iter.map(_.swap), average)
        }).partitionBy(new GenomicPositionRangePartitioner(partitions, 0))
      .mapPartitions(iter => {
        val listRepresentation = iter.map(_._2).toList
        val tempList = new ArrayBuffer[(Int, List[(ReferenceRegion, T)])]()
        for(i <- listRepresentation.indices) {
          tempList += ((i, listRepresentation(i)))
        }
        var sortedList = new ArrayBuffer[List[T]]()
        for(i <- tempList.sortBy(_._2.head._1.start)) {
          val append = listRepresentation(i._1).map(_._2)
          sortedList += append
        }
        sortedList.flatten.toIterator
      }).persist()
    println("Partitioned: ")
    y.mapPartitions(f => Iterator(f.size)).collect.foreach(println)
    this.replaceRdd(y)
    println("Replaced: ")
    this.rdd.mapPartitions(f => Iterator(f.size)).collect.foreach(println)
  }

  def getPartitionData(iter: Iterator[T], count: Int, average: Double): (List[T],List[T]) = {
    val numToTransfer = if(count > 1.4 * average) (count * 0.4).toInt else 0
    if(numToTransfer <= 0) (iter.toList, List())
    else (iter.toList.dropRight(numToTransfer), iter.drop(count - numToTransfer).toList)
  }

  def getBalancedPartitionNumber(iter: Iterator[(Long, (ReferenceRegion, T))], average: Double): Iterator[(Int, List[(ReferenceRegion,T)])] = {
    val listRepresentation = iter.toList
    listRepresentation.map(f => ((f._1/average).asInstanceOf[Int], f._2)).groupBy(_._1).mapValues(f => f.map(_._2)).toIterator

  }

  def radixSort(iter: Iterator[(ReferenceRegion, T)]): Iterator[(ReferenceRegion, T)] = {
    if(iter.isEmpty) return iter
    val a = iter.toArray
    var max = a.map(_._1.start).max - a.map(_._1.start).min
    var powerOf10 = 1
    val byDigit = Array.fill(10)(List[(ReferenceRegion, T)]())
    while(max > 0) {
      for(num <- a) {
        val digit = num._1.start.toInt/powerOf10%10
        byDigit(digit) ::= num
      }
      var i = 0
      for(j <- byDigit.indices){
        val bin = byDigit(j)
        for(num <- bin.reverse) {
          a(i) = num
          i += 1
        }
        byDigit(j) = List[(ReferenceRegion, T)]()
      }
      /*
      for(bin <- byDigit; num <- bin.reverse) {
        a(i) = num
        i += 1
      }
      */
      powerOf10 *= 10
      max /= 10
    }
    a.toIterator
  }

  private class GenomicPositionRangePartitioner[V](partitions: Int, elements: Int) extends Partitioner {

    override def numPartitions: Int = partitions

    def getRegionPartition(key: ReferenceRegion): Int = {
      val partitionNumber =
        if ((key.start.toInt - minimum.toInt) * partitions / (elements - minimum.toInt) == partitions) partitions - 1
        else (key.start.toInt - minimum.toInt) * partitions / (elements - minimum.toInt)
      partitionNumber
    }

    def getPartition(key: Any): Int = {
      key match {
        case f: ReferenceRegion => getRegionPartition(f)
        case f: (Int, Boolean) => f._1
        case f: Int => f
        case _ => throw new Exception("Reference Region Key require to partition on Genomic Position")
      }
    }

    def counting_sort(iterator: Iterator[(ReferenceRegion, T)]): Iterator[(ReferenceRegion, T)] = {
      val a = iterator.toArray
      val max = a.map(_._1.start).max
      val n = a.length
      val m = max+1
      val z = a.map(_._1.start).min
      val countArray = Array.fill((m-z).toInt)(0)
      for(i <- a){
        countArray((i._1.start - z).toInt) += 1
      }
      var i = 0
      for(j <- 0 until m.toInt) {
        for(c <- 0 until countArray(j)) {
          //a(i) = j + z
          i += 1
        }
      }
      a.toIterator
    }
  }
}

private case class GenericGenomicRDD[T](rdd: RDD[T],
                                        sequences: SequenceDictionary,
                                        regionFn: T => Seq[ReferenceRegion]) extends GenomicRDD[T, GenericGenomicRDD[T]] {

  protected def replaceRdd(newRdd: RDD[T]): GenericGenomicRDD[T] = {
    copy(rdd = newRdd)
  }

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion] = {
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
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)

    // convert record group to avro and save
    val rgMetadata = recordGroups.recordGroups
      .map(_.toMetadata)
    saveAvro("%s/_rgdict.avro".format(filePath),
      rdd.context,
      RecordGroupMetadata.SCHEMA$,
      rgMetadata)
  }
}

abstract class MultisampleAvroGenomicRDD[T <% IndexedRecord: Manifest, U <: MultisampleAvroGenomicRDD[T, U]] extends AvroGenomicRDD[T, U]
    with MultisampleGenomicRDD[T, U] {

  override protected def saveMetadata(filePath: String) {

    // get file to write to
    saveAvro("%s/_samples.avro".format(filePath),
      rdd.context,
      Sample.SCHEMA$,
      samples)

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
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
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
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
