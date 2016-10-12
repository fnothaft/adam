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

trait SortedGenomicRDD[T, U <: SortedGenomicRDD[T, U]] extends GenomicRDD[T, U] {

  var sorted: Boolean = false

  private[this] val starts: RDD[Long] = flattenRddByRegions().map(f => f._1.start)

  private[this] val elements: Long = starts.max

  private[this] val minimum: Long = starts.min

  override def repartitionByGenomicCoordinate(partitions: Int = rdd.partitions.length)(implicit c: ClassTag[T]): Unit = {
    val partitionedRDD = rdd.map(f => (getReferenceRegions(f), f))
      .partitionBy(new GenomicPositionRangePartitioner(partitions, elements.toInt))
      .map(f => f._2)
    replaceRdd(partitionedRDD)
  }

  override def wellBalancedRepartitionByGenomicCoordinate(partitions: Int = rdd.partitions.length)(implicit c: ClassTag[T]): Unit = {
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
        for (i <- listRepresentation.indices) {
          tempList += ((i, listRepresentation(i)))
        }
        var sortedList = new ArrayBuffer[List[T]]()
        for (i <- tempList.sortBy(_._2.head._1.start)) {
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

  def getPartitionData(iter: Iterator[T], count: Int, average: Double): (List[T], List[T]) = {
    val numToTransfer = if (count > 1.4 * average) (count * 0.4).toInt else 0
    if (numToTransfer <= 0) (iter.toList, List())
    else (iter.toList.dropRight(numToTransfer), iter.drop(count - numToTransfer).toList)
  }

  def getBalancedPartitionNumber(iter: Iterator[(Long, (ReferenceRegion, T))], average: Double): Iterator[(Int, List[(ReferenceRegion, T)])] = {
    val listRepresentation = iter.toList
    listRepresentation.map(f => ((f._1 / average).asInstanceOf[Int], f._2)).groupBy(_._1).mapValues(f => f.map(_._2)).toIterator

  }

  def radixSort(iter: Iterator[(ReferenceRegion, T)]): Iterator[(ReferenceRegion, T)] = {
    if (iter.isEmpty) return iter
    val a = iter.toArray
    val aCopy = a.clone()
    var max = a.map(_._1.start).max - a.map(_._1.start).min
    var powerOf10 = 1
    val byDigit = Array.fill(10)(List[Int]())
    while (max > 0) {
      for (num <- a.indices) {
        val digit = a(num)._1.start.toInt / powerOf10 % 10
        byDigit(digit) ::= num
      }
      var i = 0
      for (j <- byDigit.indices) {
        val bin = byDigit(j)
        for (num <- bin.reverse) {
          a(i) = aCopy(num)
          i += 1
        }
        byDigit(j) = List[Int]()
      }
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
        case f: ReferenceRegion     => getRegionPartition(f)
        case (f1: Int, f2: Boolean) => f1
        case f: Int                 => f
        case _                      => throw new Exception("Reference Region Key require to partition on Genomic Position")
      }
    }

  }
}
private case class GenericSortedGenomicRDD[T](rdd: RDD[T],
                                              sequences: SequenceDictionary,
                                              regionFn: T => Seq[ReferenceRegion]) extends SortedGenomicRDD[T, GenericSortedGenomicRDD[T]] {

  protected def replaceRdd(newRdd: RDD[T]): GenericSortedGenomicRDD[T] = {
    copy(rdd = newRdd)
  }

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion] = {
    regionFn(elem)
  }
}

trait MultisampleSortedGenomicRDD[T, U <: MultisampleSortedGenomicRDD[T, U]] extends SortedGenomicRDD[T, U] {
  val samples: Seq[Sample]
}
