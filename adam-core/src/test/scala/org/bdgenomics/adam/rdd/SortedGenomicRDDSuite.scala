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

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.misc.SparkFunSuite

class SortedGenomicRDDSuite extends SparkFunSuite {

  /**
   * Determines if a given partition map has been correctly sorted
   *
   * @param list The partition map
   * @return a boolean where true is sorted and false is unsorted
   */
  def isSorted(list: Seq[(ReferenceRegion, ReferenceRegion)]): Boolean = {
    val test = list.drop(1).map(_._1)
    val test2 = list.dropRight(1).map(_._2)
    !test2.zip(test).exists(f => f._1.start > f._2.start && f._1.end > f._2.end && f._1.referenceName > f._2.referenceName)
  }

  sparkTest("testing that partition and sort provide correct outputs") {
    // load in a generic bam
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    // sort and make into 16 partitions
    val y = x.repartitionAndSort(16)
    assert(isSorted(y.partitionMap.get))
    // sort and make into 32 partitions
    val z = x.repartitionAndSort(32)
    assert(isSorted(z.partitionMap.get))
    val arrayRepresentationOfZ = z.rdd.collect
    //verify sort worked on actual values
    for (currentArray <- List(y.rdd.collect, z.rdd.collect)) {
      for (i <- currentArray.indices) {
        if (i != 0) assert(arrayRepresentationOfZ(i).getStart > arrayRepresentationOfZ(i - 1).getStart ||
          (arrayRepresentationOfZ(i).getStart == arrayRepresentationOfZ(i - 1).getStart && arrayRepresentationOfZ(i).getEnd >= arrayRepresentationOfZ(i - 1).getEnd))
      }
    }

    val partitionTupleCounts: Array[Int] = z.rdd.mapPartitions(f => Iterator(f.size)).collect
    val partitionTupleCounts2: Array[Int] = y.rdd.mapPartitions(f => Iterator(f.size)).collect
    // make sure that we didn't lose any data
    assert(partitionTupleCounts.sum == partitionTupleCounts2.sum)
  }
  sparkTest("testing copartition maintains or adds sort") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(16)
    val y = x.repartitionAndSort(32)
    val a = x.copartitionByReferenceRegion(y)
    val b = z.copartitionByReferenceRegion(y)
    println(a.rdd.partitions.length)
    println(a.rdd.mapPartitions(iter => Iterator(iter.length)).collect.mkString(","))
    println(a.partitionMap.get.mkString(","))
    println(y.partitionMap.get.mkString(","))
    assert(isSorted(a.partitionMap.get))
    assert(isSorted(b.partitionMap.get))

    val starts = z.rdd.map(f => f.getStart)
  }
  sparkTest("testing that sorted shuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    // sort and make into 16 partitions
    val z = x.repartitionAndSort(16)
    // perform join using 32 partitions
    val b = z.shuffleRegionJoin(x, Some(90))
    // this will default to 1 partition
    val c = z.shuffleRegionJoin(x, Some(1))
    val d = c.rdd.map(f => (f._1.getStart, f._2.getEnd)).collect.toSet
    val e = b.rdd.map(f => (f._1.getStart, f._2.getEnd)).collect.toSet

    val setDiff = d -- e
    assert(setDiff.isEmpty)
    assert(b.rdd.count == c.rdd.count)
  }
  sparkTest("testing that sorted fullOuterShuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(16)
    val d = x.fullOuterShuffleRegionJoin(z, Some(1))
    val e = z.fullOuterShuffleRegionJoin(x, Some(16))
    assert(d.rdd.count == e.rdd.count)
  }
  sparkTest("testing that sorted rightOuterShuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(16)
    val f = z.rightOuterShuffleRegionJoin(x, Some(1)).rdd.collect
    val g = x.rightOuterShuffleRegionJoin(x).rdd.collect
    assert(f.length == g.length)
  }
  sparkTest("testing that sorted leftOuterShuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(16)
    val h = z.leftOuterShuffleRegionJoin(x, Some(1)).rdd.collect
    val i = z.leftOuterShuffleRegionJoin(x).rdd.collect
    assert(h.length == i.length)
  }
  sparkTest("testing that we can persist the sorted knowledge") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(16)
    try {
      z.save(testFile("/sortedAlignments.parquet.txt"), true)
    } catch {
      case exists: org.apache.hadoop.mapred.FileAlreadyExistsException =>
    }
    val t = sc.loadParquetAlignments(testFile("/sortedAlignments.parquet.txt"))
    assert(t.sorted)

    val j = t.shuffleRegionJoin(x, Some(1))
    val k = x.shuffleRegionJoin(t, Some(1))
    assert(j.rdd.collect.length == k.rdd.collect.length)
  }
}
