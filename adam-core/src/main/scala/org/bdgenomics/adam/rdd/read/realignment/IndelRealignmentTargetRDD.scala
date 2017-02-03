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
package org.bdgenomics.adam.rdd.read.realignment

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.rdd.GenomicRDD
import org.bdgenomics.adam.rdd.read.RichAlignmentRecordRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.utils.interval.array.IntervalArray
import scala.reflect.ClassTag

object IndelRealignmentTargetRDD {

  def apply(rdd: RichAlignmentRecordRDD,
            maxIndelSize: Int,
            maxTargetSize: Int): IndelRealignmentTargetRDD = {
    val targets = RealignmentTargetFinder(
      rdd.rdd,
      maxIndelSize,
      maxTargetSize
    )
    IndelRealignmentTargetRDD(rdd.rdd.context.parallelize(targets.toSeq),
      rdd.sequences,
      targets.isEmpty)
  }
}

private[realignment] case class IndelRealignmentTargetRDD(
    rdd: RDD[IndelRealignmentTarget],
    sequences: SequenceDictionary,
    isEmpty: Boolean,
    partitionMap: Option[Seq[Option[(ReferenceRegion, ReferenceRegion)]]] = None) extends GenomicRDD[IndelRealignmentTarget, IndelRealignmentTargetRDD] {

  private[rdd] def sorted: Boolean = partitionMap.isDefined

  protected def replaceRdd(
    newRdd: RDD[IndelRealignmentTarget],
    newPartitionMapRdd: Option[Seq[Option[(ReferenceRegion, ReferenceRegion)]]] = None): IndelRealignmentTargetRDD = {
    copy(rdd = newRdd,
      partitionMap = newPartitionMapRdd)
  }

  protected def getReferenceRegions(
    elem: IndelRealignmentTarget): Seq[ReferenceRegion] = {
    Seq(elem.readRange)
  }

  protected def buildTree(
    rdd: RDD[(ReferenceRegion, IndelRealignmentTarget)])(
      implicit tTag: ClassTag[IndelRealignmentTarget]): IntervalArray[ReferenceRegion, IndelRealignmentTarget] = {
    IntervalArray(rdd)
  }
}
