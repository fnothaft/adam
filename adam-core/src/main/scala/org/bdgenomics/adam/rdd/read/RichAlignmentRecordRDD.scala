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
package org.bdgenomics.adam.rdd.read

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.GenomicRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.utils.interval.array.IntervalArray
import scala.reflect.ClassTag

case class RichAlignmentRecordRDD(
    rdd: RDD[RichAlignmentRecord],
    sequences: SequenceDictionary,
    recordGroups: RecordGroupDictionary,
    partitionMap: Option[Seq[Option[(ReferenceRegion, ReferenceRegion)]]] = None) extends GenomicRDD[RichAlignmentRecord, RichAlignmentRecordRDD] {

  private[rdd] def sorted: Boolean = partitionMap.isDefined

  protected def buildTree(
    rdd: RDD[(ReferenceRegion, RichAlignmentRecord)])(
      implicit tTag: ClassTag[RichAlignmentRecord]): IntervalArray[ReferenceRegion, RichAlignmentRecord] = {
    IntervalArray(rdd)
  }

  protected def replaceRdd(
    newRdd: RDD[RichAlignmentRecord],
    newPartitionMapRdd: Option[Seq[Option[(ReferenceRegion, ReferenceRegion)]]] = None): RichAlignmentRecordRDD = {
    copy(rdd = newRdd,
      partitionMap = newPartitionMapRdd)
  }

  protected def getReferenceRegions(
    elem: RichAlignmentRecord): Seq[ReferenceRegion] = {
    val optRr: Option[ReferenceRegion] = ReferenceRegion.opt(elem.record)
    optRr.fold(Seq.empty[ReferenceRegion])(rr => Seq(rr))
  }

  def toAlignmentRecordRdd: AlignmentRecordRDD = {
    if (sequences.isEmpty) {
      UnalignedReadRDD(rdd.map(r => r.record),
        recordGroups,
        partitionMap)
    } else {
      AlignedReadRDD(rdd.map(r => r.record),
        sequences,
        recordGroups,
        partitionMap)
    }
  }
}
