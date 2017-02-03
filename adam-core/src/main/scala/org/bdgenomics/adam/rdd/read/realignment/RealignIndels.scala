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

import htsjdk.samtools.{ Cigar, CigarElement, CigarOperator }
import org.bdgenomics.utils.misc.Logging
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.algorithms.consensus.{ Consensus, ConsensusGenerator }
import org.bdgenomics.adam.models.{ MdTag, ReferencePosition, ReferenceRegion }
import org.bdgenomics.adam.rdd.read.RichAlignmentRecordRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.rich.RichAlignmentRecord._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.immutable.{ NumericRange, TreeSet }
import scala.collection.mutable
import scala.util.Random

private[read] object RealignIndels extends Serializable with Logging {

  /**
   * Realigns an RDD of reads.
   *
   * @param rdd RDD of reads to realign.
   * @return RDD of realigned reads.
   */
  def apply(
    rdd: RichAlignmentRecordRDD,
    consensusModel: ConsensusGenerator = ConsensusGenerator.fromReads,
    dataIsSorted: Boolean = false,
    maxIndelSize: Int = 500,
    maxConsensusNumber: Int = 30,
    lodThreshold: Double = 5.0,
    maxTargetSize: Int = 3000): RichAlignmentRecordRDD = {
    new RealignIndels(
      consensusModel,
      dataIsSorted,
      maxIndelSize,
      maxConsensusNumber,
      lodThreshold,
      maxTargetSize
    ).realignIndels(rdd)
  }

  /**
   * From a set of reads, returns the reference sequence that they overlap.
   */
  def getReferenceFromReads(reads: Iterable[RichAlignmentRecord]): (String, Long, Long) = GetReferenceFromReads.time {
    var tossedReads = 0

    // get reference and range from a single read
    val readRefs = reads.flatMap((r: RichAlignmentRecord) => {
      r.mdTag.fold {
        log.warn("Discarding read " + r.record.getReadName + " during reference re-creation.")
        tossedReads += 1
        (None: Option[(String, NumericRange[Long])])
      } { (tag) =>
        Some((tag.getReference(r), (r.getStart: Long) to r.getEnd))
      }
    })
      .toSeq
      .sortBy(_._2.head)

    // fold over sequences and append - sequence is sorted at start
    val ref = readRefs.reverse.foldRight[(String, Long)](
      ("", readRefs.head._2.head))(
        (refReads: (String, NumericRange[Long]), reference: (String, Long)) => {
          if (refReads._2.end < reference._2) {
            reference
          } else if (reference._2 >= refReads._2.head) {
            (reference._1 + refReads._1.substring((reference._2 - refReads._2.head).toInt),
              refReads._2.end)
          } else {
            // there is a gap in the sequence
            throw new IllegalArgumentException("Current sequence has a gap at " + reference._2 + "with " + refReads._2.head + "," + refReads._2.end +
              ". Discarded " + tossedReads + " in region when reconstructing region; reads may not have MD tag attached.")
          }
        })

    (ref._1, readRefs.head._2.head, ref._2)
  }
}

import org.bdgenomics.adam.rdd.read.realignment.RealignIndels._

private[read] class RealignIndels(
    val consensusModel: ConsensusGenerator = ConsensusGenerator.fromReads,
    val dataIsSorted: Boolean = false,
    val maxIndelSize: Int = 500,
    val maxConsensusNumber: Int = 30,
    val lodThreshold: Double = 5.0,
    val maxTargetSize: Int = 3000) extends Serializable with Logging {

  /**
   * Given a target group with an indel realignment target and a group of reads to realign, this method
   * generates read consensuses and realigns reads if a consensus leads to a sufficient improvement.
   *
   * @param targetGroup A tuple consisting of an indel realignment target and a seq of reads
   * @return A sequence of reads which have either been realigned if there is a sufficiently good alternative
   * consensus, or not realigned if there is not a sufficiently good consensus.
   */
  def realignTargetGroup(targetGroup: (Option[IndelRealignmentTarget], Iterable[RichAlignmentRecord])): Iterable[RichAlignmentRecord] = RealignTargetGroup.time {
    val (target, reads) = targetGroup

    if (target.isEmpty) {
      // if the indel realignment target is empty, do not realign
      reads
    } else {
      // bootstrap realigned read set with the reads that need to be realigned
      var realignedReads = reads.filter(r => r.mdTag.exists(!_.hasMismatches))

      // get reference from reads
      val (reference, refStart, refEnd) = getReferenceFromReads(reads.map(r => new RichAlignmentRecord(r)))
      val refRegion = ReferenceRegion(reads.head.record.getContigName, refStart, refEnd)

      // preprocess reads and get consensus
      val readsToClean = consensusModel.preprocessReadsForRealignment(
        reads.filter(r => r.mdTag.forall(_.hasMismatches)),
        reference,
        refRegion
      )
      var consensus = consensusModel.findConsensus(readsToClean)
        .toSeq
        .distinct

      // reduce count of consensus sequences
      if (consensus.size > maxConsensusNumber) {
        val r = new Random()
        consensus = r.shuffle(consensus).take(maxConsensusNumber)
      }

      if (readsToClean.size > 0 && consensus.size > 0) {

        // do not check realigned reads - they must match
        val totalMismatchSumPreCleaning = readsToClean.map(sumMismatchQuality(_)).sum

        /* list to log the outcome of all consensus trials. stores:
         *  - mismatch quality of reads against new consensus sequence
         *  - the consensus sequence itself
         *  - a map containing each realigned read and it's offset into the new sequence
         */
        var consensusOutcomes = List[(Int, Consensus, mutable.Map[RichAlignmentRecord, Int])]()

        // loop over all consensuses and evaluate
        consensus.foreach(c => {
          // generate a reference sequence from the consensus
          val consensusSequence = c.insertIntoReference(reference, refRegion)

          // evaluate all reads against the new consensus
          val sweptValues = readsToClean.map(r => {
            val (qual, pos) = sweepReadOverReferenceForQuality(r.getSequence, consensusSequence, r.qualityScores)
            val originalQual = sumMismatchQuality(r)

            // if the read's mismatch quality improves over the original alignment, save
            // its alignment in the consensus sequence, else store -1
            if (qual < originalQual) {
              (r, (qual, pos))
            } else {
              (r, (originalQual, -1))
            }
          })

          // sum all mismatch qualities to get the total mismatch quality for this alignment
          val totalQuality = sweptValues.map(_._2._1).sum

          // package data
          var readMappings = mutable.Map[RichAlignmentRecord, Int]()
          sweptValues.map(kv => (kv._1, kv._2._2)).foreach(m => {
            readMappings += (m._1 -> m._2)
          })

          // add to outcome list
          consensusOutcomes = (totalQuality, c, readMappings) :: consensusOutcomes
        })

        // perform reduction to pick the consensus with the lowest aggregated mismatch score
        val bestConsensusTuple = consensusOutcomes.minBy(_._1)

        val (bestConsensusMismatchSum, bestConsensus, bestMappings) = bestConsensusTuple

        // check for a sufficient improvement in mismatch quality versus threshold
        log.info("On " + refRegion + ", before realignment, sum was " + totalMismatchSumPreCleaning +
          ", best realignment has " + bestConsensusMismatchSum)
        val lodImprovement = (totalMismatchSumPreCleaning - bestConsensusMismatchSum).toDouble / 10.0
        if (lodImprovement > lodThreshold) {
          var realignedReadCount = 0

          // if we see a sufficient improvement, realign the reads
          val cleanedReads: Iterable[RichAlignmentRecord] = readsToClean.map(r => {

            val builder: AlignmentRecord.Builder = AlignmentRecord.newBuilder(r)
            val remapping = bestMappings(r)

            // if read alignment is improved by aligning against new consensus, realign
            if (remapping != -1) {

              realignedReadCount += 1

              // bump up mapping quality by 10
              builder.setMapq(r.getMapq + 10)

              // set new start to consider offset
              builder.setStart(refStart + remapping)

              // recompute cigar
              val newCigar: Cigar = {
                // if element overlaps with consensus indel, modify cigar with indel
                val (idElement, endLength, endPenalty) = if (bestConsensus.index.start == bestConsensus.index.end - 1) {
                  (new CigarElement(bestConsensus.consensus.length, CigarOperator.I),
                    r.getSequence.length - bestConsensus.consensus.length - (bestConsensus.index.start - (refStart + remapping)),
                    -bestConsensus.consensus.length)
                } else {
                  (new CigarElement((bestConsensus.index.end - 1 - bestConsensus.index.start).toInt, CigarOperator.D),
                    r.getSequence.length - (bestConsensus.index.start - (refStart + remapping)),
                    bestConsensus.consensus.length)
                }

                // compensate the end
                builder.setEnd(refStart + remapping + r.getSequence.length + endPenalty)

                val cigarElements = List[CigarElement](
                  new CigarElement((bestConsensus.index.start - (refStart + remapping)).toInt, CigarOperator.M),
                  idElement,
                  new CigarElement(endLength.toInt, CigarOperator.M)
                )

                new Cigar(cigarElements)
              }

              // update mdtag and cigar
              builder.setMismatchingPositions(MdTag.moveAlignment(r, newCigar, reference.drop(remapping), refStart + remapping).toString())
              builder.setOldPosition(r.getStart())
              builder.setOldCigar(r.getCigar())
              builder.setCigar(newCigar.toString)
              new RichAlignmentRecord(builder.build())
            } else
              new RichAlignmentRecord(builder.build())
          })

          log.info("On " + refRegion + ", realigned " + realignedReadCount + " reads to " +
            bestConsensus + " due to LOD improvement of " + lodImprovement)

          realignedReads = cleanedReads ++ realignedReads
        } else {
          log.info("On " + refRegion + ", skipping realignment due to insufficient LOD improvement (" +
            lodImprovement + "for consensus " + bestConsensus)
          realignedReads = readsToClean ++ realignedReads
        }
      }

      // return all reads that we cleaned and all reads that were initially realigned
      realignedReads
    }
  }

  /**
   * Sweeps the read across a reference sequence and evaluates the mismatch quality at each position. Returns
   * the alignment offset that leads to the lowest mismatch quality score. Invariant: reference sequence must be
   * longer than the read sequence.
   *
   * @param read Read to test.
   * @param reference Reference sequence to sweep across.
   * @param qualities Integer sequence of phred scaled base quality scores.
   * @return Tuple of (mismatch quality score, alignment offset).
   */
  def sweepReadOverReferenceForQuality(read: String, reference: String, qualities: Seq[Int]): (Int, Int) = SweepReadOverReferenceForQuality.time {

    var qualityScores = List[(Int, Int)]()

    // calculate mismatch quality score for all admissable alignment offsets
    for (i <- 0 to (reference.length - read.length)) {
      val qualityScore = sumMismatchQualityIgnoreCigar(read, reference.substring(i, i + read.length), qualities)
      qualityScores = (qualityScore, i) :: qualityScores
    }

    // perform reduction to get best quality offset
    qualityScores.minBy(_._1)
  }

  /**
   * Sums the mismatch quality of a read against a reference. Mismatch quality is defined as the sum
   * of the base quality for all bases in the read that do not match the reference. This method
   * ignores the cigar string, which treats indels as causing mismatches.
   *
   * @param read Read to evaluate.
   * @param reference Reference sequence to look for mismatches against.
   * @param qualities Sequence of base quality scores.
   * @return Mismatch quality sum.
   */
  def sumMismatchQualityIgnoreCigar(read: String, reference: String, qualities: Seq[Int]): Int = {
    val mismatchQualities = read.zip(reference)
      .zip(qualities)
      .filter(r => r._1._1 != r._1._2)
      .map(_._2)

    if (mismatchQualities.length > 0) {
      mismatchQualities.sum
    } else {
      0
    }
  }

  /**
   * Given a read, sums the mismatch quality against it's current alignment position.
   * Does NOT ignore cigar.
   *
   * @param read Read over which to sum mismatch quality.
   * @return Mismatch quality of read for current alignment.
   */
  def sumMismatchQuality(read: AlignmentRecord): Int = {
    sumMismatchQualityIgnoreCigar(
      read.getSequence,
      read.mdTag.get.getReference(read),
      read.qualityScores
    )
  }

  /**
   * Performs realignment for an RDD of reads. This includes target generation, read/target
   * classification, and read realignment.
   *
   * @param rdd Reads to realign.
   * @return Realigned read.
   */
  def realignIndels(rdd: RichAlignmentRecordRDD): RichAlignmentRecordRDD = {

    // sort the input rdd
    val sortedRdd = if (!rdd.sorted) {
      rdd.repartitionAndSort()
    } else {
      rdd
    }

    // find realignment targets
    log.info("Generating realignment targets...")
    val targets = IndelRealignmentTargetRDD(
      rdd,
      maxIndelSize,
      maxTargetSize
    )

    // we should only attempt realignment if the target set isn't empty
    if (targets.isEmpty) {
      rdd
    } else {
      val readsMappedToTarget = targets.rightOuterShuffleRegionJoinAndGroupByLeft(rdd)
        .rdd

      // realign target groups
      rdd.copy(rdd = readsMappedToTarget.flatMap(realignTargetGroup))
    }
  }
}
