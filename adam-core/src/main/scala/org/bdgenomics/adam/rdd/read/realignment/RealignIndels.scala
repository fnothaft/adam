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
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.rich.RichAlignmentRecord._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.util.ReferenceFile
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
    rdd: RDD[AlignmentRecord],
    consensusModel: ConsensusGenerator = ConsensusGenerator.fromReads,
    dataIsSorted: Boolean = false,
    maxIndelSize: Int = 500,
    maxConsensusNumber: Int = 30,
    lodThreshold: Double = 5.0,
    maxTargetSize: Int = 3000,
    maxReadsPerTarget: Int = 20000,
    optReferenceFile: Option[ReferenceFile] = None,
    falloff: Int = 1): RDD[AlignmentRecord] = {
    new RealignIndels(
      consensusModel,
      dataIsSorted,
      maxIndelSize,
      maxConsensusNumber,
      lodThreshold,
      maxTargetSize,
      maxReadsPerTarget,
      optReferenceFile,
      falloff
    ).realignIndels(rdd)
  }

  /**
   * Method to map a record to an indel realignment target. Returns the index of the target to align to if the read has a
   * target and should be realigned, else returns the "empty" target (denoted by a negative index).
   *
   * @note Generally, this function shouldn't be called directly---for most cases, prefer mapTargets.
   * @param read Read to check.
   * @param targets Sorted array of realignment targets.
   * @return If overlapping target is found, returns that target. Else, returns the "empty" target.
   *
   * @see mapTargets
   */
  @tailrec final def mapToTarget(
    read: RichAlignmentRecord,
    targets: Array[IndelRealignmentTarget],
    headIdx: Int,
    tailIdx: Int,
    targetsToDrop: Set[Int] = Set.empty): Int = {
    // Perform tail call recursive binary search
    if (TargetOrdering.contains(targets(headIdx), read)) {
      // if there is overlap, return the overlapping target, unless it has been
      // flagged to be dropped, in which case, return an empty target (negative
      // index)
      if (targetsToDrop(headIdx)) {
        -read.record.hashCode.abs
      } else {
        headIdx
      }
    } else if (tailIdx - headIdx <= 1) {
      // else, return an empty target (negative index)
      -read.record.hashCode.abs
    } else {
      // split the set and recurse
      val splitIdx = headIdx + ((tailIdx - headIdx) / 2)

      if (TargetOrdering.contains(targets(splitIdx), read)) {
        splitIdx
      } else {
        val (newHeadIdx, newTailIdx) = if (TargetOrdering.lt(targets(splitIdx), read)) {
          (splitIdx, tailIdx)
        } else {
          (headIdx, splitIdx)
        }
        mapToTarget(read, targets, newHeadIdx, newTailIdx)
      }
    }
  }

  /**
   * Maps reads to targets. Wraps both mapToTarget functions together and handles target index creation and broadcast.
   *
   * @note This function may return multiple sets of reads mapped to empty targets. This is intentional. For typical workloads, there
   * will be many more reads that map to the empty target (reads that do not need to be realigned) than reads that need to be realigned.
   * Thus, we must spread the reads that do not need to be realigned across multiple empty targets to reduce imbalance.
   *
   * @param rich_rdd RDD containing RichADAMRecords which are to be mapped to a realignment target.
   * @param targets Set of targets that are to be mapped against.
   * @param maxReadsPerTarget The maximum number of reads to allow per target.
   *
   * @return A key-value pair RDD with realignment targets matched with sets of reads.
   *
   * @see mapToTarget
   */
  def mapTargets(
    rich_rdd: RDD[RichAlignmentRecord],
    targets: Array[IndelRealignmentTarget],
    maxReadsPerTarget: Int = Int.MaxValue): RDD[(Option[(Int, IndelRealignmentTarget)], Iterable[RichAlignmentRecord])] = MapTargets.time {

    // group reads by target
    val broadcastTargets = rich_rdd.context.broadcast(targets)
    val targetSize = targets.length
    log.info("Mapping reads to %d targets.".format(targetSize))
    val targetsToDrop = rich_rdd.flatMap(r => {
      Some(mapToTarget(r, broadcastTargets.value, 0, targetSize))
        .filter(_ >= 0)
        .map(v => (v, 1))
    }).reduceByKey(_ + _)
      .filter(p => p._2 >= maxReadsPerTarget)
      .map(_._1)
      .collect
      .toSet
    val bcastTargetsToDrop = rich_rdd.context.broadcast(targetsToDrop)
    val readsMappedToTarget = rich_rdd.groupBy((r: RichAlignmentRecord) => {
      mapToTarget(r,
        broadcastTargets.value,
        0,
        targetSize,
        targetsToDrop = bcastTargetsToDrop.value)
    }, ModPartitioner(rich_rdd.partitions.length))
      .map(kv => {
        val (k, v) = kv

        if (k < 0) {
          (None, v)
        } else {
          (Some((k, broadcastTargets.value(k))), v)
        }
      })

    readsMappedToTarget
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
    val maxTargetSize: Int = 3000,
    val maxReadsPerTarget: Int = 20000,
    val optReferenceFile: Option[ReferenceFile] = None,
    val falloff: Int = 1) extends Serializable with Logging {
  require(falloff >= 1, "Falloff (%d) must be >= 1".format(falloff))

  /**
   * Given a target group with an indel realignment target and a group of reads to realign, this method
   * generates read consensuses and realigns reads if a consensus leads to a sufficient improvement.
   *
   * @param targetGroup A tuple consisting of an indel realignment target and a seq of reads
   * @param partitionIdx The ID of the partition this is on. Only used for logging.
   * @return A sequence of reads which have either been realigned if there is a sufficiently good alternative
   * consensus, or not realigned if there is not a sufficiently good consensus.
   */
  def realignTargetGroup(targetGroup: (Option[(Int, IndelRealignmentTarget)], Iterable[RichAlignmentRecord]),
                         partitionIdx: Int = 0): Iterable[RichAlignmentRecord] = RealignTargetGroup.time {
    val (target, reads) = targetGroup

    if (target.isEmpty) {
      // if the indel realignment target is empty, do not realign
      reads
    } else {
      try {
        val (targetIdx, _) = target.get
        val startTime = System.nanoTime()
        // bootstrap realigned read set with the reads that need to be realigned
        val (realignedReads, readsToRealign) = reads.partition(r => r.mdTag.exists(!_.hasMismatches))

        // get reference from reads
        val refStart = reads.map(_.getStart).min
        val refEnd = reads.map(_.getEnd).max
        val refRegion = ReferenceRegion(reads.head.record.getContigName, refStart, refEnd)
        val reference = optReferenceFile.fold(getReferenceFromReads(reads.map(r => new RichAlignmentRecord(r)))._1)(rf => GetReferenceFromFile.time { rf.extract(refRegion) })

        // preprocess reads and get consensus
        val readsToClean = consensusModel.preprocessReadsForRealignment(
          readsToRealign,
          reference,
          refRegion
        ).zipWithIndex
        val observedConsensus = consensusModel.findConsensus(reads)
          .toSeq
          .distinct

        // reduce count of consensus sequences
        val consensus = if (observedConsensus.size > maxConsensusNumber) {
          val r = new Random()
          r.shuffle(observedConsensus).take(maxConsensusNumber)
        } else {
          observedConsensus
        }

        val finalReads = if (readsToClean.size > 0 && consensus.size > 0) {

          // do not check realigned reads - they must match
          val mismatchQualities = ComputingOriginalScores.time {
            readsToClean.map(r => sumMismatchQuality(r._1)).toArray
          }
          val totalMismatchSumPreCleaning = mismatchQualities.sum

          /* list to log the outcome of all consensus trials. stores:
           *  - mismatch quality of reads against new consensus sequence
           *  - the consensus sequence itself
           *  - a map containing each realigned read and it's offset into the new sequence
           */
          val consensusOutcomes = new Array[(Int, Consensus, Map[RichAlignmentRecord, Int])](consensus.size)

          // loop over all consensuses and evaluate
          consensus.zipWithIndex.foreach(p => SweepReadsOverConsensus.time {
            val (c, cIdx) = p

            // generate a reference sequence from the consensus
            val consensusSequence = c.insertIntoReference(reference, refRegion)

            // evaluate all reads against the new consensus
            val sweptValues = readsToClean.map(p => {
              val (r, rIdx) = p
              val originalQual = mismatchQualities(rIdx)
              val qualAndPos = sweepReadOverReferenceForQuality(r.getSequence, consensusSequence, r.qualityScores, originalQual)

              (r, qualAndPos)
            })

            // sum all mismatch qualities to get the total mismatch quality for this alignment
            val totalQuality = sweptValues.map(_._2._1).sum

            // package data
            val readMappings = sweptValues.map(kv => (kv._1, kv._2._2)).toMap

            // add to outcome list
            consensusOutcomes(cIdx) = (totalQuality, c, readMappings)
          })

          // perform reduction to pick the consensus with the lowest aggregated mismatch score
          val bestConsensusTuple = consensusOutcomes.minBy(_._1)

          val (bestConsensusMismatchSum, bestConsensus, bestMappings) = bestConsensusTuple

          // check for a sufficient improvement in mismatch quality versus threshold
          log.info("On " + refRegion + ", before realignment, sum was " + totalMismatchSumPreCleaning +
            ", best realignment has " + bestConsensusMismatchSum)
          val lodImprovement = (totalMismatchSumPreCleaning - bestConsensusMismatchSum).toDouble / 10.0
          if (lodImprovement > lodThreshold) {
            FinalizingRealignments.time {
              var realignedReadCount = 0

              // generate a reference sequence from the consensus
              val consensusSequence = bestConsensus.insertIntoReference(reference, refRegion)

              // if we see a sufficient improvement, realign the reads
              val cleanedReads: Iterable[RichAlignmentRecord] = readsToClean.map(p => {
                val (r, rIdx) = p

                try {
                  val remapping = bestMappings(r)

                  val finalRemapping = if (remapping != -1) {
                    remapping
                  } else {
                    val originalQual = mismatchQualities(rIdx)
                    val (newScore, newRemapping) = sweepReadOverReferenceForQuality(r.getSequence,
                      consensusSequence,
                      r.qualityScores,
                      originalQual * falloff)
                    if (newRemapping != -1) {
                      newRemapping
                    } else {
                      -1
                    }
                  }

                  // if read alignment is improved by aligning against new consensus, realign
                  if (finalRemapping != -1) {

                    // if element overlaps with consensus indel, modify cigar with indel
                    val (idElement, endLength, endPenalty) = if (bestConsensus.index.start == bestConsensus.index.end - 1) {
                      // number of bases after the end:
                      // sequenceLength - bases before consensus - consensus length
                      // L_s            - (S_c - (S_r + remap)   - L_c
                      // L_s = length of sequence
                      // S_c = start of consensus insertion
                      // S_r = start of reference interval
                      // remap = read remapping index
                      // L_c = length of the insertion
                      val endBases = r.getSequence.length - ((bestConsensus.index.start - (refStart + finalRemapping) + 1) + bestConsensus.consensus.length)

                      (new CigarElement(bestConsensus.consensus.length, CigarOperator.I),
                        endBases,
                        -bestConsensus.consensus.length)
                    } else {
                      (new CigarElement((bestConsensus.index.end - 1 - bestConsensus.index.start).toInt, CigarOperator.D),
                        r.getSequence.length - (bestConsensus.index.start - (refStart + finalRemapping)),
                        bestConsensus.consensus.length)
                    }
                    assert(endLength > -bestConsensus.consensus.length,
                      "Read is remapped but does not overlap the consensus (el = %d).".format(endLength))
                    val alignmentLength = r.getSequence.length + endPenalty

                    if (alignmentLength <= 0) {
                      log.warn("Read %s was realigned with a negative/zero alignment length (%d at position %d) on target %s. Skipping...".format(
                        r, alignmentLength, finalRemapping, target))
                      r
                    } else {
                      realignedReadCount += 1
                      val builder: AlignmentRecord.Builder = AlignmentRecord.newBuilder(r)

                      // bump up mapping quality by 10
                      builder.setMapq(r.getMapq + 10)

                      // set new start to consider offset
                      val startLength = bestConsensus.index.start - (refStart + finalRemapping)
                      val newStart = if (finalRemapping <= (bestConsensus.index.start - refStart)) {
                        refStart + finalRemapping
                      } else {
                        assert(startLength < 0)
                        // if read remaps into consensus variant, then it should be
                        // soft clipped, and should map after the variant
                        bestConsensus.index.end
                      }
                      builder.setStart(newStart)
                      if (endLength > 0) {
                        builder.setEnd(bestConsensus.index.end + endLength)
                      } else {
                        builder.setEnd(bestConsensus.index.start + 1)
                      }

                      val adjustedIdElement = if (endLength < 0) {
                        new CigarElement(idElement.getLength + endLength.toInt, CigarOperator.S)
                      } else if (startLength < 0) {
                        new CigarElement(idElement.getLength + startLength.toInt + 1, CigarOperator.S)
                      } else {
                        idElement
                      }
                      val cigarElements = if (bestConsensus.consensus.length > 0) {
                        List[CigarElement](
                          new CigarElement((bestConsensus.index.start - (refStart + finalRemapping) + 1).toInt, CigarOperator.M),
                          adjustedIdElement,
                          new CigarElement(endLength.toInt, CigarOperator.M)
                        ).filter(_.getLength > 0)
                      } else {
                        List[CigarElement](
                          new CigarElement((bestConsensus.index.start - (refStart + finalRemapping)).toInt, CigarOperator.M),
                          adjustedIdElement,
                          new CigarElement(endLength.toInt, CigarOperator.M)
                        ).filter(_.getLength > 0)
                      }

                      val newCigar = new Cigar(cigarElements)

                      // update mdtag and cigar
                      builder.setMismatchingPositions(MdTag.moveAlignment(r,
                        newCigar,
                        reference.drop((newStart - refStart).toInt),
                        newStart).toString())
                      builder.setOldPosition(r.getStart())
                      builder.setOldCigar(r.getCigar())
                      builder.setCigar(newCigar.toString)
                      val rec = builder.build()
                      new RichAlignmentRecord(rec)
                    }
                  } else {
                    r
                  }
                } catch {
                  case t: Throwable => {
                    log.warn("Realigning read %s failed with %s.".format(r, t))
                    r
                  }
                }
              })

              log.info("On " + refRegion + ", realigned " + realignedReadCount + " reads to " +
                bestConsensus + " due to LOD improvement of " + lodImprovement)

              cleanedReads ++ realignedReads
            }
          } else {
            log.info("On " + refRegion + ", skipping realignment due to insufficient LOD improvement (" +
              lodImprovement + "for consensus " + bestConsensus)
            reads
          }
        } else {
          reads
        }
        // return all reads that we cleaned and all reads that were initially realigned
        val endTime = System.nanoTime()
        log.info("TARGET|\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%d".format(partitionIdx,
          targetIdx,
          endTime - startTime,
          refRegion.referenceName,
          refRegion.start,
          refRegion.end,
          reads.size,
          observedConsensus.size,
          consensus.size))
        finalReads
      } catch {
        case t: Throwable => {
          log.warn("Realigning target %s failed with %s.".format(target, t))
          reads
        }
      }
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
   * @param originalQuality The original score of the read vs. the reference genome.
   * @return Tuple of (mismatch quality score, alignment offset).
   */
  def sweepReadOverReferenceForQuality(read: String, reference: String, qualities: Seq[Int], originalQuality: Int): (Int, Int) = SweepReadOverReferenceForQuality.time {

    @tailrec def sweep(i: Int,
                       upTo: Int,
                       minScore: Int,
                       minPos: Int): (Int, Int) = {
      if (i >= upTo) {
        (minScore, minPos)
      } else {
        val qualityScore = sumMismatchQualityIgnoreCigar(read, reference, qualities, minScore, i)
        val (newMinScore, newMinPos) = if (qualityScore <= minScore) {
          (qualityScore, i)
        } else {
          (minScore, minPos)
        }
        sweep(i + 1, upTo, newMinScore, newMinPos)
      }
    }

    // calculate mismatch quality score for all admissable alignment offsets
    sweep(0, reference.length - read.length + 1, originalQuality, -1)
  }

  /**
   * Sums the mismatch quality of a read against a reference. Mismatch quality is defined as the sum
   * of the base quality for all bases in the read that do not match the reference. This method
   * ignores the cigar string, which treats indels as causing mismatches.
   *
   * @param read Read to evaluate.
   * @param reference Reference sequence to look for mismatches against.
   * @param qualities Sequence of base quality scores.
   * @param scoreThreshold Stops summing if score is greater than this value.
   * @param refOffset Offset into the reference sequence.
   * @return Mismatch quality sum.
   */
  def sumMismatchQualityIgnoreCigar(read: String,
                                    reference: String,
                                    qualities: Seq[Int],
                                    scoreThreshold: Int,
                                    refOffset: Int): Int = {

    @tailrec def loopAndSum(idx: Int = 0,
                            runningSum: Int = 0): Int = {
      if (idx >= read.length || (idx + refOffset) >= reference.length) {
        runningSum
      } else if (runningSum > scoreThreshold) {
        Int.MaxValue
      } else {
        val newRunningSum = if (read(idx) == reference(idx + refOffset) ||
          reference(idx + refOffset) == '_') {
          runningSum
        } else {
          runningSum + qualities(idx)
        }
        loopAndSum(idx + 1, newRunningSum)
      }
    }

    loopAndSum()
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
      read.mdTag.get.getReference(read, withGaps = true),
      read.qualityScores,
      Int.MaxValue,
      0
    )
  }

  /**
   * Performs realignment for an RDD of reads. This includes target generation, read/target
   * classification, and read realignment.
   *
   * @param rdd Reads to realign.
   * @return Realigned read.
   */
  def realignIndels(rdd: RDD[AlignmentRecord]): RDD[AlignmentRecord] = {
    val sortedRdd = if (dataIsSorted) {
      rdd.filter(r => r.getReadMapped)
    } else {
      val sr = rdd.filter(r => r.getReadMapped)
        .keyBy(ReferencePosition(_))
        .sortByKey()
      sr.map(kv => kv._2)
    }

    // we only want to convert once so let's get it over with
    val richRdd = sortedRdd.map(new RichAlignmentRecord(_))
    richRdd.cache()

    // find realignment targets
    log.info("Generating realignment targets...")
    val targets: Array[IndelRealignmentTarget] = RealignmentTargetFinder(
      richRdd,
      maxIndelSize,
      maxTargetSize
    ).toArray

    // we should only attempt realignment if the target set isn't empty
    if (targets.isEmpty) {
      val readRdd = richRdd.map(r => r.record)
      richRdd.unpersist()
      readRdd
    } else {
      // map reads to targets
      log.info("Grouping reads by target...")
      val readsMappedToTarget = RealignIndels.mapTargets(richRdd,
        targets,
        maxReadsPerTarget = maxReadsPerTarget)
      richRdd.unpersist()

      // realign target groups
      log.info("Sorting reads by reference in ADAM RDD")
      readsMappedToTarget.mapPartitionsWithIndex((idx, iter) => {
        iter.flatMap(realignTargetGroup(_, idx))
      }).map(r => r.record)
    }
  }
}
