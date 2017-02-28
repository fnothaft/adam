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
import org.bdgenomics.adam.algorithms.consensus.{
  Consensus,
  ConsensusGenerator
}
import org.bdgenomics.adam.models.{ ReferencePosition, ReferenceRegion }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variant.VariantRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.{ ADAMFunSuite, ReferenceFile }
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig, Variant }
import scala.collection.immutable.TreeSet

class RealignIndelsSuite extends ADAMFunSuite {

  def artificialReadsRdd: AlignmentRecordRDD = {
    val path = testFile("artificial.sam")
    sc.loadAlignments(path)
  }

  def artificialReads: RDD[AlignmentRecord] = {
    artificialReadsRdd.rdd
  }

  def artificialRealignedReads(cg: ConsensusGenerator = ConsensusGenerator.fromReads,
                               maxCoverage: Int = 3000,
                               optRefFile: Option[ReferenceFile] = None): RDD[AlignmentRecord] = {
    artificialReadsRdd
      .realignIndels(consensusModel = cg,
        maxReadsPerTarget = maxCoverage,
        optReferenceFile = optRefFile)
      .sortReadsByReferencePosition()
      .rdd
  }

  def gatkArtificialRealignedReads: RDD[AlignmentRecord] = {
    val path = testFile("artificial.realigned.sam")
    sc.loadAlignments(path).rdd
  }

  def makeRead(start: Long, end: Long): RichAlignmentRecord = {
    RichAlignmentRecord(AlignmentRecord.newBuilder()
      .setContigName("ctg")
      .setStart(start)
      .setEnd(end)
      .setReadMapped(true)
      .build())
  }

  test("map reads to targets") {
    val targets = Array(
      new IndelRealignmentTarget(None, ReferenceRegion("ctg", 1L, 4L)),
      new IndelRealignmentTarget(None, ReferenceRegion("ctg", 10L, 44L)),
      new IndelRealignmentTarget(None, ReferenceRegion("ctg", 100L, 400L)))

    assert(RealignIndels.mapToTarget(makeRead(0L, 1L), targets, 0, 3) < 0)
    assert(RealignIndels.mapToTarget(makeRead(0L, 2L), targets, 0, 3) === 0)
    assert(RealignIndels.mapToTarget(makeRead(1L, 2L), targets, 0, 3) === 0)
    assert(RealignIndels.mapToTarget(makeRead(3L, 6L), targets, 0, 3) === 0)
    assert(RealignIndels.mapToTarget(makeRead(6L, 8L), targets, 0, 3) < 0)
    assert(RealignIndels.mapToTarget(makeRead(8L, 12L), targets, 0, 3) === 1)
    assert(RealignIndels.mapToTarget(makeRead(10L, 12L), targets, 0, 3) === 1)
    assert(RealignIndels.mapToTarget(makeRead(14L, 36L), targets, 0, 3) === 1)
    assert(RealignIndels.mapToTarget(makeRead(35L, 50L), targets, 0, 3) === 1)
    assert(RealignIndels.mapToTarget(makeRead(45L, 50L), targets, 0, 3) < 0)
    assert(RealignIndels.mapToTarget(makeRead(90L, 100L), targets, 0, 3) < 0)
    assert(RealignIndels.mapToTarget(makeRead(90L, 101L), targets, 0, 3) === 2)
    assert(RealignIndels.mapToTarget(makeRead(200L, 300L), targets, 0, 3) === 2)
    assert(RealignIndels.mapToTarget(makeRead(200L, 600L), targets, 0, 3) === 2)
    assert(RealignIndels.mapToTarget(makeRead(700L, 1000L), targets, 0, 3) < 0)
  }

  sparkTest("checking mapping to targets for artificial reads") {
    val targets = RealignmentTargetFinder(artificialReads.map(RichAlignmentRecord(_))).toArray
    assert(targets.size === 1)
    val rr = artificialReads.map(RichAlignmentRecord(_))
    val readsMappedToTarget = RealignIndels.mapTargets(rr, targets).map(kv => {
      val (t, r) = kv

      (t, r.map(r => r.record))
    }).collect()

    assert(readsMappedToTarget.count(_._1.isDefined) === 1)

    assert(readsMappedToTarget.forall {
      case (target: Option[(Int, IndelRealignmentTarget)], reads: Seq[AlignmentRecord]) => reads.forall {
        read =>
          {
            if (read.getStart <= 25) {
              val result = target.get._2.readRange.start <= read.getStart.toLong
              result && (target.get._2.readRange.end >= read.getEnd)
            } else {
              target.isEmpty
            }
          }
      }
      case _ => false
    })
  }

  sparkTest("checking alternative consensus for artificial reads") {
    var consensus = List[Consensus]()

    // similar to realignTargetGroup() in RealignIndels
    artificialReads.collect().toList.foreach(r => {
      if (r.mdTag.get.hasMismatches) {
        consensus = Consensus.generateAlternateConsensus(r.getSequence, ReferencePosition("0", r.getStart), r.samtoolsCigar) match {
          case Some(o) => o :: consensus
          case None    => consensus
        }
      }
    })
    consensus = consensus.distinct
    assert(consensus.length > 0)
    // Note: it seems that consensus ranges are non-inclusive
    assert(consensus(0).index.start === 34)
    assert(consensus(0).index.end === 45)
    assert(consensus(0).consensus === "")
    assert(consensus(1).index.start === 54)
    assert(consensus(1).index.end === 65)
    assert(consensus(1).consensus === "")
    // TODO: add check with insertions, how about SNPs
  }

  sparkTest("checking extraction of reference from reads") {
    def checkReference(readReference: (String, Long, Long)) {
      // the first three lines of artificial.fasta
      val refStr = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      val startIndex = readReference._2.toInt
      val stopIndex = readReference._3.toInt
      assert(readReference._1.length === stopIndex - startIndex)
      assert(readReference._1 === refStr.substring(startIndex, stopIndex))
    }

    val targets = RealignmentTargetFinder(artificialReads.map(RichAlignmentRecord(_))).toArray
    val rr = artificialReads.map(RichAlignmentRecord(_))
    val readsMappedToTarget: Array[((Int, IndelRealignmentTarget), Iterable[AlignmentRecord])] = RealignIndels.mapTargets(rr, targets)
      .filter(_._1.isDefined)
      .map(kv => {
        val (t, r) = kv

        (t.get, r.map(r => r.record))
      }).collect()

    val readReference = readsMappedToTarget.map {
      case ((_, target), reads) =>
        if (!target.isEmpty) {
          val referenceFromReads: (String, Long, Long) = RealignIndels.getReferenceFromReads(reads.map(r => new RichAlignmentRecord(r)).toSeq)
          assert(referenceFromReads._2 == -1 || referenceFromReads._1.length > 0)
          checkReference(referenceFromReads)
        }
      case _ => throw new AssertionError("Mapping should contain target and reads")
    }
    assert(readReference != null)
  }

  sparkTest("checking realigned reads for artificial input") {
    val artificialRealignedReadsCollected = artificialRealignedReads()
      .collect()
    val gatkArtificialRealignedReadsCollected = gatkArtificialRealignedReads
      .collect()

    assert(artificialRealignedReadsCollected.size === gatkArtificialRealignedReadsCollected.size)

    val artificialRead4 = artificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val gatkRead4 = gatkArtificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val result = artificialRead4.zip(gatkRead4)

    assert(result.forall(
      pair => pair._1.getReadName == pair._2.getReadName))
    assert(result.forall(
      pair => pair._1.getStart == pair._2.getStart))
    assert(result.forall(
      pair => pair._1.getCigar == pair._2.getCigar))
    assert(result.forall(
      pair => pair._1.getMapq == pair._2.getMapq))
  }

  sparkTest("checking realigned reads for artificial input with reference file") {
    val artificialRealignedReadsCollected = artificialRealignedReads(optRefFile = Some(sc.loadReferenceFile(testFile("artificial.fa"), 1000)))
      .collect()
    val gatkArtificialRealignedReadsCollected = gatkArtificialRealignedReads
      .collect()

    assert(artificialRealignedReadsCollected.size === gatkArtificialRealignedReadsCollected.size)

    val artificialRead4 = artificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val gatkRead4 = gatkArtificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val result = artificialRead4.zip(gatkRead4)

    assert(result.forall(
      pair => pair._1.getReadName == pair._2.getReadName))
    assert(result.forall(
      pair => pair._1.getStart == pair._2.getStart))
    assert(result.forall(
      pair => pair._1.getCigar == pair._2.getCigar))
    assert(result.forall(
      pair => pair._1.getMapq == pair._2.getMapq))
  }

  sparkTest("checking realigned reads for artificial input using knowns") {
    val indel = Variant.newBuilder()
      .setContigName("artificial")
      .setStart(33)
      .setEnd(44)
      .setReferenceAllele("AGGGGGGGGGG")
      .setAlternateAllele("A")
      .build
    val variantRdd = VariantRDD(sc.parallelize(Seq(indel)),
      artificialReadsRdd.sequences)
    val knowns = ConsensusGenerator.fromKnownIndels(variantRdd)
    val artificialRealignedReadsCollected = artificialRealignedReads(cg = knowns)
      .collect()
    val gatkArtificialRealignedReadsCollected = gatkArtificialRealignedReads
      .collect()

    assert(artificialRealignedReadsCollected.size === gatkArtificialRealignedReadsCollected.size)

    val artificialRead4 = artificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val gatkRead4 = gatkArtificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val result = artificialRead4.zip(gatkRead4)

    assert(result.forall(
      pair => pair._1.getReadName == pair._2.getReadName))
    assert(result.forall(
      pair => pair._1.getStart == pair._2.getStart))
    assert(result.forall(
      pair => pair._1.getCigar == pair._2.getCigar))
    assert(result.forall(
      pair => pair._1.getMapq == pair._2.getMapq))
  }

  sparkTest("checking realigned reads for artificial input using knowns and reads") {
    val indel = Variant.newBuilder()
      .setContigName("artificial")
      .setStart(33)
      .setEnd(44)
      .setReferenceAllele("AGGGGGGGGGG")
      .setAlternateAllele("A")
      .build
    val variantRdd = VariantRDD(sc.parallelize(Seq(indel)),
      artificialReadsRdd.sequences)
    val knowns = ConsensusGenerator.fromKnownIndels(variantRdd)
    val union = ConsensusGenerator.union(knowns, ConsensusGenerator.fromReads)
    val artificialRealignedReadsCollected = artificialRealignedReads(cg = union)
      .collect()
    val gatkArtificialRealignedReadsCollected = gatkArtificialRealignedReads
      .collect()

    assert(artificialRealignedReadsCollected.size === gatkArtificialRealignedReadsCollected.size)

    val artificialRead4 = artificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val gatkRead4 = gatkArtificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val result = artificialRead4.zip(gatkRead4)

    assert(result.forall(
      pair => pair._1.getReadName == pair._2.getReadName))
    assert(result.forall(
      pair => pair._1.getStart == pair._2.getStart))
    assert(result.forall(
      pair => pair._1.getCigar == pair._2.getCigar))
    assert(result.forall(
      pair => pair._1.getMapq == pair._2.getMapq))
  }

  sparkTest("skip realigning reads if target is highly covered") {
    val artificialRealignedReadsCollected = artificialRealignedReads(maxCoverage = 0)
      .collect()
    val reads = artificialReads
    val result = artificialRealignedReadsCollected.zip(reads.collect())

    assert(result.forall(
      pair => pair._1.getReadName == pair._2.getReadName))
    assert(result.forall(
      pair => pair._1.getStart == pair._2.getStart))
    assert(result.forall(
      pair => pair._1.getCigar == pair._2.getCigar))
    assert(result.forall(
      pair => pair._1.getMapq == pair._2.getMapq))
  }

  test("test mismatch quality scoring") {
    val ri = new RealignIndels()
    val read = "AAAAAAAA"
    val ref = "AAGGGGAA"
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, ref, qScores, Int.MaxValue, 0) === 160)
  }

  test("test mismatch quality scoring for no mismatches") {
    val ri = new RealignIndels()
    val read = "AAAAAAAA"
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, read, qScores, Int.MaxValue, 0) === 0)
  }

  test("test mismatch quality scoring for offset") {
    val ri = new RealignIndels()
    val read = "AAAAAAAA"
    val ref = "G%s".format(read)
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, ref, qScores, Int.MaxValue, 1) === 0)
  }

  test("test mismatch quality scoring with early exit") {
    val ri = new RealignIndels()
    val read = "AAAAAAAA"
    val ref = "AAGGGGAA"
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, ref, qScores, 120, 0) === Int.MaxValue)
  }

  sparkTest("test mismatch quality scoring after unpacking read") {
    val ri = new RealignIndels()
    val read = artificialReads.first()

    assert(ri.sumMismatchQuality(read) === 800)
  }

  test("we shouldn't try to realign a region with no target") {
    val ctg = Contig.newBuilder()
      .setContigName("chr1")
      .build()
    val reads = Seq(AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(1L)
      .setEnd(4L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(9L)
      .setEnd(12L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build()).map(RichAlignmentRecord(_))
      .toIterable
    val ri = new RealignIndels()

    // this should be a NOP
    assert(ri.realignTargetGroup((None.asInstanceOf[Option[(Int, IndelRealignmentTarget)]],
      reads)).size === 2)
  }

  sparkTest("we shouldn't try to realign reads with no indel evidence") {
    val ctg = Contig.newBuilder()
      .setContigName("chr1")
      .build()
    val reads = sc.parallelize(Seq(AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(1L)
      .setEnd(4L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(10L)
      .setEnd(13L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(4L)
      .setEnd(7L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(7L)
      .setEnd(10L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build()))

    // this should be a NOP
    assert(RealignIndels(reads).count === 4)
  }

  sparkTest("test OP and OC tags") {
    artificialRealignedReads()
      .collect()
      .foreach(realn => {
        val readName = realn.getReadName()
        val op = realn.getOldPosition()
        val oc = realn.getOldCigar()

        Option(op).filter(_ >= 0).foreach(oPos => {
          val s = artificialReads.collect().filter(x => (x.getReadName() == readName))
          assert(s.filter(x => (x.getStart() == oPos)).length > 0)
          assert(s.filter(x => (x.getCigar() == oc)).length > 0)
        })
      })
  }
}
