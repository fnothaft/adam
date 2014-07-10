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
package org.bdgenomics.adam.models

import org.scalatest.FunSuite
import org.bdgenomics.formats.avro.{ ADAMContig, ADAMGenotype, ADAMPileup, ADAMRecord, ADAMVariant }

class ReferencePositionSuite extends FunSuite {

  test("create reference position from mapped read") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr1")
      .build

    val read = ADAMRecord.newBuilder()
      .setContig(contig)
      .setStart(1L)
      .setReadMapped(true)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isDefined)

    val refPos = refPosOpt.get

    assert(refPos.referenceName === "chr1")
    assert(refPos.pos === 1L)
  }

  test("create reference position from unmapped read") {
    val read = ADAMRecord.newBuilder()
      .setReadMapped(false)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isEmpty)
  }

  test("create reference position from mapped read but contig not specified") {
    val read = ADAMRecord.newBuilder()
      .setReadMapped(true)
      .setStart(1L)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isEmpty)
  }

  test("create reference position from mapped read but contig is underspecified") {
    val contig = ADAMContig.newBuilder
      // contigName is NOT set
      //.setContigName("chr1")
      .build

    val read = ADAMRecord.newBuilder()
      .setReadMapped(true)
      .setStart(1L)
      .setContig(contig)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isEmpty)
  }

  test("create reference position from mapped read but start not specified") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr1")
      .build

    val read = ADAMRecord.newBuilder()
      .setReadMapped(true)
      .setContig(contig)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isEmpty)
  }

  test("create reference position from pileup") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr2")
      .build

    val pileup = ADAMPileup.newBuilder()
      .setPosition(2L)
      .setContig(contig)
      .build()

    val refPos = ReferencePosition(pileup)

    assert(refPos.referenceName === "chr2")
    assert(refPos.pos === 2L)
  }

  test("create reference position from variant") {
    val variant = ADAMVariant.newBuilder()
      .setContig(ADAMContig.newBuilder.setContigName("chr10").build())
      .setReferenceAllele("A")
      .setVariantAllele("T")
      .setPosition(10L)
      .build()

    val refPos = ReferencePosition(variant)

    assert(refPos.referenceName === "chr10")
    assert(refPos.pos === 10L)
  }

  test("create reference position from genotype") {
    val variant = ADAMVariant.newBuilder()
      .setPosition(100L)
      .setContig(ADAMContig.newBuilder.setContigName("chr10").build())
      .setReferenceAllele("A")
      .setVariantAllele("T")
      .build()
    val genotype = ADAMGenotype.newBuilder()
      .setVariant(variant)
      .setSampleId("NA12878")
      .build()

    val refPos = ReferencePosition(genotype)

    assert(refPos.referenceName === "chr10")
    assert(refPos.pos === 100L)
  }

  test("lift over between two transcripts on the forward strand") {
    // create mappings for transcripts
    val t1 = Seq(ReferenceRegionWithOrientation("chr0", 0L, 201L))
    val t2 = Seq(ReferenceRegionWithOrientation("chr0", 50L, 101L),
      ReferenceRegionWithOrientation("chr0", 175L, 201L))

    // check forward strand
    val pos = ReferencePositionWithOrientation.liftOverToReference(60, t1)

    assert(pos.refPos.isDefined)
    assert(pos.refPos.get.referenceName === "chr0")
    assert(pos.refPos.get.pos === 60L)
    assert(!pos.negativeStrand)

    val idx = pos.liftOverFromReference(t2)

    assert(idx === 10L)
  }

  test("lift over between two transcripts on the reverse strand") {
    // create mappings for transcripts
    val t1 = Seq(ReferenceRegionWithOrientation("chr0", 201L, 0L))
    val t2 = Seq(ReferenceRegionWithOrientation("chr0", 201L, 175L),
      ReferenceRegionWithOrientation("chr0", 101L, 50L))

    // check reverse strand
    val idx = ReferencePositionWithOrientation(Some(ReferencePosition("chr0", 190L)), true)
      .liftOverFromReference(t2)

    assert(idx === 11L)

    val pos = ReferencePositionWithOrientation.liftOverToReference(idx, t1)

    assert(pos.refPos.isDefined)
    assert(pos.refPos.get.referenceName === "chr0")
    assert(pos.refPos.get.pos === 190L)
    assert(pos.negativeStrand)
  }
}
