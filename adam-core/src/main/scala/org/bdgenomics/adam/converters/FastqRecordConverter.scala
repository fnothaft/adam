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
package org.bdgenomics.adam.converters

import org.apache.hadoop.io.Text
import org.apache.spark.Logging
import org.bdgenomics.formats.avro.AlignmentRecord

class FastqRecordConverter extends Serializable with Logging {

  def convertPair(element: (Void, Text)): Iterable[AlignmentRecord] = {
    val lines = element._2.toString.split('\n')
    assert(lines.length == 8, "Record has wrong format:\n" + element._2.toString)

    // get fields for first read in pair
    val firstReadName = lines(0).drop(1)
    val firstReadSequence = lines(1)
    val firstReadQualities = lines(3)

    assert(firstReadSequence.length == firstReadQualities.length,
      "Read " + firstReadName + " has different sequence and qual length.")

    // get fields for second read in pair
    val secondReadName = lines(4).drop(1)
    val secondReadSequence = lines(5)
    val secondReadQualities = lines(7)

    assert(secondReadSequence.length == secondReadQualities.length,
      "Read " + secondReadName + " has different sequence and qual length.")

    // build and return iterators
    Iterable(AlignmentRecord.newBuilder()
      .setReadName(firstReadName)
      .setSequence(firstReadSequence)
      .setQual(firstReadQualities)
      .setReadPaired(true)
      .setProperPair(true)
      .setReadNum(0)
      .setReadNegativeStrand(null)
      .setMateNegativeStrand(null)
      .setPrimaryAlignment(null)
      .setSecondaryAlignment(null)
      .setSupplementaryAlignment(null)
      .build(),
      AlignmentRecord.newBuilder()
        .setReadName(secondReadName)
        .setSequence(secondReadSequence)
        .setQual(secondReadQualities)
        .setReadPaired(true)
        .setProperPair(true)
        .setReadNum(1)
        .setReadNegativeStrand(null)
        .setMateNegativeStrand(null)
        .setPrimaryAlignment(null)
        .setSecondaryAlignment(null)
        .setSupplementaryAlignment(null)
        .build())
  }

  def convertRead(element: (Void, Text)): AlignmentRecord = {
    val lines = element._2.toString.split('\n')
    assert(lines.length == 4, "Record has wrong format:\n" + element._2.toString)

    // get fields for first read in pair
    val readName = lines(0).drop(1)
    val readSequence = lines(1)
    val readQualities = lines(3)

    assert(readSequence.length == readQualities.length,
      "Read " + readName + " has different sequence and qual length.")

    AlignmentRecord.newBuilder()
      .setReadName(readName)
      .setSequence(readSequence)
      .setQual(readQualities)
      .setReadPaired(false)
      .setProperPair(null)
      .setReadNum(0)
      .setReadNegativeStrand(null)
      .setMateNegativeStrand(null)
      .setPrimaryAlignment(null)
      .setSecondaryAlignment(null)
      .setSupplementaryAlignment(null)
      .build()
  }
}
