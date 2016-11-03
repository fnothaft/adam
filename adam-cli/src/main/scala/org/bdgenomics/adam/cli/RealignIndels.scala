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
package org.bdgenomics.adam.cli

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.algorithms.consensus._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.realignment.{
  IndelRealignmentTarget,
  RealignIndels => IndelRealigner
}
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object RealignIndels extends BDGCommandCompanion {
  val commandName = "realignIndels"
  val commandDescription = "Runs the ADAM indel realigner in a loop"

  def apply(cmdLine: Array[String]) = {
    new RealignIndels(Args4j[RealignIndelsArgs](cmdLine))
  }
}

class RealignIndelsArgs extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM, BAM or SAM file to apply the transforms to", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "WARMUPS", usage = "Number of warmup iterations to run.", index = 1)
  var warmupsToRun: Int = 100
  @Argument(required = true, metaVar = "ITERATIONS", usage = "Number of iterations to run.", index = 2)
  var iterationsToRun: Int = 1000
}

class RealignIndels(protected val args: RealignIndelsArgs) extends BDGSparkCommand[RealignIndelsArgs] with Logging {
  val companion = RealignIndels

  def run(sc: SparkContext) {

    // load reads and collect them as an iterable collection
    val reads = sc.loadAlignments(args.inputPath)
      .sortReadsByReferencePosition()
      .rdd
      .map(read => {
        new RichAlignmentRecord(read)
      }).collect()
      .toIterable

    // we just need a dummy target
    val target = new IndelRealignmentTarget(None, ReferenceRegion("unk", 1L, 1000L))

    // build an indel realigner with default parameters
    val indelRealigner = new IndelRealigner()

    println("Realigning %d reads for %d warmup loops and %d test loops.".format(
      reads.size, args.warmupsToRun, args.iterationsToRun))

    // we need to run this for a predefined warmup period
    var realignedReads = 0

    val warmupStartTime = System.nanoTime

    (0 to args.warmupsToRun).foreach(idx => {

      val newReads = indelRealigner.realignTargetGroup((Some(target), reads))
      realignedReads += newReads.size
    })

    val warmupEndTime = System.nanoTime

    println("In %d warmup iterations, we realigned %d reads. Took %dns.".format(args.warmupsToRun,
      realignedReads,
      (warmupEndTime - warmupStartTime)))

    // reset the counter and run the real iterations
    realignedReads = 0
    val startTime = System.nanoTime

    (0 to args.iterationsToRun).foreach(idx => {

      val newReads = indelRealigner.realignTargetGroup((Some(target), reads))
      realignedReads += newReads.size
    })

    val endTime = System.nanoTime

    println("In %d test iterations, we realigned %d reads. Took %dns".format(args.iterationsToRun,
      realignedReads,
      (endTime - startTime)))
  }
}
