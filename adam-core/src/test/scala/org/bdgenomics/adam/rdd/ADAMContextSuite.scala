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

import java.io.File
import java.util.UUID
import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.predicates.HighQualityReadPredicate
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.PhredUtils._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._

class ADAMContextSuite extends ADAMFunSuite {

  sparkTest("sc.adamLoad should not fail on unmapped reads") {
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("unmapped.sam").getFile

    // Convert the reads12.sam file into a parquet file
    val bamReads: RDD[AlignmentRecord] = sc.loadAlignments(readsFilepath)
    assert(bamReads.count === 200)
  }

  sparkTest("sc.adamLoad should not load a file without a type specified") {
    //load an existing file from the resources and save it as an ADAM file.
    //This way we are not dependent on the ADAM format (as we would if we used a pre-made ADAM file)
    //but we are dependent on the unmapped.sam file existing, maybe I should make a new one
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("unmapped.sam").getFile
    val bamReads: RDD[AlignmentRecord] = sc.loadAlignments(readsFilepath)
    //save it as an Adam file so we can test the Adam loader
    val bamReadsAdamFile = new File(Files.createTempDir(), "bamReads.adam")
    bamReads.adamParquetSave(bamReadsAdamFile.getAbsolutePath)
    intercept[IllegalArgumentException] {
      val noReturnType = sc.adamLoad(bamReadsAdamFile.getAbsolutePath)
    }
    //finally just make sure we did not break anything,we came might as well
    val returnType: RDD[AlignmentRecord] = sc.adamLoad(bamReadsAdamFile.getAbsolutePath)
    assert(manifest[returnType.type] != manifest[RDD[Nothing]])
  }

  sparkTest("can read a small .SAM file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path)
    assert(reads.count() === 20)
  }

  sparkTest("can filter a .SAM file based on quality") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path, predicate = Some(classOf[HighQualityReadPredicate]))
    assert(reads.count() === 18)
  }

  test("Can convert to phred") {
    assert(successProbabilityToPhred(0.9) === 10)
    assert(successProbabilityToPhred(0.99999) === 50)
  }

  test("Can convert from phred") {
    // result is floating point, so apply tolerance
    assert(phredToSuccessProbability(10) > 0.89 && phredToSuccessProbability(10) < 0.91)
    assert(phredToSuccessProbability(50) > 0.99998 && phredToSuccessProbability(50) < 0.999999)
  }

  sparkTest("findFiles correctly finds a nested set of directories") {

    /**
     * Create the following directory structure, in the temp file location:
     *
     * .
     * |__ parent-dir/
     *     |__ subDir1/
     *     |   |__ match1/
     *     |   |__ match2/
     *     |__ subDir2/
     *     |   |__ match3/
     *     |   |__ nomatch4/
     *     |__ match5/
     *     |__ nomatch6/
     */

    val tempDir = File.createTempFile("ADAMContextSuite", "").getParentFile

    def createDir(dir: File, name: String): File = {
      val dirFile = new File(dir, name)
      dirFile.mkdir()
      dirFile
    }

    val parentName: String = "parent-" + UUID.randomUUID().toString
    val parentDir: File = createDir(tempDir, parentName)
    val subDir1: File = createDir(parentDir, "subDir1")
    val subDir2: File = createDir(parentDir, "subDir2")
    val match1: File = createDir(subDir1, "match1")
    val match2: File = createDir(subDir1, "match2")
    val match3: File = createDir(subDir2, "match3")
    val nomatch4: File = createDir(subDir2, "nomatch4")
    val match5: File = createDir(parentDir, "match5")
    val nomatch6: File = createDir(parentDir, "nomatch6")

    /**
     * Now, run findFiles() on the parentDir, and make sure we find match{1, 2, 3, 5} and _do not_
     * find nomatch{4, 6}
     */

    val paths = sc.findFiles(new Path(parentDir.getAbsolutePath), "^match.*")

    assert(paths.size === 4)

    val pathNames = paths.map(_.getName)
    assert(pathNames.contains("match1"))
    assert(pathNames.contains("match2"))
    assert(pathNames.contains("match3"))
    assert(pathNames.contains("match5"))
  }

  sparkTest("loadADAMFromPaths can load simple RDDs that have just been saved") {
    val contig = Contig.newBuilder
      .setContigName("abc")
      .setContigLength(1000000)
      .setReferenceURL("http://abc")
      .build

    val a0 = AlignmentRecord.newBuilder()
      .setRecordGroupName("group0")
      .setReadName("read0")
      .setContig(contig)
      .setStart(100)
      .setPrimaryAlignment(true)
      .setReadPaired(false)
      .setReadMapped(true)
      .build()
    val a1 = AlignmentRecord.newBuilder(a0)
      .setReadName("read1")
      .setStart(200)
      .build()

    val saved = sc.parallelize(Seq(a0, a1))
    val loc = tempLocation()
    val path = new Path(loc)

    saved.adamParquetSave(loc)
    try {
      val loaded = sc.loadAlignmentsFromPaths(Seq(path))

      assert(loaded.count() === saved.count())
    } catch {
      case (e: Exception) =>
        println(e)
        throw e
    }
  }

  /*
   Little helper function -- because apparently createTempFile creates an actual file, not
   just a name?  Whereas, this returns the name of something that could be mkdir'ed, in the
   same location as createTempFile() uses, so therefore the returned path from this method
   should be suitable for adamParquetSave().
   */
  def tempLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("ADAMContextSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  sparkTest("Can read a .gtf file") {
    val path = testFile("features/Homo_sapiens.GRCh37.75.trun20.gtf")
    val features: RDD[Feature] = sc.loadFeatures(path)
    assert(features.count === 15)
  }

  sparkTest("Can read a .bed file") {
    // note: this .bed doesn't actually conform to the UCSC BED spec...sigh...
    val path = testFile("features/gencode.v7.annotation.trunc10.bed")
    val features: RDD[Feature] = sc.loadFeatures(path)
    assert(features.count === 10)
  }

  sparkTest("Can read a .narrowPeak file") {
    val path = testFile("features/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val annot: RDD[Feature] = sc.loadFeatures(path)
    assert(annot.count === 10)
  }
}

