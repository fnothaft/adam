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

import htsjdk.variant.vcf.{ VCFHeader, VCFHeaderLine }
import java.util.concurrent.Executors
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.{ Partitioner, SparkFiles }
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ RecordGroupDictionary, ReferenceRegion, SequenceDictionary }
import org.bdgenomics.formats.avro.{ Contig, RecordGroupMetadata, Sample }
import org.bdgenomics.utils.cli.SaveArgs
import org.bdgenomics.utils.interval.array.IntervalArray
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private[rdd] class JavaSaveArgs(var outputPath: String,
                                var blockSize: Int = 128 * 1024 * 1024,
                                var pageSize: Int = 1 * 1024 * 1024,
                                var compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                                var disableDictionaryEncoding: Boolean = false,
                                var asSingleFile: Boolean = false) extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
  var deferMerging = false
}

private[rdd] object GenomicRDD {

  /**
   * Replaces file references in a command.
   *
   * @see pipe
   *
   * @param cmd Command to split and replace references in.
   * @param files List of paths to files.
   * @return Returns a split up command string, with file paths subbed in.
   */
  def processCommand(cmd: String,
                     files: Seq[String]): List[String] = {
    val filesWithIndex = files.zipWithIndex
      .map(p => {
        val (file, index) = p
        ("$%d".format(index), file)
      }).reverse

    @tailrec def replaceEscapes(cmd: String,
                                iter: Iterator[(String, String)]): String = {
      if (!iter.hasNext) {
        cmd
      } else {
        val (idx, file) = iter.next
        val newCmd = cmd.replace(idx, file)
        replaceEscapes(newCmd, iter)
      }
    }

    cmd.split(" ")
      .map(s => {
        replaceEscapes(s, filesWithIndex.toIterator)
      }).toList
  }
}

/**
 * A trait that wraps an RDD of genomic data with helpful metadata.
 *
 * @tparam T The type of the data in the wrapped RDD.
 * @tparam U The type of this GenomicRDD.
 */
trait GenomicRDD[T, U <: GenomicRDD[T, U]] {

  /**
   * The RDD of genomic data that we are wrapping.
   */
  val rdd: RDD[T]

  /**
   * The sequence dictionary describing the reference assembly this data is
   * aligned to.
   */
  val sequences: SequenceDictionary

  /**
   * The underlying RDD of genomic data, as a JavaRDD.
   */
  lazy val jrdd: JavaRDD[T] = {
    rdd.toJavaRDD()
  }

  /**
   * Applies a function that transforms the underlying RDD into a new RDD.
   *
   * @param tFn A function that transforms the underlying RDD.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transform(tFn: RDD[T] => RDD[T]): U = {
    replaceRdd(tFn(rdd))
  }

  private[rdd] val sortedTrait: SortedTrait
  private[rdd] def sorted = sortedTrait.sorted
  private[rdd] def partitionMap = sortedTrait.partitionMap

  /**
   * This case class is the source of all sorted knowledge handling in ADAM. We
   * have it as an inner class currently because there were some access issues
   */
  private[rdd] case class SortedTrait(sorted: Boolean,
                                      partitionMap: Option[Seq[(ReferenceRegion, ReferenceRegion)]]) extends Serializable
  /**
   * This repartition method repartitions all data in this.rdd and distributes it as evenly as possible
   * into the number of partitions provided. The data in this.rdd is already sorted (by ReferenceRegion.start)
   * so we ensure that it is kept that way as we move data around.
   *
   * Data is repartitioned in lists to maintain the sorted order. After the repartition, the lists are placed
   * in the correct order based on their original sorted order.
   *
   * @param partitions the number of partitions to repartition this.rdd into
   * @return a new SortedGenomicRDDMixIn with the RDD partitioned evenly
   */
  private def evenlyRepartition(partitions: Int)(implicit tTag: ClassTag[T]): GenomicRDD[T, U] = {
    assert(sorted)
    // the average number of records on each node will help us evenly repartition
    val average: Double = rdd.count.toDouble / partitions
    // we already have a sorted rdd, so let's just move the data
    // but we want to move it in blocks to maintain order
    // zipWithIndex seems to be the cheapest way to guarantee this
    val finalPartitionedRDD = rdd.zipWithIndex
      .mapPartitionsWithIndex((idx, iter) => {
        // here we get the destination partition number
        // this will calculate so that each partition has a nearly
        // equal amount of data on it
        getBalancedPartitionNumber(iter.map(_.swap), partitionMap.get(idx), average)
      }, preservesPartitioning = true)
      .partitionBy(new GenomicPositionRangePartitioner(partitions))
      .mapPartitions(iter => {
        // sorting an Iterator doesn't seem straight forward, so we cast to a list
        val listRepresentation = iter.map(_._2).toList.sortBy(_._2.head._1)
        // we take advantage of this casting to also get the bounds on the partition
        // and package that into the RDD so additional computation isn't needed
        Iterator(((listRepresentation.head._1._1, listRepresentation.last._1._2), listRepresentation.map(f => f._2)))
      }, preservesPartitioning = true)

    replaceRdd(finalPartitionedRDD.flatMap(f => f._2.flatten.map(f => f._2)), Some(finalPartitionedRDD.keys.collect))
  }

  /**
   * Infers the correct reference region for each T given a the bounds of the partition (partitionMap)
   * and returns the iterator with each T paired with the ReferenceRegion that it represents in that
   * sorted location. This is important because a T can map to multiple ReferenceRegions.
   *
   * @param iter The iterator (that partition's data) of T objects
   * @param partitionMap the bounds of the partition where _._1 is the lower bound and _._2 is the
   *                     upper bound
   * @return An iterator of all T's keyed with the specific ReferenceRegion that the T represents in
   *         the global sort
   */
  private def inferCorrectReferenceRegionsForPartition(
    iter: Iterator[T], partitionMap: (ReferenceRegion, ReferenceRegion)): Iterator[((ReferenceRegion, T))] = {

    // we put the records that map to multiple locations in this ListBuffer
    // IMPORTANT: these are records that map to multiple locations on this
    // partition. There should be very few of these
    // we use this because we have to keep track of which ReferenceRegions
    // have been used.
    val duplicates = new ListBuffer[(Seq[ReferenceRegion], T)]()
    // filter out ReferenceRegions that are not on this partition
    iter.map(f => (getReferenceRegions(f).filter(g =>
      g.compareTo(partitionMap._1) >= 0 && g.compareTo(partitionMap._2) <= 0).sorted, f))
      .map(f => {
        // the usual case: only one ReferenceRegion for the record
        if (f._1.length == 1) {
          (f._1.head, f._2)
          // if there are multiple, we have to do a little extra work to make
          // sure that we have the correct ReferenceRegion
        } else {
          val index = duplicates.indexOf(f)
          // if we've already added this record to the duplicates list
          if (index >= 0) {
            // get the inferred value from the sorted list of ReferenceRegions
            val inferredValue = (duplicates(index)._1.head, duplicates(index)._2)
            // if there are more after this
            if (duplicates(index)._1.length != 1) duplicates(index) = (duplicates(index)._1.drop(1), duplicates(index)._2)
            inferredValue
          } else {
            val inferredValue = (f._1.head, f._2)
            // add to the list of duplicates
            duplicates += ((f._1.drop(1), f._2))
            inferredValue
          }
        }
      })
  }

  /**
   * This is a function that splits each partition (iter) into lists keyed with the destination
   * partition number. It preserves the order of the data received and thus is good for moving
   * around sorted data. Each partition is given the same amount of data.
   *
   * @param iter The partition data. (k,v) where k is the index in the RDD and v is T
   * @param average the average number of tuples on all partitions
   * @return grouped lists keyed with the destination partition number. These lists maintain
   *         their original order
   */
  private def getBalancedPartitionNumber(iter: Iterator[(Long, T)], partitionMap: (ReferenceRegion, ReferenceRegion),
                                         average: Double): Iterator[(Int, ((ReferenceRegion, ReferenceRegion), List[(Long, T)]))] = {

    /**
     * Infers the correct reference region for each T given a the bounds of the partition (partitionMap)
     * and returns the iterator with each T paired with the ReferenceRegion that it represents in that
     * sorted location. This is important because a T can map to multiple ReferenceRegions.
     *
     * This version of the method works with getBalancedPartitionNumber.
     *
     * @param iter The iterator (that partition's data) with each T zipped
     * @param partitionMap the bounds of the partition where _._1 is the lower bound and _._2 is the
     *                     upper bound
     * @return An iterator of all T's keyed with the specific ReferenceRegion that the T represents in
     *         the global sort, then keyed with the globally zipped position
     */
    def inferCorrectReferenceRegionsForPartitionWithZip(
      iter: Iterator[(Long, T)],
      partitionMap: (ReferenceRegion, ReferenceRegion)): Iterator[(Long, (ReferenceRegion, T))] = {

      // converting to a list and making sure that we only look at values that are in our range
      val listRepresentation = iter.map(f => (f._1, (getReferenceRegions(f._2).filter(g => g == partitionMap._1 ||
        (g.start >= partitionMap._1.start && g.start <= partitionMap._2.end) ||
        (g.end >= partitionMap._1.start && g.end <= partitionMap._2.end)
      ).sorted, f._2))).toList
      // for now we are using Listbuffers, in the future something else may be better
      val listOfInferredData = new ListBuffer[(Long, (ReferenceRegion, T))]

      for (i <- listRepresentation.indices) {
        // the first record on the partition is always the first to be added,
        // and contains the lower bound (or next value after lower bound)
        if (i == 0) {
          listOfInferredData +=
            ((listRepresentation(i)._1, (listRepresentation(i)._2._1.head, listRepresentation(i)._2._2)))
        } else {
          val previousReferenceRegion = listOfInferredData.last._2._1
          var j = 0
          // we should never run off the end of the list because there will
          // always be at least the same number of regions as there are T's
          // in this partition. we are finding the next lowest ReferenceRegion
          // after the previous ReferenceRegion.
          while (listRepresentation(i)._2._1(j).compareTo(previousReferenceRegion) < 0) {
            j += 1
          }
          listOfInferredData +=
            ((listRepresentation(i)._1, (listRepresentation(i)._2._1(j), listRepresentation(i)._2._2)))
        }
      }
      listOfInferredData.toIterator
    }

    // converting to list so we can package data that is going to the same node with a groupBy
    val correctPartition = inferCorrectReferenceRegionsForPartitionWithZip(iter, partitionMap)
    correctPartition.map(f => ((f._1 / average).toInt, f)).toList.groupBy(_._1).mapValues(f => f.map(_._2))
      .map(f => (f._1, ((f._2.head._2._1, f._2.last._2._1), f._2.map(g => (g._1, g._2._2))))).toIterator
  }

  /**
   * Sorts our genome aligned data by reference positions, with contigs ordered
   * by index.
   *
   * @return Returns a new RDD containing sorted data.
   *
   * @note Does not support data that is unaligned or where objects align to
   *   multiple positions.
   * @see sortLexicographically
   */
  def sort(): U = {
    replaceRdd(rdd.sortBy(elem => {
      val coveredRegions = getReferenceRegions(elem)

      require(coveredRegions.nonEmpty, "Cannot sort RDD containing an unmapped element %s.".format(
        elem))
      require(coveredRegions.size == 1,
        "Cannot sort RDD containing a multimapped element. %s covers %s.".format(
          elem, coveredRegions.mkString(",")))

      val contigName = coveredRegions.head.referenceName
      val sr = sequences(contigName)
      require(sr.isDefined, "Element %s has contig name %s not in dictionary %s.".format(
        elem, contigName, sequences))
      require(sr.get.referenceIndex.isDefined,
        "Contig %s from sequence dictionary lacks an index.".format(sr))

      (sr.get.referenceIndex.get, coveredRegions.head.start)
    }))
  }

  /**
   * Sorts our genome aligned data by reference positions, with contigs ordered
   * lexicographically.
   *
   * @return Returns a new RDD containing sorted data.
   *
   * @note Does not support data that is unaligned or where objects align to
   *   multiple positions.
   * @see sort
   */
  def sortLexicographically(): U = {
    replaceRdd(rdd.sortBy(elem => {
      val coveredRegions = getReferenceRegions(elem)

      require(coveredRegions.nonEmpty, "Cannot sort RDD containing an unmapped element %s.".format(
        elem))
      require(coveredRegions.size == 1,
        "Cannot sort RDD containing a multimapped element. %s covers %s.".format(
          elem, coveredRegions.mkString(",")))

      coveredRegions.head
    }))
  }

  /**
   * Pipes genomic data to a subprocess that runs in parallel using Spark.
   *
   * Files are substituted in to the command with a $x syntax. E.g., to invoke
   * a command that uses the first file from the files Seq, use $0.
   *
   * Pipes require the presence of an InFormatterCompanion and an OutFormatter
   * as implicit values. The InFormatterCompanion should be a singleton whose
   * apply method builds an InFormatter given a specific type of GenomicRDD.
   * The implicit InFormatterCompanion yields an InFormatter which is used to
   * format the input to the pipe, and the implicit OutFormatter is used to
   * parse the output from the pipe.
   *
   * @param cmd Command to run.
   * @param files Files to make locally available to the commands being run.
   *   Default is empty.
   * @param environment A map containing environment variable/value pairs to set
   *   in the environment for the newly created process. Default is empty.
   * @param flankSize Number of bases to flank each command invocation by.
   * @return Returns a new GenomicRDD of type Y.
   *
   * @tparam X The type of the record created by the piped command.
   * @tparam Y A GenomicRDD containing X's.
   * @tparam V The InFormatter to use for formatting the data being piped to the
   *   command.
   */
  def pipe[X, Y <: GenomicRDD[X, Y], V <: InFormatter[T, U, V]](cmd: String,
                                                                files: Seq[String] = Seq.empty,
                                                                environment: Map[String, String] = Map.empty,
                                                                flankSize: Int = 0)(implicit tFormatterCompanion: InFormatterCompanion[T, U, V],
                                                                                    xFormatter: OutFormatter[X],
                                                                                    convFn: (U, RDD[X]) => Y,
                                                                                    tManifest: ClassTag[T],
                                                                                    xManifest: ClassTag[X]): Y = {

    // TODO: support broadcasting files
    files.foreach(f => {
      rdd.context.addFile(f)
    })

    // make formatter
    val tFormatter: V = tFormatterCompanion.apply(this.asInstanceOf[U])

    // make bins
    val seqLengths = sequences.records.toSeq.map(rec => (rec.name, rec.length)).toMap
    val totalLength = seqLengths.values.sum
    val bins = GenomeBins(totalLength / rdd.partitions.size, seqLengths)

    // if the input rdd is mapped, then we need to repartition
    val partitionedRdd = if (sequences.records.size > 0) {
      // get region covered, expand region by flank size, and tag with bins
      val binKeyedRdd = rdd.flatMap(r => {

        // get regions and expand
        val regions = getReferenceRegions(r).map(_.pad(flankSize))

        // get all the bins this record falls into
        val recordBins = regions.flatMap(rr => {
          (bins.getStartBin(rr) to bins.getEndBin(rr)).map(b => (rr, b))
        })

        // key the record by those bins and return
        // TODO: this should key with the reference region corresponding to a bin
        recordBins.map(b => (b, r))
      })

      // repartition yonder our data
      // TODO: this should repartition and sort within the partition
      binKeyedRdd.repartitionAndSortWithinPartitions(ManualRegionPartitioner(bins.numBins))
        .values
    } else {
      rdd
    }

    // are we in local mode?
    val isLocal = partitionedRdd.context.isLocal

    // call map partitions and pipe
    val pipedRdd = partitionedRdd.mapPartitions(iter => {

      // get files
      // from SPARK-3311, SparkFiles doesn't work in local mode.
      // so... we'll bypass that by checking if we're running in local mode.
      // sigh!
      val locs = if (isLocal) {
        files
      } else {
        files.map(f => {
          SparkFiles.get(f)
        })
      }

      // split command and create process builder
      val finalCmd = GenomicRDD.processCommand(cmd, locs)
      val pb = new ProcessBuilder(finalCmd)
      pb.redirectError(ProcessBuilder.Redirect.INHERIT)

      // add environment variables to the process builder
      val pEnv = pb.environment()
      environment.foreach(kv => {
        val (k, v) = kv
        pEnv.put(k, v)
      })

      // start underlying piped command
      val process = pb.start()
      val os = process.getOutputStream()
      val is = process.getInputStream()

      // wrap in and out formatters
      val ifr = new InFormatterRunner[T, U, V](iter, tFormatter, os)
      val ofr = new OutFormatterRunner[X, OutFormatter[X]](xFormatter, is)

      // launch thread pool and submit formatters
      val pool = Executors.newFixedThreadPool(2)
      pool.submit(ifr)
      val futureIter = pool.submit(ofr)

      // wait for process to finish
      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw new RuntimeException("Piped command %s exited with error code %d.".format(
          finalCmd, exitCode))
      }

      // shut thread pool
      pool.shutdown()

      futureIter.get
    })

    // build the new GenomicRDD
    val newRdd = convFn(this.asInstanceOf[U], pipedRdd)

    // if the original rdd was aligned and the final rdd is aligned, then we must filter
    if (newRdd.sequences.isEmpty ||
      sequences.isEmpty) {
      newRdd
    } else {
      def filterPartition(idx: Int, iter: Iterator[X]): Iterator[X] = {

        // get the region for this partition
        val region = bins.invert(idx)

        // map over the iterator and filter out any items that don't belong
        iter.filter(x => {

          // get the regions for x
          val regions = newRdd.getReferenceRegions(x)

          // are there any regions that overlap our current region
          !regions.forall(!_.overlaps(region))
        })
      }

      // run a map partitions with index and discard all items that fall outside of their
      // own partition's region bound
      newRdd.transform(_.mapPartitionsWithIndex(filterPartition))
    }
  }

  protected def replaceRdd(newRdd: RDD[T], newPartitionMapRdd: Option[Seq[(ReferenceRegion, ReferenceRegion)]] = None): U

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion]

  protected def flattenRddByRegions(): RDD[(ReferenceRegion, T)] = {
    rdd.flatMap(elem => {
      getReferenceRegions(elem).map(r => (r, elem))
    })
  }

  /**
   * Runs a filter that selects data in the underlying RDD that overlaps a
   * single genomic region.
   *
   * @param query The region to query for.
   * @return Returns a new GenomicRDD containing only data that overlaps the
   *   query region.
   */
  def filterByOverlappingRegion(query: ReferenceRegion): U = {
    replaceRdd(rdd.filter(elem => {

      // where can this item sit?
      val regions = getReferenceRegions(elem)

      // do any of these overlap with our query region?
      regions.exists(_.overlaps(query))
    }), partitionMap)
  }

  /**
   * Runs a filter that selects data in the underlying RDD that overlaps several genomic regions.
   *
   * @param querys The regions to query for.
   * @return Returns a new GenomicRDD containing only data that overlaps the
   *   querys region.
   */
  def filterByOverlappingRegions(querys: List[ReferenceRegion]): U = {
    replaceRdd(rdd.filter(elem => {

      val regions = getReferenceRegions(elem)

      querys.map(query => {
        regions.exists(_.overlaps(query))
      }).fold(false)((a, b) => a || b)
    }), partitionMap)
  }

  protected def buildTree(
    rdd: RDD[(ReferenceRegion, T)])(
      implicit tTag: ClassTag[T]): IntervalArray[ReferenceRegion, T]

  /**
   * Performs a broadcast inner join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is an inner join, all values who do not overlap a value from the other
   * RDD are dropped.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def broadcastRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, X), Z] = {

    // key the RDDs and join
    GenericGenomicRDD[(T, X)](InnerTreeRegionJoin[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions()),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => { getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2) })
      .asInstanceOf[GenomicRDD[(T, X), Z]]
  }

  /**
   * Performs a broadcast right outer join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is a right outer join, all values in the left RDD that do not overlap a
   * value from the right RDD are dropped. If a value from the right RDD does
   * not overlap any values in the left RDD, it will be paired with a `None`
   * in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterBroadcastRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], X), Z]](
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], X), Z] = {

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], X)](RightOuterTreeRegionJoin[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions()),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
      .asInstanceOf[GenomicRDD[(Option[T], X), Z]]
  }

  /**
   * Computes the partition size and final sequence dictionary for a join.
   *
   * @param optPartitions Optional user-requested number of partitions for the
   *   end of the shuffle.
   * @param genomicRdd The genomic RDD we are joining against.
   * @return Returns a tuple containing the (partition size, final sequence
   *   dictionary after the join).
   */
  private[rdd] def joinPartitionSizeAndSequences[X, Y <: GenomicRDD[X, Y]](
    optPartitions: Option[Int],
    genomicRdd: GenomicRDD[X, Y]): (Long, SequenceDictionary) = {

    require(!(sequences.isEmpty && genomicRdd.sequences.isEmpty),
      "Both RDDs at input to join have an empty sequence dictionary!")

    // what sequences do we wind up with at the end?
    val finalSequences = sequences ++ genomicRdd.sequences

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val estPartitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // if the user provides too high of a partition count, the estimated number
    // of partitions can go to 0
    val partitions = if (estPartitions >= 1) {
      estPartitions
    } else {
      1
    }

    (finalSequences.records.map(_.length).sum / partitions,
      finalSequences)
  }

  /**
   * Performs a broadcast inner join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is an inner join, all values who do not overlap a value from the other
   * RDD are dropped.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def broadcastRegionJoinAndGroupByRight[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Iterable[T], X), Z]](genomicRdd: GenomicRDD[X, Y])(
    implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Iterable[T], X), Z] = {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[T], X)](InnerTreeRegionJoinAndGroupByRight[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions()),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => { (kv._1.flatMap(getReferenceRegions) ++ genomicRdd.getReferenceRegions(kv._2)).toSeq })
      .asInstanceOf[GenomicRDD[(Iterable[T], X), Z]]
  }

  /**
   * Performs a broadcast right outer join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is a right outer join, all values in the left RDD that do not overlap a
   * value from the right RDD are dropped. If a value from the right RDD does
   * not overlap any values in the left RDD, it will be paired with a `None`
   * in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterBroadcastRegionJoinAndGroupByRight[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Iterable[T], X), Z]](genomicRdd: GenomicRDD[X, Y])(
    implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Iterable[T], X), Z] = {

    // key the RDDs and join
    GenericGenomicRDD[(Iterable[T], X)](RightOuterTreeRegionJoinAndGroupByRight[T, X]().broadcastAndJoin(
      buildTree(flattenRddByRegions()),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
      .asInstanceOf[GenomicRDD[(Iterable[T], X), Z]]
  }

  /**
   * All shuffleRegionJoins share the same code to prepare, so this is a helper
   * function that prepares all the data for joining. It starts by repartitioning
   * (if needed) the leftRdd. Next, it copartitions the right Rdd with it. Before
   * returning the RDDs, it puts them in the format that the joins need. The
   * ReferenceRegion is inferred for each individual record in the RDD.
   *
   * @param leftRdd Usually the object that invoked the function, unless a
   *                repartition had to occur. The repartition only happens in cases where
   *                the number of partitions requested for the join differs from the
   *                number of partitions.
   * @param genomicRdd The RDD to join to.
   * @return A tuple containing both RDDs ready to join. _._1 is the leftRdd and _._2 is the rightRdd
   */
  private def prepareForShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](
    leftRdd: GenomicRDD[T, U],
    genomicRdd: GenomicRDD[X, Y])(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): ((RDD[((ReferenceRegion, Int), T)], RDD[((ReferenceRegion, Int), X)])) = {

    assert(leftRdd.sorted)
    val rightRdd: GenomicRDD[X, Y] = genomicRdd.copartitionByReferenceRegion(leftRdd)
    assert(rightRdd.sorted)

    // Partition Map for the left side
    val leftPmap = leftRdd.partitionMap.get
    val leftRddReadyToJoin = leftRdd.rdd.mapPartitionsWithIndex((idx, iter) => {
      leftRdd.inferCorrectReferenceRegionsForPartition(iter, leftPmap(idx)).map(f => ((f._1, idx), f._2))
    })

    // Partition map for the right side
    val rightPmap = rightRdd.partitionMap.get
    val rightRddReadyToJoin = rightRdd.rdd.mapPartitionsWithIndex((idx, iter) => {
      rightRdd.inferCorrectReferenceRegionsForPartition(iter, rightPmap(idx)).map(f => ((f._1, idx), f._2))
    })
    // Co-partitioned and properly keyed with ReferenceRegions and partition index
    (leftRddReadyToJoin, rightRddReadyToJoin)
  }

  /**
   * Performs a sort-merge inner join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is an inner join, all values who do not
   * overlap a value from the other RDD are dropped.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None)(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, X), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd: GenomicRDD[T, U] = if (sorted) {
      if (partitions != rdd.partitions.length) evenlyRepartition(partitions) else this
    } else {
      repartitionAndSort(partitions)
    }

    val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
    val leftRddReadyToJoin = preparedRdds._1
    val rightRddReadyToJoin = preparedRdds._2
    // did we co-partition the data into the same number of partitions?
    assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(T, X)](
      InnerShuffleRegionJoin[T, X](endSequences,
        endSequences.records.map(_.length).sum / partitions, rdd.context)
      .joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
      endSequences,
      kv => {
        getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2)
      }, leftRdd.partitionMap).asInstanceOf[GenomicRDD[(T, X), Z]]
  }

  /**
   * Performs a sort-merge right outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a right outer join, all values in the
   * left RDD that do not overlap a value from the right RDD are dropped.
   * If a value from the right RDD does not overlap any values in the left
   * RDD, it will be paired with a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], X), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None)(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], X), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val rightRdd = if (genomicRdd.sorted) {
      if (partitions != rdd.partitions.length) genomicRdd.evenlyRepartition(partitions) else genomicRdd
    } else genomicRdd.repartitionAndSort(partitions)

    val preparedRdds = rightRdd.prepareForShuffleRegionJoin(rightRdd, this)
    val leftRddReadyToJoin = preparedRdds._2
    val rightRddReadyToJoin = preparedRdds._1
    assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(Option[T], X)](
      RightOuterShuffleRegionJoin[T, X](endSequences,
        endSequences.records.map(_.length).sum / partitions, rdd.context)
      .joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
      endSequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      }, rightRdd.partitionMap).asInstanceOf[GenomicRDD[(Option[T], X), Z]]
  }

  /**
   * Performs a sort-merge left outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a left outer join, all values in the
   * right RDD that do not overlap a value from the left RDD are dropped.
   * If a value from the left RDD does not overlap any values in the right
   * RDD, it will be paired with a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  def leftOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Option[X]), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None)(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, Option[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd: GenomicRDD[T, U] = if (sorted) {
      if (partitions != rdd.partitions.length) evenlyRepartition(partitions) else this
    } else {
      repartitionAndSort(partitions)
    }

    val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
    val leftRddReadyToJoin = preparedRdds._1
    val rightRddReadyToJoin = preparedRdds._2
    assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(T, Option[X])](
      LeftOuterShuffleRegionJoin[T, X](endSequences,
        endSequences.records.map(_.length).sum / partitions, rdd.context)
      .joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
      endSequences,
      kv => {
        Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v))).flatten.flatten ++
          getReferenceRegions(kv._1)
      }, leftRdd.partitionMap).asInstanceOf[GenomicRDD[(T, Option[X]), Z]]
  }

  /**
   * Performs a sort-merge full outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a full outer join, if a value from either
   * RDD does not overlap any values in the other RDD, it will be paired with
   * a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and values that did not
   *   overlap will be paired with a `None`.
   */
  def fullOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Option[X]), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None)(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], Option[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(rdd.partitions.length)

    val leftRdd = if (sorted) {
      if (partitions != rdd.partitions.length) evenlyRepartition(partitions) else this
    } else {
      repartitionAndSort(partitions)
    }

    val rightRdd = if (genomicRdd.sorted) {
      if (partitions != rdd.partitions.length) genomicRdd.evenlyRepartition(partitions) else genomicRdd
    } else {
      genomicRdd.repartitionAndSort(partitions)
    }

    val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
    val leftRddReadyToJoin = prepareForShuffleRegionJoin(leftRdd, leftRdd)._2
    val rightRddReadyToJoin = preparedRdds._2
    assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(Option[T], Option[X])](
      FullOuterShuffleRegionJoin[T, X](endSequences,
        endSequences.records.map(_.length).sum / partitions, rdd.context)
      .joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
      endSequences,
      kv => {
        Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v)),
          kv._1.map(v => getReferenceRegions(v))).flatten.flatten
      }, leftRdd.partitionMap).asInstanceOf[GenomicRDD[(Option[T], Option[X]), Z]]
  }

  /**
   * Performs a sort-merge inner join between this RDD and another RDD,
   * followed by a groupBy on the left value.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is an inner join, all values who do not
   * overlap a value from the other RDD are dropped. In the same operation,
   * we group all values by the left item in the RDD.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD..
   */
  def shuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Iterable[X]), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None)(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, Iterable[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd: GenomicRDD[T, U] = if (sorted) {
      if (partitions != rdd.partitions.length) evenlyRepartition(partitions) else this
    } else {
      repartitionAndSort(partitions)
    }

    val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
    val leftRddReadyToJoin = preparedRdds._1
    val rightRddReadyToJoin = preparedRdds._2
    assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

    GenericGenomicRDD[(T, Iterable[X])](
      InnerShuffleRegionJoinAndGroupByLeft[T, X](endSequences,
        endSequences.records.map(_.length).sum / partitions, rdd.context)
      .joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
      endSequences,
      kv => {
        (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
          getReferenceRegions(kv._1)).toSeq
      }, leftRdd.partitionMap).asInstanceOf[GenomicRDD[(T, Iterable[X]), Z]]
  }

  /**
   * Performs a sort-merge right outer join between this RDD and another RDD,
   * followed by a groupBy on the left value, if not null.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. In the same operation, we group all values by the left
   * item in the RDD. Since this is a right outer join, all values from the
   * right RDD who did not overlap a value from the left RDD are placed into
   * a length-1 Iterable with a `None` key.
   *
   * @param genomicRdd The right RDD in the join.
   * @param optPartitions Optional parameter for providing number of partitions
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD, and all values from the
   *   right RDD that did not overlap an item in the left RDD.
   */
  def rightOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Iterable[X]), Z]](
    genomicRdd: GenomicRDD[X, Y],
    optPartitions: Option[Int] = None)(
      implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], Iterable[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd = if (sorted) {
      if (partitions != rdd.partitions.length) evenlyRepartition(partitions) else this
    } else {
      repartitionAndSort(partitions)
    }

    val rightRdd = if (genomicRdd.sorted) {
      if (partitions != rdd.partitions.length) genomicRdd.evenlyRepartition(partitions) else genomicRdd
    } else {
      genomicRdd.repartitionAndSort(partitions)
    }

    val preparedRdds = prepareForShuffleRegionJoin(leftRdd, genomicRdd)
    val leftRddReadyToJoin = preparedRdds._1
    val rightRddReadyToJoin = preparedRdds._2//prepareForShuffleRegionJoin(leftRdd, rightRdd)._2
    assert(leftRddReadyToJoin.partitions.length == rightRddReadyToJoin.partitions.length)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    GenericGenomicRDD[(Option[T], Iterable[X])](
      RightOuterShuffleRegionJoinAndGroupByLeft[T, X](endSequences,
        endSequences.records.map(_.length).sum / partitions, rdd.context)
      .joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadyToJoin),
      endSequences,
      kv => {
        (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
          kv._1.toSeq.flatMap(v => getReferenceRegions(v))).toSeq
      }, rightRdd.partitionMap).asInstanceOf[GenomicRDD[(Option[T], Iterable[X]), Z]]
  }

  /**
   * The purpose of this method is to copartition two RDDs according to their ReferenceRegions.
   * It is useful during ShuffleRegionJoins because it guarantees no extra shuffling to get correct
   * and complete region joins. RDD is already sorted coming in, so we take precaution to keep it
   * that way.
   *
   * The simplest way of sending data to the correct node is to just send everything that has a
   * start position that could fall within the boundaries
   *
   * This is best used under the condition that (repeatedly) repartitioning is more expensive than
   * calculating the proper location of the records of this.rdd. It requires a pass through the
   * co-located RDD to get the correct partition(s) for each record. It will assign a record to
   * multiple partitions if necessary.
   *
   * @param rddToCoPartitionWith the rdd that this.rdd will be copartitioned with
   * @return returns the newly repartitioned rdd that is copartitioned with rddToCoPartitionWith
   */
  private[rdd] def copartitionByReferenceRegion[X, Y <: GenomicRDD[X, Y]](
    rddToCoPartitionWith: GenomicRDD[X, Y])(implicit tTag: ClassTag[T], xTag: ClassTag[X]): U = {

    // if the other RDD is not sorted, we can't guarantee proper copartition
    assert(rddToCoPartitionWith.sorted)
    val destinationPartitionMap = rddToCoPartitionWith.partitionMap.get
    //number of partitions we will have after repartition
    val numPartitions = destinationPartitionMap.length

    val finalPartitionedRDD =
      if (sorted) {
        // we're working with sorted data here, so it's important to keep things in
        // the order. We are going to be moving the data around in a Seq to control
        // the order.
        rdd.mapPartitionsWithIndex((idx, iter) => {
          // we need to derive the ReferenceRegion for each T before we do anything
          // else.
          inferCorrectReferenceRegionsForPartition(iter, partitionMap.get(idx)).map(f => {
            // inside the map over the partition we do a map over the partition map
            // this implementation is in lieu of for...yield
            destinationPartitionMap.zipWithIndex.map(partition => {
              // if there is overlap between the partition map and the current value
              if (f._1.covers(partition._1._1) || f._1.covers(partition._1._2) ||
                (f._1.compareTo(partition._1._1) >= 0 && f._1.compareTo(partition._1._2) <= 0)) {
                // we use Options here so scala doesn't lose typing information
                // it is important to note that the Seq will contain the same data
                // in the first value of the tuple for every record. This is the
                // destination partition index.
                Some((partition._2, f))
              } else None
              // next we get rid of the None values from the Seq we just made
            }).filter(_.isDefined)
          })
        }, preservesPartitioning = true)
          // this part is less intuitive. it's important that things stay ordered
          // for the sake of preventing another (full) sort. when we arrive here
          // in the code, we have an RDD[Option[Seq[(Int, (ReferenceRegion, T))]]],
          // which is quite nasty. what we need is for every Seq to be keyed by the
          // first ReferenceRegion and destination partition index so the
          // partitioner can assign it correctly.
          .mapPartitions(iter => {
            // since each Seq contains the same partition index for each tuple, we
            // just pull the first one out of the Seq and drop the rest
            iter.map(f => ((f.head.get._2._1, f.head.get._1), f.map(g => g.get._2)))
          })
          .repartitionAndSortWithinPartitions(
            new GenomicPositionRangePartitioner(numPartitions, destinationPartitionMap))
          // return to an RDD[(ReferenceRegion, T)]
          .flatMap(f => f._2)
      } else {
        // we aren't working with sorted data here, so maintaining order
        // isn't important. we also avoid presorting the data because
        // we end up shuffling the data around less. We simply perform a
        // partition then sort by ReferenceRegion
        flattenRddByRegions()
          .mapPartitionsWithIndex((idx, iter) => {
            // this is how we determine the destination partition number
            iter.map(f => {
              // inside the map over the partition we do a map over the partition map
              // this implementation is in lieu of for...yield
              destinationPartitionMap.zipWithIndex.map(partition => {
                // if there is overlap between the partition map and the current value
                if (f._1.covers(partition._1._1) || f._1.covers(partition._1._2) ||
                  (f._1.compareTo(partition._1._1) >= 0 && f._1.compareTo(partition._1._2) <= 0) ||
                  (f._1.compareTo(partition._1._2) >= 0)) {
                  // we use Options here so scala doesn't lose typing information
                  Some((partition._2, f))
                } else None
              })
            })
          }, preservesPartitioning = true)
          // finally we filter out the None values and flatten down
          // the result is an RDD[(Int, (ReferenceRegion, T))].
          // the Int is the destination partition index
          .flatMap(f => f.filter(_.isDefined).map(g => g.get))
          .map(f => ((f._2._1, f._1), f._2))
          .repartitionAndSortWithinPartitions(
            new GenomicPositionRangePartitioner(numPartitions, destinationPartitionMap))
          .map(_._2)
      }
    // here we get the new partition map
    // for now, we can't use the left RDD's partition map because
    // the bounds don't always include the values we copartitioned with.
    // in other words, joins allow data that starts before a given
    // ReferenceRegion to combine given they overlap.
    // this means that we cannot use the bounds of the left RDD because
    // they may not necessarily be the bounds of the right RDD
    val newPartitionMap = finalPartitionedRDD.mapPartitions(iter => {
      getPartitionMapBoundsFromRdd(iter)
    }, preservesPartitioning = true).collect

    replaceRdd(finalPartitionedRDD.values, Some(newPartitionMap))
  }

  /**
   * Gets the partition bounds from a ReferenceRegion keyed Iterator
   *
   * @param iter The data on a given partition. ReferenceRegion keyed
   * @return The bounds of the ReferenceRegions on that partition, in an Iterator
   */
  private def getPartitionMapBoundsFromRdd(iter: Iterator[(ReferenceRegion, T)]): Iterator[(ReferenceRegion, ReferenceRegion)] = {
    val listRep = iter.toList
    Iterator((listRep.head._1, listRep.last._1))
    //    if (iter.isEmpty) Iterator()
    //    else {
    //      val head = iter.next._1
    //      if (iter.hasNext) {
    //        var tail = iter.next._1
    //        while (iter.hasNext) tail = iter.next._1
    //        Iterator((head, tail))
    //        //only one ReferenceRegion on this partition
    //      } else Iterator((head, head))
    //    }
  }

  /**
   * This method is the first method called when there is some need for the
   * data to be sorted. We are assuming no previous knowledge about data
   * distribution or skew, and as such we simply partition based on range.
   * This can present some issues when the data is extremely skewed, but
   * we attempt to do our best in solving this problem.
   *
   * The output from this method is a SortedGenomicRDD, which simply
   * contains the knowledge that this was sorted and overrides many methods
   * that would be improved by sorting the data first.
   *
   * @param partitions The number of partitions to split the data into
   * @return A SortedGenomicRDDMixIn that contains the sorted and partitioned
   *         RDD
   */
  def repartitionAndSort(partitions: Int = rdd.partitions.length)(implicit c: ClassTag[T]): U = {
    // here we let spark do the heavy lifting
    val partitionedRDD = flattenRddByRegions()
      .sortBy(f => f._1, ascending = true, partitions)
    replaceRdd(partitionedRDD.values,
      Some(partitionedRDD.mapPartitions(iter => getPartitionMapBoundsFromRdd(iter), preservesPartitioning = true).collect))
  }

  private[rdd] class GenomicPositionRangePartitioner[V](
      partitions: Int,
      partitionMap: Seq[(ReferenceRegion, ReferenceRegion)] = Seq.empty[(ReferenceRegion, ReferenceRegion)]) extends Partitioner {

    override def numPartitions: Int = partitions

    def getPartition(key: Any): Int = {
      key match {
        case f: Int                         => f
        case (f1: ReferenceRegion, f2: Int) => f2
        case _                              => throw new Exception("Reference Region Key require to partition on Genomic Position")
      }
    }
  }
}

private case class GenericGenomicRDD[T](rdd: RDD[T],
                                        sequences: SequenceDictionary,
                                        regionFn: T => Seq[ReferenceRegion],
                                        newPartitionMap: Option[Seq[(ReferenceRegion, ReferenceRegion)]] = None) extends GenomicRDD[T, GenericGenomicRDD[T]] {

  override val sortedTrait = new SortedTrait(sorted = newPartitionMap.isDefined, newPartitionMap)

  protected def buildTree(
    rdd: RDD[(ReferenceRegion, T)])(
      implicit tTag: ClassTag[T]): IntervalArray[ReferenceRegion, T] = {
    IntervalArray(rdd)
  }

  protected def getReferenceRegions(elem: T): Seq[ReferenceRegion] = {
    regionFn(elem)
  }

  protected def replaceRdd(newRdd: RDD[T], newPartitionMap: Option[Seq[(ReferenceRegion, ReferenceRegion)]] = None): GenericGenomicRDD[T] = {
    copy(rdd = newRdd, newPartitionMap = newPartitionMap)
  }
}

/**
 * A trait describing a GenomicRDD with data from multiple samples.
 */
trait MultisampleGenomicRDD[T, U <: MultisampleGenomicRDD[T, U]] extends GenomicRDD[T, U] {

  /**
   * The samples who have data contained in this GenomicRDD.
   */
  val samples: Seq[Sample]
}

/**
 * An abstract class describing a GenomicRDD where:
 *
 * * The data are Avro IndexedRecords.
 * * The data are associated to read groups (i.e., they are reads or fragments).
 */
abstract class AvroReadGroupGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroReadGroupGenomicRDD[T, U]] extends AvroGenomicRDD[T, U] {

  /**
   * A dictionary describing the read groups attached to this GenomicRDD.
   */
  val recordGroups: RecordGroupDictionary

  override protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    val contigSchema = Contig.SCHEMA$
    if (sorted) {
      contigSchema.addProp("sorted", "true".asInstanceOf[Any])
      contigSchema.addProp("partitionMap", partitionMap.get.mkString(",").asInstanceOf[Any])
    }

    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      contigSchema,
      contigs)

    // convert record group to avro and save
    val rgMetadata = recordGroups.recordGroups
      .map(_.toMetadata)

    val recordGroupMetaData = RecordGroupMetadata.SCHEMA$
    if (sorted) {
      recordGroupMetaData.addProp("sorted", "true".asInstanceOf[Any])
      recordGroupMetaData.addProp("partitionMap", partitionMap.get.mkString(",").asInstanceOf[Any])
    }

    saveAvro("%s/_rgdict.avro".format(filePath),
      rdd.context,
      recordGroupMetaData,
      rgMetadata)
  }
}

/**
 * An abstract class that extends the MultisampleGenomicRDD trait, where the data
 * are Avro IndexedRecords.
 */
abstract class MultisampleAvroGenomicRDD[T <% IndexedRecord: Manifest, U <: MultisampleAvroGenomicRDD[T, U]] extends AvroGenomicRDD[T, U]
    with MultisampleGenomicRDD[T, U] {

  /**
   * The header lines attached to the file.
   */
  val headerLines: Seq[VCFHeaderLine]

  override protected def saveMetadata(filePath: String) {

    val sampleSchema = Sample.SCHEMA$
    if (sorted) {
      sampleSchema.addProp("sorted", "true".asInstanceOf[Any])
      sampleSchema.addProp("partitionMap", partitionMap.get.mkString(",").asInstanceOf[Any])
    }

    // write vcf headers to file
    VCFHeaderUtils.write(new VCFHeader(headerLines.toSet),
      new Path("%s/_header".format(filePath)),
      rdd.context.hadoopConfiguration)
    // get file to write to
    saveAvro("%s/_samples.avro".format(filePath),
      rdd.context,
      sampleSchema,
      samples)

    val contigSchema = Contig.SCHEMA$
    if (sorted) {
      contigSchema.addProp("sorted", "true".asInstanceOf[Any])
      contigSchema.addProp("partitionMap", partitionMap.get.mkString(",").asInstanceOf[Any])
    }

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      contigSchema,
      contigs)
  }
}

/**
 * An abstract class that extends GenomicRDD and where the underlying data is
 * Avro IndexedRecords. This abstract class provides methods for saving to
 * Parquet, and provides hooks for writing the metadata.
 */
abstract class AvroGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroGenomicRDD[T, U]] extends ADAMRDDFunctions[T]
    with GenomicRDD[T, U] {

  /**
   * Called in saveAsParquet after saving RDD to Parquet to save metadata.
   *
   * Writes any necessary metadata to disk. If not overridden, writes the
   * sequence dictionary to disk as Avro.
   *
   * @param filePath The filepath to the file where we will save the Metadata
   */
  protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    val contigSchema = Contig.SCHEMA$
    if (sorted) {
      contigSchema.addProp("sorted", "true".asInstanceOf[Any])
      contigSchema.addProp("partitionMap", partitionMap.get.mkString(",").asInstanceOf[Any])
    }

    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      contigSchema,
      contigs)
  }

  /**
   * Saves RDD as a directory of Parquet files.
   *
   * The RDD is written as a directory of Parquet files, with
   * Parquet configuration described by the input param args.
   * The provided sequence dictionary is written at args.outputPath/_seqdict.avro
   * as Avro binary.
   *
   * @param args Save configuration arguments.
   */
  def saveAsParquet(args: SaveArgs) {
    saveAsParquet(
      args.outputPath,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionaryEncoding
    )
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   *   Default is false.
   */
  def saveAsParquet(
    filePath: String,
    blockSize: Int = 128 * 1024 * 1024,
    pageSize: Int = 1 * 1024 * 1024,
    compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
    disableDictionaryEncoding: Boolean = false) {
    saveRddAsParquet(filePath,
      blockSize,
      pageSize,
      compressCodec,
      disableDictionaryEncoding)
    saveMetadata(filePath)
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   */
  def saveAsParquet(
    filePath: java.lang.String,
    blockSize: java.lang.Integer,
    pageSize: java.lang.Integer,
    compressCodec: CompressionCodecName,
    disableDictionaryEncoding: java.lang.Boolean) {
    saveAsParquet(
      new JavaSaveArgs(filePath,
        blockSize = blockSize,
        pageSize = pageSize,
        compressionCodec = compressCodec,
        disableDictionaryEncoding = disableDictionaryEncoding))
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   */
  def saveAsParquet(filePath: java.lang.String) {
    saveAsParquet(new JavaSaveArgs(filePath))
  }
}

/**
 * A trait for genomic data that is not aligned to a reference (e.g., raw reads).
 */
trait Unaligned {

  val sequences = SequenceDictionary.empty
}
