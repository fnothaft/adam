# Architecture {#architecture}

ADAM is architected as an extensible, parallel framework for working with both
aligned and unaligned genomic data using [Apache
Spark](https://spark.apache.org). Unlike traditional genomics tools, ADAM is
built as a modular stack, where a set of schemas define and provide a narrow
waist. This stack architecture enables the support of a wide range of data
formats and optimized query patterns without changing the data structures and
query patterns that users are programming against.

## The ADAM Stack Model {#stack-model}

The stack model that ADAM is based upon was introduced in [@massie13] and
further refined in [@nothaft15], and is depicted in the figure below. This
stack model seperates computational patterns from the data model, and the
data model from the serialized representation of the data on disk. This
enables developers to write queries that will run seamlessly on both a single
node, or on a distributed cluster, on legacy genomics data files or on
data stored in a high performance columnar storage format, on sorted or
unsorted data, without making any modifications to their query. Additionally,
this allows developers to write at a higher level of abstraction without
sacrificing performance, since we have the freedom to change the implementation
of a layer of the stack in order to improve the performance of a given query.

![The ADAM Stack Model](source/img/stack-model.pdf)

This stack model is divided into seven levels. Starting from the bottom,
these are:

1. The *physical storage* layer is the type of storage media (e.g., hard
   disk/solid state drives) that are used to store the data.
2. The *data distribution* layer determines how data is made accessible to all
   of the nodes in the cluster. Data may be made accessible through a
   POSIX-compliant shared file system such as NFS [@sandberg85], a non-POSIX
   file system that implements Hadoop's APIs (e.g., HDFS), or through a
   block-store, such as Amazon S3.
3. The *materialized data* layer defines how the logical data in a single
   record maps to bytes on disk. We advocate for the use of [Apache
   Parquet](https://parquet.apache.org), a high performance columnar storage
   format based off of Google's Dremel database [@melnik10]. To support
   conventional genomics file formats, we exchange the Parquet implementation
   of this layer with a layer that maps the given schema into a traditional
   genomics file format (e.g., SAM/BAM/CRAM, BED/GTF/GFF/NarrowPeak, VCF).
4. The *schema* layer defines the logical representation of a datatype.
5. The *evidence access* layer is the software layer that implements and
   executes queries against data stored on disk. While ADAM was originally
   built around Apache Spark's Resilient Distributed Dataset (RDD)
   API [@zaharia12], ADAM has recently enabled the use of Spark
   SQL [@armbrust15] for evidence access and query.
6. The *presentation* layer provides high level abstractions for interacting
   with a parallel collection of genomic data. In ADAM, we implement this layer
   through the [GenomicRDD](#genomic-rdd) classes. This layer presents users
   with a view of the metadata associated with a collection of genomic data,
   and APIs for [transforming](#transforming) and [joining](#join) genomic data.
   Additionally, this is the layer where we provide cross-language support.
7. The *application* layer is the layer where a user writes their application
   on top of the provided APIs.

Our stack model derives its inspiration from the layered Open Systems
Interconnection (OSI) networking stack [@zimmermann80], which eventually served
as the inspiration for the Internet Protocol stack, and from the concept of data
independence in database systems. We see this approach as an alternative to the
"stack smashing" commonly seen in genomics APIs, such as the GATK's "walker"
interface [@mckenna10]. In these APIs, implementation details about the layout
of data on disk (is the data sorted?) are propegated up to the application layer
of the API and exposed to user code. This limits the sorts of queries that can
be expressed efficiently to queries that can easily be run against linear sorted
data. Additionally, these "smashed stacks" typically expose very low level APIs,
such as a sorted iterator, which increases the cost to implementing a query and
can lead to trivial mistakes.

## The bdg-formats schemas {#schemas}

The schemas that comprise ADAM's narrow waist are defined in the
[bdg-formats](https://github.com/bigdatagenomics/bdg-formats) project, using the
[Apache Avro](https://avro.apache.org) schema description language. This schema
definition language automatically generates implementations of this schema for
multiple common languages, including Java, C, C++, and Python. bdg-formats
contains 15 schemas in total, with seven core schemas:

* The *AlignmentRecord* schema represents a genomic read, along with that read's
  alignment to a reference genome, if available.
* The *Feature* schema represents a generic genomic feature. This record can be
  used to tag a region of the genome with an annotation, such as coverage
  observed over that region, or the coordinates of an exon.
* The *Fragment* schema represents a set of read alignments that came from a
  single sequenced fragment.
* The *Genotype* schema represents a genotype call, along with annotations about
  the quality/read support of the called genotype.
* The *NucleotideContigFragment* schema represents a section of a contig's
  sequence.
* The *Variant* schema represents a sequence variant, along with statistics
  about that variant's support across a group of samples, and annotations about
  the effect of the variant.

The bdg-formats schemas are designed so that common fields are easy to query,
while maintaining extensibility and the ability to interoperate with common
genomics file formats. Where necessary, the bdg-formats schemas are nested,
which allows for the description of complex nested features and groupings (such
as the Fragment record, which groups together AlignmentRecords). All fields in
the bdg-formats schemas are nullable, and the schemas themselves do not contain
invariants around valid values for a field. Instead, we validate data on ingress
and egress to/from a conventional genomic file format. This allows users to take
advantage of features such as field projection, which can improve the
performance of queries like [flagstat](#flagstat) by an order of magnitude.

## Interacting with data through ADAM's evidence access layer {#evidence-access}

ADAM exposes access to distributed datasets of genomic data through the
[ADAMContext](#adam-context) entrypoint. The ADAMContext wraps Apache Spark's
SparkContext, which tracks the configuration and state of the current running
Spark application. On top of the SparkContext, the ADAMContext provides data
loading functions which yield [GenomicRDD](#genomic-rdd)s. The GenomicRDD
classes provide a wrapper around Apache Spark's two APIs for manipulating
distributed datasets: the legacy Resilient Distributed Dataset [@zaharia12] and
the new Spark SQL Dataset/DataFrame API [@armbrust15]. Additionally, the
GenomicRDD is enriched with genomics-specific metadata such as computational
lineage and sample metadata, and optimized genomics-specific query patterns
such as [region joins](#join) and the [auto-parallelizing pipe API](#pipes)
for running legacy tools using Apache Spark.

// todo: add description of metadata across different genomicrdd impl's
// todo: add diagram of genomicrdds and legacy formats