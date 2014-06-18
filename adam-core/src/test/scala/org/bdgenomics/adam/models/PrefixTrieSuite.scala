/*
 * Copyright (c) 2014 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

class PrefixTrieSuite extends FunSuite {

  test("kmers with ambiguous bases don't get added to the trie") {
    val trie = PrefixTrie(Map("ANCT" -> 0.5,
      "ACTN" -> 1.0))

    assert(trie.size === 0)
    assert(!trie.contains("ANCT"))
    assert(!trie.contains("ACTN"))
  }

  test("building a trie fails if we have different length keys") {
    intercept[AssertionError] {
      PrefixTrie(Map("ACTCGA" -> 1.2,
        "ACTCA" -> 1.1))
    }
  }

  test("insert keys into a trie, and retrieve them") {
    val trie = PrefixTrie(Map("ACCTA" -> 1,
      "ACTGA" -> 2,
      "CCTCA" -> 3))

    assert(trie.size === 3)

    // check for values inserted into trie
    assert(trie.contains("ACCTA"))
    assert(trie.get("ACCTA") === 1)
    assert(trie.contains("ACTGA"))
    assert(trie.get("ACTGA") === 2)
    assert(trie.contains("CCTCA"))
    assert(trie.get("CCTCA") === 3)
  }

  val sampleTrie = PrefixTrie(Map(
    "AACACT" -> 1,
    "AACACC" -> 4,
    "ATGGTC" -> 2,
    "CACTGC" -> 5,
    "CCTCGA" -> 4,
    "GGCGTC" -> 6,
    "TCCTCG" -> 4,
    "TTCTTC" -> 2))

  test("perform a wildkey search") {
    val foundKVs = sampleTrie.search("A****C")

    assert(foundKVs.size === 2)
    assert(foundKVs("AACACC") === 4)
    assert(foundKVs("ATGGTC") === 2)
  }

  test("perform a prefix search") {
    val foundKVs = sampleTrie.prefixSearch("AACA")

    assert(foundKVs.size === 2)
    assert(foundKVs("AACACT") === 1)
    assert(foundKVs("AACACC") === 4)
  }

  test("perform a suffix search") {
    val foundKVs = sampleTrie.suffixSearch("TC")

    assert(foundKVs.size === 3)
    assert(foundKVs("ATGGTC") === 2)
    assert(foundKVs("GGCGTC") === 6)
    assert(foundKVs("TTCTTC") === 2)
  }

  test("test getters") {
    // test on a key that is in the trie
    assert(sampleTrie.get("AACACT") === 1)
    assert(sampleTrie.getOrElse("AACACT", 4) === 1)
    assert(sampleTrie.getIfExists("AACACT").isDefined)
    assert(sampleTrie.getIfExists("AACACT").get === 1)

    // test on a key that is not in the trie
    intercept[IllegalArgumentException] {
      sampleTrie.get("AAGACT")
    }
    assert(sampleTrie.getOrElse("AAGACT", 4) === 4)
    assert(sampleTrie.getIfExists("AAGACT").isEmpty)
  }
}
