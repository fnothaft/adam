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

import org.apache.avro.specific.SpecificRecord

class AvroProduct[T <% SpecificRecord](val record: T) extends Product {

  /**
   * We can equal the underlying IndexedRecord, or our same class.
   *
   * @param obj An object to see if we can test for equality.
   * @return True if we can compare for equality.
   */
  def canEqual(obj: Any): Boolean = obj match {
    case a: T              => true
    case a: AvroProduct[T] => true
    case _                 => false
  }

  /**
   * @param obj An object to test for equality.
   * @return True if there is an underlying Avro record that matches.
   */
  override def equals(obj: Any): Boolean = obj match {
    case a: T              => record.equals(a)
    case a: AvroProduct[T] => record.equals(a.record)
    case _                 => false
  }

  /**
   * @return The number of fields in a schema.
   */
  def productArity: Int = record.getSchema().getFixedSize()

  /**
   * @param n The index to get the value of.
   * @return The value at a given index.
   */
  def productElement(n: Int): Any = record.get(n)
}
