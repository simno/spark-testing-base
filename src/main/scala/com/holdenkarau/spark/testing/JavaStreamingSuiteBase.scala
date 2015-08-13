/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.holdenkarau.spark.testing

import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag


import org.apache.spark._
import org.apache.spark.api.java.JavaRDDLike
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.api.java.JavaDStream._
import org.apache.spark.streaming.dstream.DStream


/** Exposes streaming test functionality in a Java-friendly way */
trait JavaStreamingSuiteBase extends StreamingSuiteBase {
  /**
   * Create a test stream and attach it to the supplied context.
   * The stream will be created from the supplied lists of Java objects.
   */
  def attachTestInputStream[T](sc: SparkContext, ssc_ : TestStreamingContext,
      data: JList[JList[T]]): JavaDStream[T] = {
    val seqData: Seq[Seq[T]] = data.asScala.map(_.asScala)
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    new JavaDStream[T](createTestInputStream(sc, ssc_, seqData))
  }

  /**
   * Test unary JavaDStream operation with a list of inputs, with number of
   * batches to run same as the number of expected output values
   */
  def testJavaOperation[U, V](
    input: JList[JList[U]],
    operation: JavaDStream[U] => JavaDStream[V],
    expectedOutput: JList[JList[V]],
    useSet: Boolean) {
    testJavaOperation(input, operation, expectedOutput, input.size(), useSet)
  }
  /**
   * Test unary JavaDStream operation with a list of inputs, with number of
   * batches to run same as the number of expected output values
    * @param input      Sequence of input collections
    * @param operation  Binary DStream operation to be applied to the 2 inputs
    * @param expectedOutput List of expected output collections
    * @param numBatches Number of batches to run the operation for
    * @param useSet     Compare the output values with the expected output values
    *                   as sets (order matters) or as lists (order does not matter)
   */
  def testJavaOperation[U, V](
    input: JList[JList[U]],
    operation: JavaDStream[U] => JavaDStream[V],
    expectedOutput: JList[JList[V]],
    numBatches: Int,
    useSet: Boolean) {
    implicit val cm1: ClassTag[U] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[U]]
    implicit val cm2: ClassTag[V] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[V]]
    def wrappedOperation(input: DStream[U]): DStream[V] = {
      operation(new JavaDStream(input)).dstream
    }
  }
}
