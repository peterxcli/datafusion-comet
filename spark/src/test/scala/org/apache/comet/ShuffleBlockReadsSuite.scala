/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet

import java.io.{ByteArrayInputStream, EOFException, InputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.ClosedByInterruptException

import org.scalatest.funsuite.AnyFunSuite

class ShuffleBlockReadsSuite extends AnyFunSuite {

  /** Serves at most `step` bytes per read and records the largest request. */
  private class TrickleStream(bytes: Array[Byte], step: Int) extends InputStream {
    private val in = new ByteArrayInputStream(bytes)
    var largestRequest = 0
    var closed = false

    override def read(): Int = in.read()

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      largestRequest = largestRequest.max(len)
      in.read(b, off, len.min(step))
    }

    override def close(): Unit = closed = true
  }

  private def bytes(n: Int): Array[Byte] = Array.tabulate(n)(i => (i * 31).toByte)

  private def frame(body: Array[Byte]): Array[Byte] = {
    val header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
    header.putLong(body.length + 8L).putLong(1L)
    header.array() ++ body
  }

  test("reads the full length across short reads") {
    val source = bytes(1000)
    val dst = new Array[Byte](1000)
    assert(ShuffleBlockReads.read(new TrickleStream(source, 7), dst, 1000) == 1000)
    assert(dst.sameElements(source))
  }

  test("returns fewer bytes only at end of stream") {
    val dst = new Array[Byte](100)
    assert(ShuffleBlockReads.read(new TrickleStream(bytes(40), 3), dst, 100) == 40)
  }

  test("fills a direct buffer in reads no larger than the scratch") {
    val size = ShuffleBlockReads.MAX_READ * 3 + 17
    val source = bytes(size)
    val stream = new TrickleStream(source, Int.MaxValue)
    val dst = ByteBuffer.allocateDirect(size)
    assert(
      ShuffleBlockReads.read(stream, new Array[Byte](ShuffleBlockReads.MAX_READ), dst, size) ==
        size)
    assert(stream.largestRequest <= ShuffleBlockReads.MAX_READ)
    dst.flip()
    val copied = new Array[Byte](size)
    dst.get(copied)
    assert(copied.sameElements(source))
  }

  test("an interrupted read closes the stream and keeps the interrupt") {
    val stream = new TrickleStream(bytes(100), 10)
    Thread.currentThread().interrupt()
    try {
      intercept[ClosedByInterruptException] {
        ShuffleBlockReads.read(stream, new Array[Byte](100), 100)
      }
      assert(stream.closed)
      assert(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
    }
  }

  test("block iterator frames blocks across short reads") {
    val first = bytes(300)
    val second = bytes(ShuffleBlockReads.MAX_READ + 5)
    val iter = new CometShuffleBlockIterator(new TrickleStream(frame(first) ++ frame(second), 5))
    for (body <- Seq(first, second)) {
      assert(iter.hasNext() == body.length)
      val buffer = iter.getBuffer.duplicate()
      buffer.position(0).limit(body.length)
      val copied = new Array[Byte](body.length)
      buffer.get(copied)
      assert(copied.sameElements(body))
    }
    assert(iter.hasNext() == -1)
  }

  test("block iterator treats a partial header or body as corruption") {
    val framed = frame(bytes(64))
    intercept[EOFException] {
      new CometShuffleBlockIterator(new ByteArrayInputStream(framed.take(10))).hasNext()
    }
    intercept[EOFException] {
      new CometShuffleBlockIterator(new ByteArrayInputStream(framed.dropRight(1))).hasNext()
    }
  }
}
