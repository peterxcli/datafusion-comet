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

package org.apache.comet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;

/** Bulk reads of shuffle block frames from a stream. */
public final class ShuffleBlockReads {

  /** Largest single read: below glibc's 128 KiB mmap threshold, as FileInputStream mallocs it. */
  public static final int MAX_READ = 64 * 1024;

  private ShuffleBlockReads() {}

  /** Reads up to {@code length} bytes into {@code dst}, returning fewer only at end of stream. */
  public static int read(InputStream in, byte[] dst, int length) throws IOException {
    int read = 0;
    while (read < length) {
      checkInterrupted(in);
      int n = in.read(dst, read, length - read);
      if (n < 0) {
        break;
      }
      read += n;
    }
    return read;
  }

  /**
   * Reads up to {@code length} bytes into {@code dst} through {@code scratch}, returning fewer only
   * at end of stream.
   */
  public static int read(InputStream in, byte[] scratch, ByteBuffer dst, int length)
      throws IOException {
    int read = 0;
    while (read < length) {
      checkInterrupted(in);
      int n = in.read(scratch, 0, Math.min(length - read, scratch.length));
      if (n < 0) {
        break;
      }
      dst.put(scratch, 0, n);
      read += n;
    }
    return read;
  }

  /**
   * Closes {@code in} and throws if the thread is interrupted, as an interruptible channel does.
   */
  private static void checkInterrupted(InputStream in) throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      in.close();
      throw new ClosedByInterruptException();
    }
  }
}
