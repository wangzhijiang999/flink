/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.PartitionBuffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.PartitionData;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.PartitionFileRegion;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * BoundedData is the data store in a single bounded blocking subpartition.
 *
 * <h2>Life cycle</h2>
 *
 * <p>The BoundedData is first created during the "write phase" by writing a sequence of buffers
 * through the {@link #writeBuffer(Buffer)} method.
 * The write phase is ended by calling {@link #finishWrite()}.
 * After the write phase is finished, the data can be read multiple times through readers created
 * via {@link #createReader(ResultSubpartitionView)}.
 * Finally, the BoundedData is dropped / deleted by calling {@link #close()}.
 *
 * <h2>Thread Safety and Concurrency</h2>
 *
 * <p>The implementations generally make no assumptions about thread safety.
 * The only contract is that multiple created readers must be able to work independently concurrently.
 */
interface BoundedData extends Closeable {

	/**
	 * Writes this buffer to the bounded data.
	 * This call fails if the writing phase was already finished via {@link #finishWrite()}.
	 */
	void writeBuffer(Buffer buffer) throws IOException;

	/**
	 * Finishes the current region and prevents further writes.
	 * After calling this method, further calls to {@link #writeBuffer(Buffer)} will fail.
	 */
	void finishWrite() throws IOException;

	/**
	 * Gets a reader for the bounded data. Multiple readers may be created.
	 * This call only succeeds once the write phase was finished via {@link #finishWrite()}.
	 */
	BoundedData.Reader createReader(ResultSubpartitionView subpartitionView) throws IOException;

	/**
	 * Gets a reader for the bounded data. Multiple readers may be created.
	 * This call only succeeds once the write phase was finished via {@link #finishWrite()}.
	 */
	default BoundedData.Reader createReader() throws IOException {
		return createReader(new NoOpResultSubpartitionView());
	}

	/**
	 * Gets the number of bytes of all written data (including the metadata in the buffer headers).
	 */
	long getSize();

	// ------------------------------------------------------------------------

	/**
	 * A reader to the bounded data.
	 */
	interface Reader extends Closeable {

		@Nullable
		BoundedPartitionData nextData() throws IOException;
	}

	interface BoundedPartitionData {

		boolean isBuffer();

		PartitionData build(boolean isDataAvailable, boolean isEventAvailable, int dataBufferBacklog);
	}

	final class BoundedPartitionFileRegion implements BoundedPartitionData {

		private final FileChannel channel;
		private final ByteBuffer headerBuffer;
		private final long position;
		private final int count;
		private final Buffer.DataType type;
		private final boolean isCompressed;

		BoundedPartitionFileRegion(
				FileChannel channel,
				ByteBuffer headerBuffer,
				long position,
				int count,
				Buffer.DataType type,
				boolean isCompressed) {

			this.channel = checkNotNull(channel);
			this.headerBuffer = checkNotNull(headerBuffer);
			this.position = position;
			this.count = count;
			this.type = type;
			this.isCompressed = isCompressed;
		}

		@Override
		public boolean isBuffer() {
			return type == Buffer.DataType.DATA_BUFFER;
		}

		@Override
		public PartitionData build(
				boolean isDataAvailable,
				boolean isEventAvailable,
				int dataBufferBacklog) {
			return new PartitionFileRegion(
				channel,
				position,
				count,
				isDataAvailable,
				isEventAvailable,
				type,
				isCompressed,
				dataBufferBacklog,
				headerBuffer) {
			};
		}
	}

	final class BoundedPartitionBuffer implements BoundedPartitionData {

		private final Buffer buffer;

		BoundedPartitionBuffer(Buffer buffer) {
			this.buffer = buffer;
		}

		@Override
		public boolean isBuffer() {
			return buffer.isBuffer();
		}

		@Override
		public PartitionBuffer build(
			boolean isDataAvailable,
			boolean isEventAvailable,
			int dataBufferBacklog) {

			return new PartitionBuffer(
				buffer,
				isDataAvailable,
				isEventAvailable,
				dataBufferBacklog);
		}
	}
}
