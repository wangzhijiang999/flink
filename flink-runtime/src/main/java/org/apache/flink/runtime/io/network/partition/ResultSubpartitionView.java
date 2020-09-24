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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import org.apache.flink.runtime.io.network.netty.NettyMessage.FileRegionResponse;
import org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionResponseMessage;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A view to consume a {@link ResultSubpartition} instance.
 */
public interface ResultSubpartitionView {

	/**
	 * Returns the next {@link PartitionData} instance of this queue iterator.
	 *
	 * <p>If there is currently no instance available, it will return <code>null</code>.
	 * This might happen for example when a pipelined queue producer is slower
	 * than the consumer or a spilled queue needs to read in more data.
	 *
	 * <p><strong>Important</strong>: The consumer has to make sure that each
	 * buffer instance will eventually be recycled with {@link Buffer#recycleBuffer()}
	 * after it has been consumed.
	 */
	@Nullable
	PartitionData getNextData() throws IOException;

	void notifyDataAvailable();

	void releaseAllResources() throws IOException;

	boolean isReleased();

	void resumeConsumption();

	Throwable getFailureCause();

	boolean isAvailable(int numCreditsAvailable);

	int unsynchronizedGetNumberOfQueuedBuffers();

	abstract class PartitionData {

		private final boolean isDataAvailable;
		private final boolean isEventAvailable;
		private final int buffersInBacklog;

		PartitionData(boolean isDataAvailable, boolean isEventAvailable, int buffersInBacklog) {
			this.isDataAvailable = isDataAvailable;
			this.isEventAvailable = isEventAvailable;
			this.buffersInBacklog = buffersInBacklog;
		}

		public abstract boolean isBuffer();

		public abstract Buffer getBuffer(MemorySegment segment, BufferRecycler recycler) throws IOException;

		public abstract PartitionResponseMessage buildMessage(InputChannelID id, int sequenceNumber) throws IOException;

		public boolean isMoreAvailable(int credits) {
			return credits > 0 ? isDataAvailable : isEventAvailable;
		}

		public boolean isDataAvailable() {
			return isDataAvailable;
		}

		@VisibleForTesting
		boolean isEventAvailable() {
			return isEventAvailable;
		}

		int buffersInBacklog() {
			return buffersInBacklog;
		}
	}

	class PartitionBuffer extends PartitionData {

		private final Buffer buffer;

		public PartitionBuffer(Buffer buffer, boolean isDataAvailable, boolean isEventAvailable, int buffersInBacklog) {
			super(isDataAvailable, isEventAvailable, buffersInBacklog);
			this.buffer = checkNotNull(buffer);
		}

		@Override
		public PartitionResponseMessage buildMessage(InputChannelID id, int sequenceNumber) {
			return new BufferResponse(
				buffer,
				buffer.getDataType(),
				buffer.isCompressed(),
				sequenceNumber,
				id,
				buffersInBacklog(),
				buffer.readableBytes());
		}

		@Override
		public boolean isBuffer() {
			return buffer.isBuffer();
		}

		@VisibleForTesting
		public Buffer buffer() {
			return buffer;
		}

		@Override
		public Buffer getBuffer(MemorySegment segment, BufferRecycler recycler) {
			return buffer;
		}
	}

	class PartitionFileRegion extends PartitionData {

		private final FileChannel fileChannel;
		private final long position;
		private final int count;
		private final Buffer.DataType dataType;
		private final boolean isCompressed;
		private final ByteBuffer headerBuffer;

		PartitionFileRegion(
			FileChannel fileChannel,
			long position,
			int count,
			boolean isDataAvailable,
			boolean isEventAvailable,
			Buffer.DataType dataType,
			boolean isCompressed,
			int buffersInBacklog,
			ByteBuffer headerBuffer) {

			super(isDataAvailable, isEventAvailable, buffersInBacklog);

			this.fileChannel = checkNotNull(fileChannel);
			this.position = position;
			this.count = count;
			this.dataType = dataType;
			this.isCompressed = isCompressed;
			this.headerBuffer = checkNotNull(headerBuffer);
		}

		@Override
		public PartitionResponseMessage buildMessage(InputChannelID id, int sequenceNumber) throws IOException {
			id.writeTo(headerBuffer);
			headerBuffer.putInt(sequenceNumber);
			headerBuffer.putInt(buffersInBacklog());
			headerBuffer.put((byte) dataType.ordinal());
			headerBuffer.put((byte) (isCompressed ? 1 : 0));
			headerBuffer.putInt(count);

			headerBuffer.flip();

			return new FileRegionResponse(fileChannel, position, count, headerBuffer);
		}

		@Override
		public boolean isBuffer() {
			return dataType == Buffer.DataType.DATA_BUFFER;
		}

		public Buffer getBuffer(MemorySegment segment, BufferRecycler recycler) throws IOException {
			final ByteBuffer targetBuf;
			try {
				targetBuf = segment.wrap(0, count);
			}
			catch (BufferUnderflowException | IllegalArgumentException e) {
				// buffer underflow if header buffer is undersized
				// IllegalArgumentException if size is outside memory segment size
				throw new IOException("The spill file is corrupt: buffer size and boundaries invalid");
			}

			BufferReaderWriterUtil.readByteBufferFully(fileChannel, targetBuf);
			return new NetworkBuffer(segment, recycler, dataType, isCompressed, count);
		}
	}
}
