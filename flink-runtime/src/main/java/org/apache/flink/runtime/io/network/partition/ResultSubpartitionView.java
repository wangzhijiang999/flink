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
import org.apache.flink.runtime.io.network.buffer.DummyBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage;
import org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import org.apache.flink.runtime.io.network.netty.NettyMessage.FileRegionResponse;
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

	default void notifyPriorityEvent(int priorityBufferNumber) {
	}

	void releaseAllResources() throws IOException;

	boolean isReleased();

	void resumeConsumption();

	Throwable getFailureCause();

	boolean isAvailable(int numCreditsAvailable);

	int unsynchronizedGetNumberOfQueuedBuffers();

	/**
	 * Partition data.
	 */
	abstract class PartitionData {

		private final int backlog;
		private final int sequenceNumber;
		private final Buffer.DataType nextDataType;

		PartitionData(Buffer.DataType nextDataType, int backlog, int sequenceNumber) {
			this.nextDataType = checkNotNull(nextDataType);
			this.backlog = backlog;
			this.sequenceNumber = sequenceNumber;
		}

		public abstract boolean isBuffer();

		public abstract Buffer getBuffer(@Nullable MemorySegment segment) throws IOException;

		public abstract NettyMessage buildMessage(InputChannelID id) throws IOException;

		public Buffer.DataType getNextDataType() {
			return nextDataType;
		}

		int buffersInBacklog() {
			return backlog;
		}

		public int getSequenceNumber() {
			return sequenceNumber;
		}

		public boolean isDataAvailable() {
			return nextDataType != Buffer.DataType.NONE;
		}
	}

	/**
	 * Partition buffer.
	 */
	class PartitionBuffer extends PartitionData {

		private final Buffer buffer;

		public PartitionBuffer(Buffer buffer, int backlog, Buffer.DataType nextDataType, int sequenceNumber) {
			super(nextDataType, backlog, sequenceNumber);
			this.buffer = checkNotNull(buffer);
		}

		@Override
		public NettyMessage buildMessage(InputChannelID id) {
			return new BufferResponse(
				buffer,
				new NettyMessage.ResponseInfo(
					id,
					getSequenceNumber(),
					buffersInBacklog(),
					buffer.getDataType(),
					buffer.isCompressed()),
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
		public Buffer getBuffer(MemorySegment segment) {
			return buffer;
		}
	}

	/**
	 * Partition file region.
	 */
	class PartitionFileRegion extends PartitionData {

		private final FileChannel fileChannel;
		private final long position;
		private final int count;
		private final Buffer.DataType dataType;
		private final boolean isCompressed;

		PartitionFileRegion(
			FileChannel fileChannel,
			long position,
			int count,
			Buffer.DataType dataType,
			Buffer.DataType nextDataType,
			boolean isCompressed,
			int backlog,
			int sequenceNumber) {

			super(nextDataType, backlog, sequenceNumber);

			this.fileChannel = checkNotNull(fileChannel);
			this.position = position;
			this.count = count;
			this.dataType = checkNotNull(dataType);
			this.isCompressed = isCompressed;
		}

		@Override
		public NettyMessage buildMessage(InputChannelID id) throws IOException {
			return new FileRegionResponse(
				fileChannel,
				position,
				count,
				new NettyMessage.ResponseInfo(
					id,
					getSequenceNumber(),
					buffersInBacklog(),
					dataType,
					isCompressed));
		}

		@Override
		public boolean isBuffer() {
			return dataType == Buffer.DataType.DATA_BUFFER;
		}

		public Buffer getBuffer(MemorySegment segment) throws IOException {
			final ByteBuffer buffer;
			try {
				buffer = segment.wrap(0, count);
			}
			catch (BufferUnderflowException | IllegalArgumentException e) {
				// buffer underflow if header buffer is undersized
				// IllegalArgumentException if size is outside memory segment size
				throw new IOException("The spill file is corrupt: buffer size and boundaries invalid");
			}

			BufferReaderWriterUtil.readByteBufferFully(fileChannel, buffer);
			return new NetworkBuffer(segment, DummyBufferRecycler.INSTANCE, dataType, isCompressed, count);
		}
	}
}
