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

import com.sun.jna.Memory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A view to consume a {@link ResultSubpartition} instance.
 */
public interface ResultSubpartitionView {

	/**
	 * Returns the next {@link Buffer} instance of this queue iterator.
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
	RawMessage getNextRawMessage() throws IOException;

	void notifyDataAvailable();

	void releaseAllResources() throws IOException;

	boolean isReleased();

	void resumeConsumption();

	Throwable getFailureCause();

	boolean isAvailable(int numCreditsAvailable);

	int unsynchronizedGetNumberOfQueuedBuffers();

	abstract class RawMessage {
		private final boolean isDataAvailable;
		private final boolean isEventAvailable;
		final int buffersInBacklog;

		RawMessage(boolean isDataAvailable, boolean isEventAvailable, int buffersInBacklog) {
			this.isDataAvailable = isDataAvailable;
			this.isEventAvailable = isEventAvailable;
			this.buffersInBacklog = buffersInBacklog;
		}

		public boolean isMoreAvailable(int credits) {
			boolean moreAvailable;
			if (credits > 0) {
				moreAvailable = isDataAvailable;
			} else {
				moreAvailable = isEventAvailable;
			}
			return moreAvailable;
		}

		public abstract boolean isBuffer();

		public boolean isDataAvailable() {
			return isDataAvailable;
		}

		public boolean isEventAvailable() {
			return isEventAvailable;
		}

		public int buffersInBacklog() {
			return buffersInBacklog;
		}

		public abstract Buffer getBuffer(MemorySegment segment) throws IOException;

		public abstract NettyMessage buildMessage(InputChannelID id, int sequenceNumber) throws IOException;
	}

	class RawBufferMessage extends RawMessage {
		private final Buffer buffer;

		public RawBufferMessage(Buffer buffer, boolean isDataAvailable, boolean isEventAvailable, int buffersInBacklog) {
			super(isDataAvailable, isEventAvailable, buffersInBacklog);
			this.buffer = buffer;
		}

		@Override
		public NettyMessage buildMessage(InputChannelID id, int sequenceNumber) {
			return new NettyMessage.BufferResponse(
				buffer,
				buffer.getDataType(),
				buffer.isCompressed(),
				sequenceNumber,
				id,
				buffersInBacklog,
				buffer.readableBytes());

		}

		@Override
		public boolean isBuffer() {
			return buffer.isBuffer();
		}

		public Buffer buffer() {
			return buffer;
		}

		public Buffer getBuffer(MemorySegment segment) {
			return buffer;
		}
	}

	class RawFileRegion extends RawMessage {
		private final FileChannel fileChannel;
		private final long position;
		private final int size;
		private final Buffer.DataType dataType;
		private final boolean isCompressed;
		private final ByteBuffer header;

		RawFileRegion(
			FileChannel fileChannel,
			long position,
			int size,
			boolean isDataAvailable,
			boolean isEventAvailable,
			Buffer.DataType dataType,
			boolean isCompressed,
			int buffersInBacklog,
			ByteBuffer header) {

			super(isDataAvailable, isEventAvailable, buffersInBacklog);
			this.fileChannel = fileChannel;
			this.position = position;
			this.size = size;
			this.dataType = dataType;
			this.isCompressed = isCompressed;
			this.header = header;
		}

		@Override
		public NettyMessage buildMessage(InputChannelID id, int sequenceNumber) throws IOException {
			id.writeTo(header);
			header.putInt(sequenceNumber);
			header.putInt(buffersInBacklog);
			header.put((byte) dataType.ordinal());
			header.put((byte) (isCompressed ? 1 : 0));
			header.putInt(size);
			header.flip();
			return new NettyMessage.FileRegionMessage(
				fileChannel,
				position,
				size,
				header);
		}

		public boolean isCompressed() {
			return isCompressed;
		}

		@Override
		public boolean isBuffer() {
			return dataType == Buffer.DataType.DATA_BUFFER;
		}

		public int size() {
			return size;
		}

		public Buffer.DataType dataType() {
			return dataType;
		}

		public FileChannel channel() {
			return fileChannel;
		}

		public Buffer getBuffer(MemorySegment segment) throws IOException {
			final ByteBuffer targetBuf;
			try {
				targetBuf = segment.wrap(0, size);
			}
			catch (BufferUnderflowException | IllegalArgumentException e) {
				// buffer underflow if header buffer is undersized
				// IllegalArgumentException if size is outside memory segment size
				throw new IOException("The spill file is corrupt: buffer size and boundaries invalid");
			}

			BufferReaderWriterUtil.readByteBufferFully(fileChannel, targetBuf);
			return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType, isCompressed, size);
		}
	}
}
