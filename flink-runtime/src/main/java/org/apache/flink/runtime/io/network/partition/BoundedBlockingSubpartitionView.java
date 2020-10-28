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
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The reader (read view) of a BoundedBlockingSubpartition based on
 * {@link org.apache.flink.shaded.netty4.io.netty.channel.FileRegion}.
 */
public class BoundedBlockingSubpartitionView implements ResultSubpartitionView {

	/** The result subpartition that we read. */
	private final BoundedBlockingSubpartition parent;

	/** The reader/decoder to the file region with the data we currently read from. */
	private final BoundedData.Reader dataReader;

	/** The remaining number of data buffers (not events) in the result. */
	private int dataBufferBacklog;

	/** The remaining number of data buffers and events in the result. */
	private int numBuffersAndEvents;

	/** Flag whether this reader is released. */
	private boolean isReleased;

	private int sequenceNumber;

	BoundedBlockingSubpartitionView(
		BoundedBlockingSubpartition parent,
		Path filePath,
		int numDataBuffers,
		int numBuffersAndEvents) throws IOException {

		this.parent = checkNotNull(parent);

		checkNotNull(filePath);
		this.dataReader = new FileRegionReader(filePath);

		checkArgument(numDataBuffers >= 0);
		this.dataBufferBacklog = numDataBuffers;

		checkArgument(numBuffersAndEvents >= 0);
		this.numBuffersAndEvents = numBuffersAndEvents;
	}

	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() throws IOException {
		if (isReleased) {
			return null;
		}

		Buffer current = dataReader.nextBuffer();
		if (current == null) {
			// as per contract, we must return null when the reader is empty,
			// but also in case the reader is disposed (rather than throwing an exception)
			return null;
		}

		updateStatistics(current);

		// We can assume all the data are non-events for batch jobs to avoid pre-fetching the next header
		Buffer.DataType nextDataType = numBuffersAndEvents > 0 ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.NONE;
		return BufferAndBacklog.fromBufferAndLookahead(current, nextDataType, dataBufferBacklog, sequenceNumber++);
	}

	private void updateStatistics(Buffer data) {
		if (data.isBuffer()) {
			dataBufferBacklog--;
		}
		numBuffersAndEvents--;
	}

	@Override
	public boolean isAvailable(int numCreditsAvailable) {
		// For batch jobs we can assume there are no other events except EndOfPartitionEvent, then it has
		// no essential effect to ignore whether the next buffer is event for simplification.
		return numCreditsAvailable > 0 && numBuffersAndEvents > 0;
	}

	@Override
	public void releaseAllResources() throws IOException {
		// it is not a problem if this method executes multiple times
		isReleased = true;

		IOUtils.closeQuietly(dataReader);

		// Notify the parent that this one is released. This allows the parent to
		// eventually release all resources (when all readers are done and the
		// parent is disposed).
		parent.releaseReaderReference(this);
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public Throwable getFailureCause() {
		// we can never throw an error after this was created
		return null;
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return parent.unsynchronizedGetNumberOfQueuedBuffers();
	}

	@Override
	public void notifyDataAvailable() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public void resumeConsumption() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public String toString() {
		return String.format("Blocking Subpartition Reader: ID=%s, index=%d",
			parent.parent.getPartitionId(),
			parent.getSubPartitionIndex());
	}

	/**
	 * The reader to wrap {@link org.apache.flink.shaded.netty4.io.netty.channel.FileRegion}
	 * based buffer from {@link BoundedBlockingSubpartition}.
	 */
	static final class FileRegionReader implements BoundedData.Reader {

		private final FileChannel fileChannel;

		private final ByteBuffer headerBuffer;

		private long lastPosition = -1L;

		private int lastBufferSize;

		FileRegionReader(Path filePath) throws IOException {
			this.fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);
			this.headerBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();
		}

		@Nullable
		@Override
		public Buffer nextBuffer() throws IOException {
			// As the file region transfer via network channel will not modify the underlining file position,
			// then we need update the position with last buffer size before reading the next buffer header.
			if (fileChannel.position() == lastPosition) {
				fileChannel.position(fileChannel.position() + lastBufferSize);
			}

			Buffer buffer = BufferReaderWriterUtil.readFromByteChannel(fileChannel, headerBuffer);
			if (buffer != null) {
				lastBufferSize = buffer.getSize();
				lastPosition = fileChannel.position();
			}
			return buffer;
		}

		@Override
		public void close() throws IOException {
			fileChannel.close();
		}
	}
}
