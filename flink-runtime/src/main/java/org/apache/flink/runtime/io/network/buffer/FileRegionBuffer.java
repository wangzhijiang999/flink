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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Wrapper for {@link org.apache.flink.shaded.netty4.io.netty.channel.FileRegion} instances.
 *
 * <p>The file region can be sent via network channel which supports zero-copy file transfer,
 * and it can also be read into memory segment locally via provided {@link #readInto(MemorySegment)}.
 */
public class FileRegionBuffer extends DefaultFileRegion implements Buffer {

	/** The number of bytes to be read or transferred from this file region. */
	private final int bufferSize;

	private final FileChannel fileChannel;

	/** The {@link DataType} this buffer represents. */
	private DataType dataType;

	/** Whether the buffer is compressed or not. */
	private boolean isCompressed;

	public FileRegionBuffer(
			FileChannel fileChannel,
			int count,
			DataType dataType,
			boolean isCompressed) throws IOException {

		super(fileChannel, fileChannel.position(), count);

		this.fileChannel = checkNotNull(fileChannel);
		this.bufferSize = count;
		this.dataType = checkNotNull(dataType);
		this.isCompressed = isCompressed;
	}

	// ------------------------------------------------------------------------
	// Buffer override methods
	// ------------------------------------------------------------------------

	@Override
	public boolean isBuffer() {
		return dataType.isBuffer();
	}

	@Override
	public MemorySegment getMemorySegment() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public int getMemorySegmentOffset() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public ReadOnlySlicedNetworkBuffer readOnlySlice() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public ReadOnlySlicedNetworkBuffer readOnlySlice(int index, int length) {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public int getMaxCapacity() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public int getReaderIndex() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public ByteBuffer getNioBufferReadable() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public ByteBuf asByteBuf() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public void setSize(int writerIndex) {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public int getSize() {
		return bufferSize;
	}

	@Override
	public int readableBytes() {
		return bufferSize;
	}

	@Override
	public void setAllocator(ByteBufAllocator allocator) {
		// nothing to do
	}

	@Override
	public BufferRecycler getRecycler() {
		return null;
	}

	@Override
	public void recycleBuffer() {
		// nothing to do
	}

	@Override
	public boolean isRecycled() {
		return false;
	}

	@Override
	public FileRegionBuffer retainBuffer() {
		return (FileRegionBuffer) super.retain();
	}

	@Override
	public boolean isCompressed() {
		return isCompressed;
	}

	@Override
	public void setCompressed(boolean isCompressed) {
		this.isCompressed = isCompressed;
	}

	@Override
	public DataType getDataType() {
		return dataType;
	}

	@Override
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	// ------------------------------------------------------------------------
	// File region override methods
	// ------------------------------------------------------------------------

	@Override
	public void deallocate() {
		// nothing to do
	}

	// ------------------------------------------------------------------------

	public Buffer readInto(MemorySegment segment) throws IOException {
		final ByteBuffer buffer = segment.wrap(0, bufferSize);
		BufferReaderWriterUtil.readByteBufferFully(fileChannel, buffer);

		return new NetworkBuffer(
			segment,
			BufferRecycler.DummyBufferRecycler.INSTANCE,
			dataType,
			isCompressed,
			bufferSize);
	}
}
