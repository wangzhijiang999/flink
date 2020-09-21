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

import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The reader (read view) of a BoundedBlockingSubpartition.
 */
final class BoundedBlockingSubpartitionReader implements ResultSubpartitionView {

	/** The result subpartition that we read. */
	private final BoundedBlockingSubpartition parent;

	/** The reader/decoder to the memory mapped region with the data we currently read from.
	 * Null once the reader empty or disposed.*/
	@Nullable
	private BoundedData.Reader dataReader;

	/** The remaining number of data buffers (not events) in the result. */
	private int dataBufferBacklog;

	private int numTotalBuffers;

	/** Flag whether this reader is released. Atomic, to avoid double release. */
	private boolean isReleased;

	/**
	 * Convenience constructor that takes a single buffer.
	 */
	BoundedBlockingSubpartitionReader(
			BoundedBlockingSubpartition parent,
			BoundedData data,
			int numDataBuffers,
			int numTotalBuffers) throws IOException {

		this.parent = checkNotNull(parent);

		checkNotNull(data);
		this.dataReader = data.createReader(this);

		checkArgument(numDataBuffers >= 0);
		this.dataBufferBacklog = numDataBuffers;

		checkArgument(numTotalBuffers >= 0);
		this.numTotalBuffers = numTotalBuffers;
	}

	@Nullable
	@Override
	public RawMessage getNextRawMessage() throws IOException {
		assert dataReader != null;

		BoundedData.RawData current = dataReader.nextData();
		if (current == null) {
			return null;
		}

		if (current.isBuffer()) {
			dataBufferBacklog--;
		}
		numTotalBuffers--;

		return current.buildRawMessage(
			numTotalBuffers > 0,
			false,
			dataBufferBacklog);
	}

	@Override
	public void notifyDataAvailable() {
	}

	@Override
	public void releaseAllResources() throws IOException {
		// it is not a problem if this method executes multiple times
		isReleased = true;

		IOUtils.closeQuietly(dataReader);

		// nulling these fields means the read method and will fail fast
		dataReader = null;

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
	public void resumeConsumption() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public boolean isAvailable(int numCreditsAvailable) {
		return numCreditsAvailable > 0 && numTotalBuffers > 0;
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
	public String toString() {
		return String.format("Blocking Subpartition Reader: ID=%s, index=%d",
				parent.parent.getPartitionId(),
				parent.getSubPartitionIndex());
	}
}
