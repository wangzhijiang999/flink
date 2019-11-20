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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.InputPersister;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.io.IOException;

/**
 * {@link UnalignedCheckpointBarrierHandler} keep tracks of received {@link CheckpointBarrier} on given
 * channels and update the checkpoint flag when receiving the first barrier. Also it would persist the
 * received buffers for the channel without barrier yet.
 */
@Internal
public class UnalignedCheckpointBarrierHandler extends CheckpointBarrierHandler {
	/** Flags that indicate whether a channel already received barrier. */
	private final boolean[] barrierReceivedChannels;

	private CheckpointBarrier currentBarrier;

	private int totalNumberOfInputChannels;

	/** The number of already closed channels. */
	private int numClosedChannels;

	private int numReceivedChannels;

	private final InputPersister persister;

	public UnalignedCheckpointBarrierHandler(
			int totalNumberOfInputChannels,
			AbstractInvokable invokable,
			InputPersister persister) {
		super(invokable);

		this.persister = persister;
		this.totalNumberOfInputChannels = totalNumberOfInputChannels;
		this.barrierReceivedChannels = new boolean[totalNumberOfInputChannels];
	}

	@Override
	public void notifyReceivedBuffer(Buffer buffer, int channelIndex) throws IOException {
		persister.addBuffer(buffer, channelIndex);
	}

	@Override
	public void notifyReceivedBarrier(CheckpointBarrier barrier, int channelIndex) throws IOException {
		processBarrier(barrier, channelIndex, 0);
	}

	@Override
	public synchronized boolean processBarrier(CheckpointBarrier barrier, int channelIndex, long bufferedBytes) throws IOException {
		final long barrierId = barrier.getId();
		final long currentCheckpointId = currentBarrier.getId();
		if (numReceivedChannels > 0) {
			if (barrierId == currentCheckpointId) {
				onBarrier(channelIndex);
			}
			// TODO we do not expect this case happen because it would waste the previous persisted buffers,
			// and also need to cleanup the partial files. We can adjust the checkpoint coordinator to trigger
			// the next checkpoint after the current one finished.
			else if (barrierId > currentCheckpointId) {
				// let the task know we are not completing this
				notifyAbort(
					currentCheckpointId,
					new CheckpointException("Barrier id: " + barrierId, CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED));

				// begin a new checkpoint
				beginNewAlignment(barrier, channelIndex);
			}
		} else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint
			beginNewAlignment(barrier, channelIndex);
		}

		if (numReceivedChannels + numClosedChannels == totalNumberOfInputChannels) {
			releaseBlocksAndResetBarriers();
		}
		return true;
	}

	@Override
	public synchronized boolean processEndOfPartition() throws Exception {
		numClosedChannels++;

		if (numReceivedChannels > 0) {
			// let the task know we skip a checkpoint
			notifyAbort(
				currentBarrier.getId(),
				new CheckpointException("Barrier id: " + currentBarrier.getId(), CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED));
		}
		return true;
	}

	@Override
	public synchronized void releaseBlocksAndResetBarriers() {
		for (int i = 0; i < barrierReceivedChannels.length; i++) {
			barrierReceivedChannels[i] = false;
		}

		numReceivedChannels = 0;
	}

	private synchronized void beginNewAlignment(CheckpointBarrier checkpointBarrier, int channelIndex) throws IOException {
		currentBarrier = checkpointBarrier;
		onBarrier(channelIndex);
		notifyCheckpoint();
	}

	private void notifyCheckpoint() {
		if (toNotifyOnCheckpoint != null) {
			toNotifyOnCheckpoint.triggerCheckpointAsync(
				new CheckpointMetaData(currentBarrier.getId(), currentBarrier.getTimestamp()),
				currentBarrier.getCheckpointOptions(),
				false);
		}
	}

	/**
	 * Blocks the given channel index, from which a barrier has been received.
	 *
	 * @param channelIndex The channel index to block.
	 */
	private synchronized void onBarrier(int channelIndex) throws IOException {
		if (!barrierReceivedChannels[channelIndex]) {
			barrierReceivedChannels[channelIndex] = true;

			numReceivedChannels++;
		} else {
			throw new IOException("Stream corrupt: Repeated barrier for same checkpoint on input " + channelIndex);
		}
	}

	@Override
	public boolean isBlocked(int channel) {
		return false;
	}

	@Override
	public boolean processCancellationBarrier(CancelCheckpointMarker cancelBarrier) {
		return false;
	}

	@Override
	public long getLatestCheckpointId() {
		return currentBarrier.getId();
	}

	@Override
	public long getAlignmentDurationNanos() {
		return 0;
	}

	@Override
	public void checkpointSizeLimitExceeded(long maxBufferedBytes) {
	}
}
