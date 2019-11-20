/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link InputPersisterImpl} takes the buffers and events from a data stream and persists them
 * asynchronously using {@link RecoverableFsDataOutputStream}.
 */
@Internal
public class InputPersisterImpl implements InputPersister {
	private final Writer writer;

	public InputPersisterImpl(
			RecoverableWriter recoverableWriter,
			Path recoverableWriterBasePath,
			int numberOfChannels) {
		writer = new Writer(recoverableWriter, recoverableWriterBasePath, numberOfChannels);
		writer.start();
	}

	@Override
	public void addBuffer(Buffer buffer, int channelIndex) {
		writer.add(buffer.retainBuffer());
	}

	public CompletableFuture<?> persist() throws IOException {
		return writer.persist();
	}

	@Override
	public void close() throws IOException, InterruptedException {
		writer.close();
	}

	private static class Writer extends Thread implements AutoCloseable {
		private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
		private static final Buffer FINISH_MARKER = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(42), memorySegment -> {});

		private volatile boolean running = true;

		private final Queue<Buffer> handover = new ArrayDeque<>();
		private final RecoverableWriter recoverableWriter;
		private final Path recoverableWriterBasePath;

		private final int numberOfChannels;
		private int numberOfFinishedChannels;

		@Nullable
		private Throwable asyncException;
		private int partId;
		private CompletableFuture<?> persistFuture = CompletableFuture.completedFuture(null);
		private RecoverableFsDataOutputStream currentOutputStream;

		public Writer(RecoverableWriter recoverableWriter, Path recoverableWriterBasePath, int numberOfChannels) {
			this.recoverableWriter = recoverableWriter;
			this.recoverableWriterBasePath = recoverableWriterBasePath;
			this.numberOfChannels = numberOfChannels;
		}

		public synchronized void add(Buffer buffer) {
			checkErroneousUnsafe();

			boolean wasEmpty = handover.isEmpty();
			handover.add(buffer);
			if (wasEmpty) {
				notify();
			}
		}

		public synchronized CompletableFuture<?> persist() throws IOException {
			checkErroneousUnsafe();
			checkState(persistFuture.isDone(), "TODO support multiple pending persist requests (multiple ongoing checkpoints?)");
			if (persistFuture.isDone()) {
				persistFuture = new CompletableFuture<>();
			}
			add(FINISH_MARKER);

			return persistFuture;
		}

		public synchronized void checkErroneous() {
			checkErroneousUnsafe();
		}

		@Override
		public void run() {
			try {
				openNewOutputStream();

				while (running) {
					write(get());
				}
			}
			catch (Throwable t) {
				synchronized (this) {
					if (running) {
						asyncException = t;
					}
					if (!persistFuture.isDone()) {
						persistFuture.completeExceptionally(t);
					}
				}
				LOG.error("unhandled exception in the Writer", t);
			}
		}

		private void write(Buffer buffer) throws IOException {
			try {
				int offset = buffer.getMemorySegmentOffset();
				MemorySegment segment = buffer.getMemorySegment();
				int numBytes = buffer.getSize();

				currentOutputStream.write(segment.getArray(), offset, numBytes);
			}
			finally {
				buffer.recycleBuffer();
			}
		}

		private synchronized Buffer get() throws InterruptedException, IOException {
			while (handover.isEmpty()) {
				wait();
			}

			Buffer buffer = handover.poll();
			if (buffer == FINISH_MARKER) {
				if (++numberOfFinishedChannels == numberOfChannels) {
					currentOutputStream.closeForCommit().commit();
					openNewOutputStream();
					persistFuture.complete(null);
					assert handover.isEmpty();
				}
				return get();
			}

			return buffer;
		}

		@Override
		public void close() throws InterruptedException, IOException {
			try {
				checkErroneous();
				running = false;
				interrupt();
				join();
			}
			finally {
				currentOutputStream.close();
			}
		}

		private void openNewOutputStream() throws IOException {
			currentOutputStream = recoverableWriter.open(assemblePartFilePath());
		}

		private Path assemblePartFilePath() {
			return new Path(recoverableWriterBasePath, "part-file." + partId++);
		}

		private void checkErroneousUnsafe() {
			if (asyncException != null) {
				throw new RuntimeException(asyncException);
			}
		}
	}
}
