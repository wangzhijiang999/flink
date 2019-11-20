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
import org.apache.flink.core.fs.FileSystem;
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
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link OutputPersisterImpl} takes the buffers and events from a data stream and persists them
 * asynchronously using {@link RecoverableFsDataOutputStream}.
 */
@Internal
public class OutputPersisterImpl implements OutputPersister {

	private final Writer writer;

	public OutputPersisterImpl(Path path) throws IOException {
		writer = new Writer(FileSystem.get(path.toUri()).createRecoverableWriter(), path);
		writer.start();
	}

	@Override
	public void add(Collection<BufferConsumer> bufferConsumers, int channelId) {
		writer.add(bufferConsumers);
	}

	@Override
	public CompletableFuture<?> finish() throws IOException {
		return writer.finish();
	}

	@Override
	public void close() throws IOException, InterruptedException {
		writer.close();
	}

	private static class Writer extends Thread implements AutoCloseable {
		private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

		private static final BufferConsumer FINISH_MARKER = new BufferConsumer(
			MemorySegmentFactory.allocateUnpooledSegment(42),
			memorySegment -> {},
			false);

		private final Queue<BufferConsumer> handover = new ArrayDeque<>();
		private final RecoverableWriter recoverableWriter;
		private final Path recoverableWriterBasePath;

		@Nullable
		private Throwable asyncException;
		private int partId;
		private CompletableFuture<?> finishFuture = CompletableFuture.completedFuture(null);
		private RecoverableFsDataOutputStream currentOutputStream;

		private volatile boolean running = true;

		public Writer(RecoverableWriter recoverableWriter, Path recoverableWriterBasePath) throws IOException {
			this.recoverableWriter = recoverableWriter;
			this.recoverableWriterBasePath = recoverableWriterBasePath;

			openNewOutputStream();
		}

		public synchronized void add(BufferConsumer bufferConsumer) {
			checkErroneousUnsafe();

			boolean wasEmpty = handover.isEmpty();
			handover.add(bufferConsumer);
			if (wasEmpty) {
				notify();
			}
		}

		public synchronized void add(Collection<BufferConsumer> bufferConsumers) {
			checkErroneousUnsafe();

			boolean wasEmpty = handover.isEmpty();
			handover.addAll(bufferConsumers);
			if (wasEmpty) {
				notify();
			}
		}

		public synchronized CompletableFuture<?> finish() {
			checkErroneousUnsafe();
			checkState(finishFuture.isDone(), "TODO support multiple pending persist requests (multiple ongoing checkpoints?)");

			if (finishFuture.isDone()) {
				finishFuture = new CompletableFuture<>();
			}
			add(FINISH_MARKER);

			return finishFuture;
		}

		public synchronized void checkErroneous() {
			checkErroneousUnsafe();
		}

		@Override
		public void run() {
			try {
				while (running) {
					write(get());
				}
			}
			catch (Throwable t) {
				synchronized (this) {
					if (running) {
						asyncException = t;
					}
					if (!finishFuture.isDone()) {
						finishFuture.completeExceptionally(t);
					}
				}
				LOG.error("unhandled exception in the writer", t);
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
			while (true) {
				BufferConsumer bufferConsumer = handover.poll();
				if (bufferConsumer == null) {
					wait();
					continue;
				}

				if (bufferConsumer == FINISH_MARKER) {
					currentOutputStream.closeForCommit().commit();
					finishFuture.complete(null);
					assert handover.isEmpty();
					continue;
				}

				Buffer buffer = bufferConsumer.build();
				if (bufferConsumer.isFinished()) {
					bufferConsumer.close();
				}
				if (buffer.readableBytes() > 0) {
					return buffer;
				}
			}
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
