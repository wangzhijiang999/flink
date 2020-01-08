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

package org.apache.flink.runtime.io.network;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * The dummy implementation of {@link BufferPersister} used for tests or the case of without input/output.
 */
@Internal
public class NoOpBufferPersisterImpl implements BufferPersister {
	private static final CompletableFuture<?> completedFuture = CompletableFuture.completedFuture(null);

	private static final String ERROR_MSG = "This method should never be called";

	public NoOpBufferPersisterImpl() {
	}

	@Override
	public void addBuffer(Buffer buffer, int channelIndex) {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public void addBuffers(Collection<Buffer> buffers, int channelIndex) {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public void finish() {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public CompletableFuture<?> getCompleteFuture() {
		return completedFuture;
	}

	@Override
	public void close() {
	}
}
