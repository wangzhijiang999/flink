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
 * The {@link BufferPersister} spills the in-flight input & output buffers for unaligned checkpoint.
 */
@Internal
public interface BufferPersister extends AutoCloseable {

	/**
	 * The buffer from the given channel index is added for spilling.
	 */
	void addBuffer(Buffer buffer, int index);

	void addBuffers(Collection<Buffer> buffers, int index);

	/**
	 * All the inflighting buffers are already added via {@link #addBuffer(Buffer, int)}.
	 */
	void finish();

	/**
	 * Gets the future to judge whether all the added buffers finish spilling internally or not.
	 */
	CompletableFuture<?> getCompleteFuture();
}
