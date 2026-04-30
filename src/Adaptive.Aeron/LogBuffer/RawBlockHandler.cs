/*
 * Copyright 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.IO;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Handler for a raw block of fragments from the log that are contained in the underlying file.
    /// </summary>
    /// <param name="fileStream"> read-only stream over the log file containing the block. </param>
    /// <param name="fileOffset"> at which the block begins, including any frame headers. </param>
    /// <param name="termBuffer"> mapped over the block of fragments. </param>
    /// <param name="termOffset"> in <paramref name="termBuffer"/> at which the block begins, including any frame headers. </param>
    /// <param name="length"> of the block in bytes, including any frame headers, aligned up to <see cref="FrameDescriptor.FRAME_ALIGNMENT"/>. </param>
    /// <param name="sessionId"> of the stream of fragments. </param>
    /// <param name="termId"> of the stream of fragments. </param>
    public delegate void RawBlockHandler(
        FileStream fileStream,
        long fileOffset,
        UnsafeBuffer termBuffer,
        int termOffset,
        int length,
        int sessionId,
        int termId);
}
