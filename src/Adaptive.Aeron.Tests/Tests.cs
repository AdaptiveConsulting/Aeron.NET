/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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

using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.Tests;

public static class Tests
{
    public static CountersManager NewCountersManager(int dataLength)
    {
        return new CountersManager(
            new UnsafeBuffer(BufferUtil.AllocateDirect(CountersMetadataBufferLength(dataLength))),
            new UnsafeBuffer(BufferUtil.AllocateDirect(dataLength))
        );
    }

    public static int CountersMetadataBufferLength(int counterValuesBufferLength)
    {
        return counterValuesBufferLength * (CountersReader.METADATA_LENGTH / CountersReader.COUNTER_LENGTH);
    }
}
