using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.Tests;

public class Tests
{
    public static CountersManager NewCountersManager(int dataLength)
    {
        return new CountersManager(
            new UnsafeBuffer(BufferUtil.AllocateDirect(countersMetadataBufferLength(dataLength))),
            new UnsafeBuffer(BufferUtil.AllocateDirect(dataLength)));
    }

    public static int countersMetadataBufferLength(int counterValuesBufferLength)
    {
        return counterValuesBufferLength * (CountersReader.METADATA_LENGTH / CountersReader.COUNTER_LENGTH);
    }
}