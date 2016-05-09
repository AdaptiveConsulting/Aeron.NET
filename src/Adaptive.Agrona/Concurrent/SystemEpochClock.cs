using System;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona.Concurrent
{
    public class SystemEpochClock : IEpochClock
    {
        public long Time()
        {
            return UnixTimeConverter.CurrentUnixTimeMillis();
        }
    }
}