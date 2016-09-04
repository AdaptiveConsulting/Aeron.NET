using System;
using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    public interface IThreadFactory
    {
        Thread NewThread(ThreadStart runner);
    }
}