using System.Diagnostics;
using System.Threading;

namespace Adaptive.Aeron.Samples.Common
{
    /// <summary>
    /// Tracker and reporter of rates.
    /// 
    /// Uses volatile semantics for counters.
    /// </summary>
    public class RateReporter
    {
        /// <summary>
        /// Interface for reporting of rate information
        /// </summary>
        public delegate void Reporter(double messagesPerSec, double bytesPerSec, long totalMessages, long totalBytes);

        private readonly int ReportIntervalMS;
        private readonly Reporter ReportingFunc;

        private volatile bool Halt_Renamed = false;
        private long TotalBytes;
        private long TotalMessages;
        private long LastTotalBytes;
        private readonly Stopwatch _stopwatch;
        private long LastTotalMessages;


        /// <summary>
        /// Create a rate reporter with the given report interval in nanoseconds and the reporting function.
        /// </summary>
        /// <param name="reportInterval"> in nanoseconds </param>
        /// <param name="reportingFunc"> to call for reporting rates </param>
        public RateReporter(int reportInterval, Reporter reportingFunc)
        {
            ReportIntervalMS = reportInterval;
            ReportingFunc = reportingFunc;
            _stopwatch = Stopwatch.StartNew();
        }

        /// <summary>
        /// Run loop for the rate reporter
        /// </summary>
        public virtual void Run()
        {
            do
            {
                Thread.Sleep(ReportIntervalMS); // == Park?

                var currentTotalMessages = TotalMessages;
                var currentTotalBytes = TotalBytes;
                var timeSpanNs = _stopwatch.ElapsedMilliseconds;
                var messagesPerSec = (currentTotalMessages - LastTotalMessages)*ReportIntervalMS/(double) timeSpanNs;
                var bytesPerSec = (currentTotalBytes - LastTotalBytes)*ReportIntervalMS/(double) timeSpanNs;

                ReportingFunc(messagesPerSec, bytesPerSec, currentTotalMessages, currentTotalBytes);

                LastTotalBytes = currentTotalBytes;
                LastTotalMessages = currentTotalMessages;
            } while (!Halt_Renamed);
        }

        /// <summary>
        /// Signal the run loop to exit. Does not block.
        /// </summary>
        public virtual void Halt()
        {
            Halt_Renamed = true;
        }

        /// <summary>
        /// Tell rate reporter of number of messages and bytes received, sent, etc.
        /// </summary>
        /// <param name="messages"> received, sent, etc. </param>
        /// <param name="bytes"> received, sent, etc. </param>
        public virtual void OnMessage(long messages, long bytes)
        {
            TotalBytes += bytes;
            TotalMessages += messages;
        }
    }
}