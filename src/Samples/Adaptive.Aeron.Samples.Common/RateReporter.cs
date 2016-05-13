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

        private readonly int _reportIntervalMs;
        private readonly Reporter _reportingFunc;

        private volatile bool _halt;
        private long _totalBytes;
        private long _totalMessages;
        private long _lastTotalBytes;
        private readonly Stopwatch _stopwatch;
        private long _lastTotalMessages;


        /// <summary>
        /// Create a rate reporter with the given report interval in nanoseconds and the reporting function.
        /// </summary>
        /// <param name="reportInterval"> in nanoseconds </param>
        /// <param name="reportingFunc"> to call for reporting rates </param>
        public RateReporter(int reportInterval, Reporter reportingFunc)
        {
            _reportIntervalMs = reportInterval;
            _reportingFunc = reportingFunc;
            _stopwatch = Stopwatch.StartNew();
        }

        /// <summary>
        /// Run loop for the rate reporter
        /// </summary>
        public void Run()
        {
            do
            {
                Thread.Sleep(_reportIntervalMs); // == Park?

                var currentTotalMessages = _totalMessages;
                var currentTotalBytes = _totalBytes;
                var timespanMs = _stopwatch.ElapsedMilliseconds;
                var messagesPerSec = (currentTotalMessages - _lastTotalMessages)*_reportIntervalMs/(double) timespanMs;
                var bytesPerSec = (currentTotalBytes - _lastTotalBytes)*_reportIntervalMs/(double) timespanMs;

                _reportingFunc(messagesPerSec, bytesPerSec, currentTotalMessages, currentTotalBytes);

                _lastTotalBytes = currentTotalBytes;
                _lastTotalMessages = currentTotalMessages;
                _stopwatch.Restart();
            } while (!_halt);
        }

        /// <summary>
        /// Signal the run loop to exit. Does not block.
        /// </summary>
        public void Halt()
        {
            _halt = true;
        }

        /// <summary>
        /// Tell rate reporter of number of messages and bytes received, sent, etc.
        /// </summary>
        /// <param name="messages"> received, sent, etc. </param>
        /// <param name="bytes"> received, sent, etc. </param>
        public void OnMessage(long messages, long bytes)
        {
            _totalBytes += bytes;
            _totalMessages += messages;
        }
    }
}