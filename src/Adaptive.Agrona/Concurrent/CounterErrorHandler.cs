using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Agrona.Concurrent
{
    using System;

    /// <summary>
    /// An <seealso cref="ErrorHandler"/> which calls <seealso cref="AtomicCounter.Increment()"/> before delegating the exception.
    /// </summary>
    public class CountedErrorHandler : IErrorHandler
    {
        private readonly ErrorHandler _errorHandler;
        private readonly AtomicCounter _errorCounter;

        public readonly ErrorHandler AsErrorHandler;

        /// <summary>
        /// Construct a counted error handler with a delegate and counter.
        /// </summary>
        /// <param name="errorHandler"> to delegate to. </param>
        /// <param name="errorCounter"> to increment before delegation. </param>
        public CountedErrorHandler(ErrorHandler errorHandler, AtomicCounter errorCounter)
        {
            Objects.RequireNonNull(errorHandler, "errorHandler");
            Objects.RequireNonNull(errorCounter, "errorCounter");

            _errorHandler = errorHandler;
            _errorCounter = errorCounter;

            AsErrorHandler = OnError;
        }

        public void OnError(Exception throwable)
        {
            _errorCounter.Increment();
            _errorHandler(throwable);
        }
    }
}