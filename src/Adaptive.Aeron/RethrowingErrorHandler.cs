using System;
using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Error handler that will rethrow an <see cref="Exception"/>.
    /// </summary>
    public class RethrowingErrorHandler : IErrorHandler
    {
        /// <summary>
        /// Singleton instance to avoid allocation.
        /// </summary>
        public static readonly IErrorHandler INSTANCE = new RethrowingErrorHandler();

        public void OnError(Exception exception)
        {
            throw exception;
        }
    }
}