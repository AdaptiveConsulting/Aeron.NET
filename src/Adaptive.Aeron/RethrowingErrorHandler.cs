using System;
using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Error handler that will rethrow an <see cref="Exception"/>.
    /// </summary>
    public class RethrowingErrorHandler
    {
        /// <summary>
        /// Singleton instance to avoid allocation.
        /// </summary>
        public static readonly ErrorHandler INSTANCE = exception => throw exception;
    }
}