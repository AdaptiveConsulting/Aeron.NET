using System;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Callback interface for handling an error/exception that has occurred when processing an operation or event.
    /// </summary>
    public interface IErrorHandler
    {
        /// <summary>
        /// Callback to notify of an error that has occurred when processing an operation or event.
        /// <param name="exception"> exception that occurred while processing an operation or event.</param>
        /// </summary>
        void OnError(Exception exception);
    }
}