using System;

namespace Adaptive.Agrona
{
    public interface IErrorHandler
    {
        /// <summary>
        /// Callback to notify of an error that has occurred when processing an operation or event.
        /// 
        /// This method is assumed non-throwing, so rethrowing the exception or triggering further exceptions would be a bug.
        /// 
        /// <param name="exception"> exception that occurred while processing an operation or event.</param>
        /// </summary>
        void OnError(Exception exception);
    }
}