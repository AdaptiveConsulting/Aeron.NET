using System;

namespace Adaptive.Agrona
{
    /// <summary>
    /// <seealso cref="ErrorHandler"/> that can insert into a chain of responsibility so it handles an error and then delegates
    /// on to the next in the chain. This allows for taking action pre or post invocation of the next delegate.
    /// <para>
    /// Implementations are responsible for calling the next in the chain.
    /// </para>
    /// </summary>
    public interface DelegatingErrorHandler
    {
        /// <summary>
        /// Set the next <seealso cref="ErrorHandler"/> to be called in a chain.
        /// </summary>
        /// <param name="errorHandler"> the next <seealso cref="ErrorHandler"/> to be called in a chain. </param>
        void Next(ErrorHandler errorHandler);

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