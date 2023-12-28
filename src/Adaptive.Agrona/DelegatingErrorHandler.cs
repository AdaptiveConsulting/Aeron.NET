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
    public interface DelegatingErrorHandler : IErrorHandler
    {
        /// <summary>
        /// Set the next <seealso cref="ErrorHandler"/> to be called in a chain.
        /// </summary>
        /// <param name="errorHandler"> the next <seealso cref="ErrorHandler"/> to be called in a chain. </param>
        void Next(IErrorHandler errorHandler);
    }
}