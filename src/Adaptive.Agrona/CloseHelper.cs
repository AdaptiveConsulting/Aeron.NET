using System;

namespace Adaptive.Agrona
{
    public class CloseHelper
    {
        // Note Olivier: this is not really relevant in .NET since .Dispose should never throw but we want to keep the implementation close to the Java codebase

        /// <summary>
        /// Quietly close a <seealso cref="IDisposable"/> dealing with nulls and exceptions.
        /// </summary>
        /// <param name="disposable"> to be disposed. </param>
        public static void QuietDispose(IDisposable disposable)
        {
            try
            {
                disposable?.Dispose();
            }
            catch
            {
            }
        }
    }
}