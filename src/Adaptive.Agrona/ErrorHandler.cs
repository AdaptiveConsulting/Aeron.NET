using System;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Callback to notify of an error that has occurred when processing an operation or event.
    /// <param name="exception"> exception that occurred while processing an operation or event.</param>
    /// </summary>
    public delegate void ErrorHandler(Exception exception);
}