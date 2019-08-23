
namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Type of exception.
    /// </summary>
    public enum Category
    {
        /// <summary>
        /// Exception indicates a fatal condition. Recommendation is to terminate process immediately to avoid
        /// state corruption.
        /// </summary>
        FATAL,

        /// <summary>
        /// Exception is an error. Corrective action is recommended if understood, otherwise treat as fatal.
        /// </summary>
        ERROR,

        /// <summary>
        /// Exception is a warning. Action has been, or will be, taken to handle the condition.
        /// Additional corrective action by the application may be needed.
        /// </summary>
        WARN
    }
}