using Adaptive.Agrona.Concurrent.Errors;

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// A means to capture an event of significance that does not require a stack trace, so it can be lighter-weight
    /// and take up less space in a <seealso cref="DistinctErrorLog"/>.
    /// </summary>
    public class AeronEvent : AeronException
    {
        public AeronEvent(string message) : base(message)
        {
        }

        public AeronEvent(string message, Category category) : base(message, category)
        {
        }
    }
}