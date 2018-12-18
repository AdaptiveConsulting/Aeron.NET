using System;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Delegeate for delivery of End of Stream image notification to a <see cref="Subscription"/>
    /// </summary>
    /// <param name="image"> that has reached End Of Stream.</param>
    [Obsolete]
    public delegate void EndOfStreamHandler(Image image);
}