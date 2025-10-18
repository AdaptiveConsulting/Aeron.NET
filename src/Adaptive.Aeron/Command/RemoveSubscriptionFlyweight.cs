namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message for removing a Subscription.
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                          Client ID                            |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                    Command Correlation ID                     |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                       Registration ID                         |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </summary>
    public class RemoveSubscriptionFlyweight : RemoveMessageFlyweight
    {
    }
}