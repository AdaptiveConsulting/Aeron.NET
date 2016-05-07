namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// List of event types used in the control protocol between the
    /// media driver and the core.
    /// </summary>
    public class ControlProtocolEvents
    {
        // Clients to Media Driver

        /// <summary>
        /// Add Publication </summary>
        public const int ADD_PUBLICATION = 0x01;
        /// <summary>
        /// Remove Publication </summary>
        public const int REMOVE_PUBLICATION = 0x02;
        /// <summary>
        /// Add Subscriber </summary>
        public const int ADD_SUBSCRIPTION = 0x04;
        /// <summary>
        /// Remove Subscriber </summary>
        public const int REMOVE_SUBSCRIPTION = 0x05;
        /// <summary>
        /// Keepalive from Client </summary>
        public const int CLIENT_KEEPALIVE = 0x06;

        // Media Driver to Clients

        /// <summary>
        /// Error Response </summary>
        public const int ON_ERROR = 0x0F01;
        /// <summary>
        /// New subscription Buffer Notification </summary>
        public const int ON_AVAILABLE_IMAGE = 0x0F02;
        /// <summary>
        /// New publication Buffer Notification </summary>
        public const int ON_PUBLICATION_READY = 0x0F03;
        /// <summary>
        /// Operation Succeeded </summary>
        public const int ON_OPERATION_SUCCESS = 0x0F04;
        /// <summary>
        /// Inform client of timeout and removal of inactive image </summary>
        public const int ON_UNAVAILABLE_IMAGE = 0x0F05;
    }

}