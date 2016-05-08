using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message for removing a Publication or Subscription.
    /// 
    /// <para>
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                            Client ID                          |
    /// +---------------------------------------------------------------+
    /// |                    Command Correlation ID                     |
    /// +---------------------------------------------------------------+
    /// |                         Registration ID                       |
    /// +---------------------------------------------------------------+
    /// </para>
    /// </summary>
    public class RemoveMessageFlyweight : CorrelatedMessageFlyweight
    {
        private static readonly int REGISTRATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// Get the registration id field
        /// </summary>
        /// <returns> registration id field </returns>
        public virtual long RegistrationId()
        {
            return buffer.GetLong(offset + REGISTRATION_ID_OFFSET);
        }

        /// <summary>
        /// Set registration  id field
        /// </summary>
        /// <param name="registrationId"> field value </param>
        /// <returns> flyweight </returns>
        public virtual RemoveMessageFlyweight RegistrationId(long registrationId)
        {
            buffer.PutLong(offset + REGISTRATION_ID_OFFSET, registrationId);

            return this;
        }

        public static int Length()
        {
            return LENGTH + BitUtil.SIZE_OF_LONG;
        }
    }
}