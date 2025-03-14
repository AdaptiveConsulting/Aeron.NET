namespace Adaptive.Archiver
{
    /// <summary>
    /// Fluent API for setting optional replay parameters. Allows the user to configure starting position,
    /// replay length, bounding counter (for a bounded replay) and the max length for file I/O operations.
    ///
    /// Not threadsafe. 
    /// </summary>
    public class ReplayParams
    {
        private int boundingLimitCounterId;
        private int fileIoMaxLength;
        private long position;
        private long length;
        private long replayToken;
        private long subscriptionRegistrationId;

        /// <summary>
        /// Default, initialise all values to "null".
        /// </summary>
        public ReplayParams()
        {
            Reset();
        }

        /// <summary>
        /// reset all value to "null", allows for an instance to be reused
        /// </summary>
        /// <returns> this for a fluent API </returns>
        public ReplayParams Reset()
        {
            boundingLimitCounterId = Aeron.Aeron.NULL_VALUE;
            fileIoMaxLength = Aeron.Aeron.NULL_VALUE;
            position = AeronArchive.NULL_POSITION;
            length = AeronArchive.NULL_LENGTH;
            replayToken = Aeron.Aeron.NULL_VALUE;
            subscriptionRegistrationId = Aeron.Aeron.NULL_VALUE;
            return this;
        }

        /// <summary>
        /// Set the position to start the replay. If set to <seealso cref="AeronArchive.NULL_POSITION"/> (which is the default) then
        /// the stream will be replayed from the start.
        /// </summary>
        /// <param name="position"> to start the replay from. </param>
        /// <returns> this for a fluent API. </returns>
        public ReplayParams Position(long position)
        {
            this.position = position;
            return this;
        }

        /// <summary>
        /// Position to start the replay at.
        /// </summary>
        /// <returns> position for the start of the replay. </returns>
        /// <seealso cref="ReplayParams.Position(long)"/>
        public long Position()
        {
            return position;
        }

        /// <summary>
        /// The length of the recorded stream to replay. If set to <seealso cref="AeronArchive.NULL_POSITION"/> (the default) will
        /// replay a whole stream of unknown length. If set to <seealso cref="long.MaxValue"/> it will follow a live recording.
        /// </summary>
        /// <param name="length"> of the recording to be replayed. </param>
        /// <returns> this for a fluent API. </returns>
        public ReplayParams Length(long length)
        {
            this.length = length;
            return this;
        }

        /// <summary>
        /// Length of the recording to replay.
        /// </summary>
        /// <returns> length of the recording to replay. </returns>
        /// <seealso cref="ReplayParams.Length(long)"/>
        public long Length()
        {
            return length;
        }

        /// <summary>
        /// Sets the counter id to be used for bounding the replay. Setting this value will trigger the sending of a
        /// bounded replay request instead of a normal replay.
        /// </summary>
        /// <param name="boundingLimitCounterId"> counter to use to bound the replay </param>
        /// <returns> this for a fluent API </returns>
        public ReplayParams BoundingLimitCounterId(int boundingLimitCounterId)
        {
            this.boundingLimitCounterId = boundingLimitCounterId;
            return this;
        }

        /// <summary>
        /// Gets the counterId specified for the bounding the replay. Returns <seealso cref="Aeron.Aeron.NULL_VALUE"/> if unspecified.
        /// </summary>
        /// <returns> the counter id to bound the replay. </returns>
        public int BoundingLimitCounterId()
        {
            return this.boundingLimitCounterId;
        }

        /// <summary>
        /// The maximum size of a file operation when reading from the archive to execute the replay. Will use the value
        /// defined in the context otherwise. This can be used reduce the size of file IO operations to lower the
        /// priority of some replays. Setting it to a value larger than the context value will have no affect.
        /// </summary>
        /// <param name="fileIoMaxLength"> maximum length of a replay file operation </param>
        /// <returns> this for a fluent API </returns>
        public ReplayParams FileIoMaxLength(int fileIoMaxLength)
        {
            this.fileIoMaxLength = fileIoMaxLength;
            return this;
        }

        /// <summary>
        /// Gets the maximum length for file IO operations in the replay. Defaults to <seealso cref="Aeron.Aeron.NULL_VALUE"/> if not
        /// set, which will trigger the use of the Archive.Context default.
        /// </summary>
        /// <returns> maximum file length for IO operations during replay. </returns>
        public int FileIoMaxLength()
        {
            return this.fileIoMaxLength;
        }

        /// <summary>
        /// Determines if the parameter setup has requested a bounded replay.
        /// </summary>
        /// <returns> true if the replay should be bounded, false otherwise. </returns>
        public bool IsBounded()
        {
            return Aeron.Aeron.NULL_VALUE != boundingLimitCounterId;
        }
        
        /// <summary>
        /// Set a token used for replays when the initiating image is not the one used to create the archive
        /// connection/session.
        /// </summary>
        /// <param name="replayToken"> token to identify the replay </param>
        /// <returns> this for a fluent API. </returns>
        public ReplayParams ReplayToken(long replayToken)
        {
            this.replayToken = replayToken;
            return this;
        }

        /// <summary>
        /// Get a token used for replays when the initiating image is not the one used to create the archive
        /// connection/session.
        /// </summary>
        /// <returns> the replay token </returns>
        public long ReplayToken()
        {
            return replayToken;
        }

        /// <summary>
        /// Set the subscription registration id to be used when doing a start replay using response channels and the
        /// response subscription is already created.
        /// </summary>
        /// <param name="registrationId"> of the subscription to receive the replay (should be set up with control-mode=response). </param>
        public void SubscriptionRegistrationId(long registrationId)
        {
            this.subscriptionRegistrationId = registrationId;
        }

        /// <summary>
        /// Get the subscription registration id to be used when doing a start replay using response channels and the
        /// response subscription is already created.
        /// </summary>
        /// <returns> registrationId of the subscription to receive the replay (should be set up with control-mode=response). </returns>
        public long SubscriptionRegistrationId()
        {
            return subscriptionRegistrationId;
        }
    }
}