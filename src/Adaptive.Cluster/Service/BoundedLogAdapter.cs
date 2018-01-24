using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Status;
using Adaptive.Agrona.Concurrent;
using Io.Aeron.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Adapter for reading a log with a limit applied beyond which the consumer cannot progress.
    /// </summary>
    internal sealed class BoundedLogAdapter : IControlledFragmentHandler
    {
        private bool InstanceFieldsInitialized = false;

        private void InitializeInstanceFields()
        {
            fragmentAssembler = new ImageControlledFragmentAssembler(this, INITIAL_BUFFER_LENGTH, true);
        }

        private const int FRAGMENT_LIMIT = 10;
        private const int INITIAL_BUFFER_LENGTH = 4096;

        private ImageControlledFragmentAssembler fragmentAssembler;
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionOpenEventDecoder openEventDecoder = new SessionOpenEventDecoder();
        private readonly SessionCloseEventDecoder closeEventDecoder = new SessionCloseEventDecoder();
        private readonly SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
        private readonly TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
        private readonly ServiceActionRequestDecoder actionRequestDecoder = new ServiceActionRequestDecoder();

        private readonly Image image;
        private readonly ReadableCounter limit;
        private readonly ClusteredServiceAgent agent;

        internal BoundedLogAdapter(Image image, ReadableCounter limit, ClusteredServiceAgent agent)
        {
            if (!InstanceFieldsInitialized)
            {
                InitializeInstanceFields();
                InstanceFieldsInitialized = true;
            }

            this.image = image;
            this.limit = limit;
            this.agent = agent;
        }

        public Image Image()
        {
            return image;
        }

        public int Poll()
        {
            return image.BoundedControlledPoll(fragmentAssembler, limit.Get(), FRAGMENT_LIMIT);
        }

        public ControlledFragmentHandlerAction OnFragment(UnsafeBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            switch (messageHeaderDecoder.TemplateId())
            {
                case SessionHeaderDecoder.TEMPLATE_ID:
                {
                    sessionHeaderDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    agent.OnSessionMessage(sessionHeaderDecoder.ClusterSessionId(), sessionHeaderDecoder.CorrelationId(), sessionHeaderDecoder.Timestamp(), buffer, offset + ClientSession.SESSION_HEADER_LENGTH, length - ClientSession.SESSION_HEADER_LENGTH, header);

                    break;
                }

                case TimerEventDecoder.TEMPLATE_ID:
                {
                    timerEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    agent.OnTimerEvent(timerEventDecoder.CorrelationId(), timerEventDecoder.Timestamp());
                    break;
                }

                case SessionOpenEventDecoder.TEMPLATE_ID:
                {
                    openEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    string responseChannel = openEventDecoder.ResponseChannel();
                    byte[] principalData = new byte[openEventDecoder.PrincipalDataLength()];
                    openEventDecoder.GetPrincipalData(principalData, 0, principalData.Length);

                    agent.OnSessionOpen(openEventDecoder.ClusterSessionId(), openEventDecoder.Timestamp(), openEventDecoder.ResponseStreamId(), responseChannel, principalData);
                    break;
                }

                case SessionCloseEventDecoder.TEMPLATE_ID:
                {
                    closeEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    agent.OnSessionClose(closeEventDecoder.ClusterSessionId(), closeEventDecoder.Timestamp(), closeEventDecoder.CloseReason());
                    break;
                }

                case ServiceActionRequestDecoder.TEMPLATE_ID:
                {
                    actionRequestDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    agent.OnServiceAction(header.Position(), actionRequestDecoder.Timestamp(), actionRequestDecoder.Action());
                    break;
                }
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}