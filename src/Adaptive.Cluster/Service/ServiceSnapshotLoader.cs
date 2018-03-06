using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal class ServiceSnapshotLoader : IControlledFragmentHandler
    {
        private const int FRAGMENT_LIMIT = 10;

        private bool inSnapshot = false;
        private bool isDone = false;
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SnapshotMarkerDecoder snapshotMarkerDecoder = new SnapshotMarkerDecoder();
        private readonly ClientSessionDecoder clientSessionDecoder = new ClientSessionDecoder();
        private readonly Image image;
        private readonly ClusteredServiceAgent agent;

        internal ServiceSnapshotLoader(Image image, ClusteredServiceAgent agent)
        {
            this.image = image;
            this.agent = agent;
        }

        public bool IsDone()
        {
            return isDone;
        }

        public int Poll()
        {
            return image.ControlledPoll(this, FRAGMENT_LIMIT);
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            int templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case SnapshotMarkerDecoder.TEMPLATE_ID:
                    snapshotMarkerDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    long typeId = snapshotMarkerDecoder.TypeId();
                    if (typeId != ClusteredServiceContainer.SNAPSHOT_TYPE_ID)
                    {
                        throw new InvalidOperationException("Unexpected snapshot type: " + typeId);
                    }

                    switch (snapshotMarkerDecoder.Mark())
                    {
                        case SnapshotMark.BEGIN:
                            if (inSnapshot)
                            {
                                throw new InvalidOperationException("Already in snapshot");
                            }

                            inSnapshot = true;
                            return ControlledFragmentHandlerAction.CONTINUE;

                        case SnapshotMark.END:
                            if (!inSnapshot)
                            {
                                throw new InvalidOperationException("Missing begin snapshot");
                            }

                            isDone = true;
                            return ControlledFragmentHandlerAction.BREAK;
                    }

                    break;

                case ClientSessionDecoder.TEMPLATE_ID:
                    clientSessionDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    string responseChannel = clientSessionDecoder.ResponseChannel();
                    byte[] principalData = new byte[clientSessionDecoder.PrincipalDataLength()];
                    clientSessionDecoder.GetPrincipalData(principalData, 0, principalData.Length);

                    agent.AddSession(clientSessionDecoder.ClusterSessionId(), clientSessionDecoder.ResponseStreamId(), responseChannel, principalData);
                    break;

                default:
                    throw new InvalidOperationException("Unknown template id: " + templateId);
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}