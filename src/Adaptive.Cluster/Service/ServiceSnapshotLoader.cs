using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal class ServiceSnapshotLoader : IControlledFragmentHandler
    {
        private const int FRAGMENT_LIMIT = 10;

        private bool inSnapshot = false;
        private bool isDone = false;
        private int appVersion;
        private ClusterTimeUnit timeUnit;

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SnapshotMarkerDecoder snapshotMarkerDecoder = new SnapshotMarkerDecoder();
        private readonly ClientSessionDecoder clientSessionDecoder = new ClientSessionDecoder();
        private readonly ImageControlledFragmentAssembler fragmentAssembler;
        private readonly Image image;
        private readonly ClusteredServiceAgent agent;

        internal ServiceSnapshotLoader(Image image, ClusteredServiceAgent agent)
        {
            this.fragmentAssembler = new ImageControlledFragmentAssembler(this);
            this.image = image;
            this.agent = agent;
        }

        internal bool IsDone()
        {
            return isDone;
        }

        internal int AppVersion()
        {
            return appVersion;
        }

        internal ClusterTimeUnit TimeUnit()
        {
            return timeUnit;
        }

        internal int Poll()
        {
            return image.ControlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" +
                                           schemaId);
            }

            int templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case SnapshotMarkerDecoder.TEMPLATE_ID:
                    snapshotMarkerDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    long typeId = snapshotMarkerDecoder.TypeId();
                    if (typeId != ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID)
                    {
                        throw new ClusterException("unexpected snapshot type: " + typeId);
                    }

                    switch (snapshotMarkerDecoder.Mark())
                    {
                        case SnapshotMark.BEGIN:
                            if (inSnapshot)
                            {
                                throw new ClusterException("already in snapshot");
                            }

                            inSnapshot = true;
                            appVersion = snapshotMarkerDecoder.AppVersion();
                            timeUnit = snapshotMarkerDecoder.TimeUnit() == ClusterTimeUnit.NULL_VALUE
                                ? ClusterTimeUnit.MILLIS
                                : snapshotMarkerDecoder.TimeUnit();
                            
                            return ControlledFragmentHandlerAction.CONTINUE;

                        case SnapshotMark.END:
                            if (!inSnapshot)
                            {
                                throw new ClusterException("missing begin snapshot");
                            }

                            isDone = true;
                            return ControlledFragmentHandlerAction.BREAK;
                    }

                    break;

                case ClientSessionDecoder.TEMPLATE_ID:
                    clientSessionDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    string responseChannel = clientSessionDecoder.ResponseChannel();
                    byte[] encodedPrincipal = new byte[clientSessionDecoder.EncodedPrincipalLength()];
                    clientSessionDecoder.GetEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.Length);

                    agent.AddSession(
                        clientSessionDecoder.ClusterSessionId(),
                        clientSessionDecoder.ResponseStreamId(),
                        responseChannel,
                        encodedPrincipal);
                    break;
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}