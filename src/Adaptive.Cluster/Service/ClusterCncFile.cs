using System;
using System.Diagnostics;
using System.IO;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    public class ClusterCncFile : IDisposable
    {
        public const string FILENAME = "cnc.dat";
        public const int ALIGNMENT = 1024;

        private readonly CncHeaderDecoder cncHeaderDecoder;
        private readonly CncHeaderEncoder cncHeaderEncoder;
        private readonly CncFile cncFile;
        private readonly UnsafeBuffer cncBuffer;

        public ClusterCncFile(FileInfo file, ClusterComponentType type, int totalFileLength, IEpochClock epochClock, long timeoutMs)
        {
            cncFile = new CncFile(file, file.Exists, CncHeaderDecoder.VersionEncodingOffset(), CncHeaderDecoder.ActivityTimestampEncodingOffset(), totalFileLength, timeoutMs, epochClock, (version) =>
            {
                if (version != CncHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new ArgumentException("CnC file version " + version + " does not match software:" + CncHeaderDecoder.SCHEMA_VERSION);
                }
            }, null);

            cncBuffer = cncFile.Buffer();

            cncHeaderDecoder = new CncHeaderDecoder();
            cncHeaderEncoder = new CncHeaderEncoder();

            cncHeaderEncoder.Wrap(cncBuffer, 0);
            cncHeaderDecoder.Wrap(cncBuffer, 0, CncHeaderDecoder.BLOCK_LENGTH, CncHeaderDecoder.SCHEMA_VERSION);

            ClusterComponentType existingType = cncHeaderDecoder.FileType();

            if (existingType != ClusterComponentType.NULL && existingType != type)
            {
                throw new InvalidOperationException("existing CnC file type " + existingType + " not same as required type " + type);
            }

            cncHeaderEncoder.FileType(type);
            cncHeaderEncoder.Pid(Process.GetCurrentProcess().Id);
        }

        public ClusterCncFile(DirectoryInfo directory, string filename, IEpochClock epochClock, long timeoutMs, Action<string> logger)
        {
            cncFile = new CncFile(directory, filename, CncHeaderDecoder.VersionEncodingOffset(), CncHeaderDecoder.ActivityTimestampEncodingOffset(), timeoutMs, epochClock, (version) =>
            {
                if (version != CncHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new ArgumentException("CnC file version " + version + " does not match software:" + CncHeaderDecoder.SCHEMA_VERSION);
                }
            }, logger);

            cncBuffer = cncFile.Buffer();

            cncHeaderDecoder = new CncHeaderDecoder();
            cncHeaderEncoder = null;

            cncHeaderDecoder.Wrap(cncBuffer, 0, CncHeaderDecoder.BLOCK_LENGTH, CncHeaderDecoder.SCHEMA_VERSION);
        }

        public virtual void Dispose()
        {
            cncFile?.Dispose();
        }

        public virtual void SignalCncReady()
        {
            cncFile.SignalCncReady(CncHeaderEncoder.SCHEMA_VERSION);
        }

        public virtual void UpdateActivityTimestamp(long nowMs)
        {
            cncFile.TimestampOrdered(nowMs);
        }

        public virtual CncHeaderEncoder Encoder()
        {
            return cncHeaderEncoder;
        }

        public virtual CncHeaderDecoder Decoder()
        {
            return cncHeaderDecoder;
        }

        public static int AlignedTotalFileLength(int alignment, string aeronDirectory, string archiveChannel, string serviceControlChannel, string ingressChannel, string serviceName, string authenticator)
        {
            if (aeronDirectory == null) throw new ArgumentNullException(nameof(aeronDirectory));
            if (archiveChannel == null) throw new ArgumentNullException(nameof(archiveChannel));
            if (serviceControlChannel == null) throw new ArgumentNullException(nameof(serviceControlChannel));

            return BitUtil.Align(CncHeaderEncoder.BLOCK_LENGTH + (6 * VarAsciiEncodingEncoder.LengthEncodingLength()) + aeronDirectory.Length + archiveChannel.Length + serviceControlChannel.Length + ((null == ingressChannel) ? 0 : ingressChannel.Length) + ((null == serviceName) ? 0 : serviceName.Length) + ((null == authenticator) ? 0 : authenticator.Length), alignment);
        }
    }
}