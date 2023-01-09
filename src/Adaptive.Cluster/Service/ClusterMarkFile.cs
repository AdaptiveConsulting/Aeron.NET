using System;
using System.Diagnostics;
using System.IO;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Errors;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs.Mark;
using Adaptive.Cluster.Service;

namespace Adaptive.Cluster
{
    /// <summary>
    /// Used to indicate if a cluster service is running and what configuration it is using. Errors encountered by
    /// the service are recorded in within this file by a <see cref="DistinctErrorLog"/>
    /// </summary>
    public class ClusterMarkFile : IDisposable
    {
        public const int MAJOR_VERSION = 0;
        public const int MINOR_VERSION = 3;
        public const int PATCH_VERSION = 0;
        public static readonly int SEMANTIC_VERSION = SemanticVersion.Compose(MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);

        public const int HEADER_LENGTH = 8 * 1024;
        public const int VERSION_FAILED = -1;
        
        public const string FILE_EXTENSION = ".dat";
        public const string FILENAME = "cluster-mark" + FILE_EXTENSION;
        public const string SERVICE_FILENAME_PREFIX = "cluster-mark-service-";

        private readonly MarkFileHeaderDecoder headerDecoder = new MarkFileHeaderDecoder();
        private readonly MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder();
        private readonly MarkFile markFile;
        private readonly UnsafeBuffer buffer;
        private readonly UnsafeBuffer errorBuffer;

        /// <summary>
        /// Create new <seealso cref="MarkFile"/> for a cluster service but check if an existing service is active.
        /// </summary>
        /// <param name="file">              full qualified file to the <seealso cref="MarkFile"/>. </param>
        /// <param name="type">              of cluster component the <seealso cref="MarkFile"/> represents. </param>
        /// <param name="errorBufferLength"> for storing the error log. </param>
        /// <param name="epochClock">        for checking liveness against. </param>
        /// <param name="timeoutMs">         for the activity check on an existing <seealso cref="MarkFile"/>. </param>
        public ClusterMarkFile(
            FileInfo file,
            ClusterComponentType type,
            int errorBufferLength,
            IEpochClock epochClock,
            long timeoutMs)
        {
            var markFileExists = file.Exists;
            int totalFileLength = HEADER_LENGTH + errorBufferLength;

            markFile = new MarkFile(
                file,
                markFileExists,
                MarkFileHeaderDecoder.VersionEncodingOffset(),
                MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(),
                totalFileLength,
                timeoutMs,
                epochClock,
                (version) =>
                {
                    if (VERSION_FAILED == version && markFileExists)
                    {
                        Console.WriteLine("mark file version -1 indicates error on previous startup.");
                    }
                    else if (SemanticVersion.Major(version) != MAJOR_VERSION)
                    {
                        throw new ClusterException("mark file major version " + SemanticVersion.Major(version) +
                                                   " does not match software: " + MAJOR_VERSION);
                    }
                },
                null);

            buffer = markFile.Buffer();
            errorBuffer = new UnsafeBuffer(buffer, HEADER_LENGTH, errorBufferLength);


            headerEncoder.Wrap(buffer, 0);
            headerDecoder.Wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);

            if (markFileExists)
            {
                if (buffer.Capacity != totalFileLength)
                {
                    throw new ClusterException(
                        "ClusterMarkFile capacity=" + buffer.Capacity + " < expectedCapacity=" + totalFileLength);
                }

                var existingErrorBufferLength = headerDecoder.ErrorBufferLength();
                var existingErrorBuffer = new UnsafeBuffer(
                    buffer, headerDecoder.HeaderLength(), existingErrorBufferLength);

                SaveExistingErrors(file, existingErrorBuffer, type, Aeron.Aeron.Context.FallbackLogger());
                existingErrorBuffer.SetMemory(0, existingErrorBufferLength, 0);
            }
            else
            {
                headerEncoder.CandidateTermId(Aeron.Aeron.NULL_VALUE);
            }

            var existingType = headerDecoder.ComponentType();

            if (existingType != ClusterComponentType.NULL && existingType != type)
            {
                if (existingType != ClusterComponentType.BACKUP || ClusterComponentType.CONSENSUS_MODULE != type)
                {
                    throw new ClusterException(
                        "existing Mark file type " + existingType + " not same as required type " + type);
                }
            }

            headerEncoder.ComponentType(type);
            headerEncoder.HeaderLength(HEADER_LENGTH);
            headerEncoder.ErrorBufferLength(errorBufferLength);
            headerEncoder.Pid(Process.GetCurrentProcess().Id);
            headerEncoder.StartTimestamp(epochClock.Time());
        }

        /// <summary>
        /// Construct to read the status of an existing <seealso cref="MarkFile"/> for a cluster component.
        /// </summary>
        /// <param name="directory">  in which the mark file exists. </param>
        /// <param name="filename">   for the <seealso cref="MarkFile"/>. </param>
        /// <param name="epochClock"> to be used for checking liveness. </param>
        /// <param name="timeoutMs">  to wait for file to exist. </param>
        /// <param name="logger">     to which debug information will be written if an issue occurs. </param>
        public ClusterMarkFile(DirectoryInfo directory, string filename, IEpochClock epochClock, long timeoutMs,
            Action<string> logger)
        {
            markFile = new MarkFile(
                directory,
                filename,
                MarkFileHeaderDecoder.VersionEncodingOffset(),
                MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(),
                timeoutMs,
                epochClock,
                (version) =>
                {
                    if (SemanticVersion.Major(version) != MAJOR_VERSION)
                    {
                        throw new ClusterException("mark file major version " + SemanticVersion.Major(version) + 
                                                    " does not match software: " + AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION);
                    }

                },
                logger);

            buffer = markFile.Buffer();
            headerDecoder.Wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);
            errorBuffer = new UnsafeBuffer(buffer, headerDecoder.HeaderLength(), headerDecoder.ErrorBufferLength());
        }

        /// <summary>
        /// Determines if this path name matches the service mark file name pattern
        /// </summary>
        /// <param name="path">       to examine. </param>
        /// <returns> true if the name matches. </returns>
        public static bool IsServiceMarkFile(FileInfo path)
        {
            string fileName = path.Name;
            return fileName.StartsWith(SERVICE_FILENAME_PREFIX, StringComparison.Ordinal) && fileName.EndsWith(FILE_EXTENSION, StringComparison.Ordinal);
        }

        /// <summary>
        /// Determines if this path name matches the consensus module file name pattern.
        /// </summary>
        /// <param name="path">       to examine. </param>
        /// <returns> true if the name matches. </returns>
        public static bool IsConsensusModuleMarkFile(FileInfo path)
        {
            return path.Name.Equals(FILENAME);
        }
        
        /// <inheritdoc />
        public void Dispose()
        {
            markFile?.Dispose();
        }
        
        /// <summary>
        /// Check if the <seealso cref="MarkFile"/> is closed.
        /// </summary>
        /// <returns> true if the <seealso cref="MarkFile"/> is closed. </returns>
        public bool IsClosed
        {
            get
            {
                return markFile.IsClosed();
            }
        }
        
        /// <summary>
        /// Get the current value of a candidate term id if a vote is placed in an election.
        /// </summary>
        /// <returns> the current candidate term id within an election after voting or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if
        /// no voting phase of an election is currently active. </returns>
        public long CandidateTermId()
        {
            return buffer.GetLongVolatile(MarkFileHeaderDecoder.CandidateTermIdEncodingOffset());
        }

        /// <summary>
        /// Record the fact that a node is aware of an election, so it can survive a restart.
        /// </summary>
        /// <param name="candidateTermId"> to record that a vote has taken place. </param>
        /// <param name="fileSyncLevel"> as defined by cluster file sync level.</param>
        public void CandidateTermId(long candidateTermId, int fileSyncLevel)
        {
            buffer.PutLongVolatile(MarkFileHeaderEncoder.CandidateTermIdEncodingOffset(), candidateTermId);

            if (fileSyncLevel > 0)
            {
                markFile.MappedByteBuffer().Flush();
            }
        }
        
        /// <summary>
        /// Record the fact that a node is aware of an election, so it can survive a restart.
        /// </summary>
        /// <param name="candidateTermId"> to record that a vote has taken place. </param>
        /// <param name="fileSyncLevel">   as defined by cluster file sync level. </param>
        /// <returns> the max of the existing and proposed candidateTermId. </returns>
        public long ProposeMaxCandidateTermId(long candidateTermId, int fileSyncLevel)
        {
            long existingCandidateTermId = buffer.GetLongVolatile(MarkFileHeaderEncoder.CandidateTermIdEncodingOffset());

            if (candidateTermId > existingCandidateTermId)
            {
                CandidateTermId(candidateTermId, fileSyncLevel);
                return candidateTermId;
            }

            return existingCandidateTermId;
        }

        /// <summary>
        /// Cluster member id either assigned statically or as the result of dynamic membership join.
        /// </summary>
        /// <returns> cluster member id either assigned statically or as the result of dynamic membership join. </returns>
        public int MemberId()
        {
            return buffer.GetInt(MarkFileHeaderDecoder.MemberIdEncodingOffset());
        }

        /// <summary>
        /// Member id assigned as part of dynamic join of a cluster.
        /// </summary>
        /// <param name="memberId"> assigned as part of dynamic join of a cluster. </param>
        public void MemberId(int memberId)
        {
            buffer.PutInt(MarkFileHeaderEncoder.MemberIdEncodingOffset(), memberId);
        }
        
        /// <summary>
        /// Identity of the cluster instance so multiple clusters can run on the same driver.
        /// </summary>
        /// <returns> id of the cluster instance so multiple clusters can run on the same driver. </returns>
        public int ClusterId()
        {
            return buffer.GetInt(MarkFileHeaderDecoder.ClusterIdEncodingOffset());
        }

        /// <summary>
        /// Identity of the cluster instance so multiple clusters can run on the same driver.
        /// </summary>
        /// <param name="clusterId"> of the cluster instance so multiple clusters can run on the same driver. </param>
        public void ClusterId(int clusterId)
        {
            buffer.PutInt(MarkFileHeaderEncoder.ClusterIdEncodingOffset(), clusterId);
        }

        /// <summary>
        /// Signal the cluster component has concluded successfully and ready to start.
        /// </summary>
        public void SignalReady()
        {
            markFile.SignalReady(SEMANTIC_VERSION);
        }

        /// <summary>
        /// Signal the cluster component has failed to conclude and cannot start.
        /// </summary>
        public void SignalFailedStart()
        {
            markFile.SignalReady(VERSION_FAILED);
        }

        /// <summary>
        /// Update the activity timestamp as a proof of life.
        /// </summary>
        /// <param name="nowMs"> activity timestamp as a proof of life. </param>
        public void UpdateActivityTimestamp(long nowMs)
        {
            if (!markFile.IsClosed())
            {
                markFile.TimestampOrdered(nowMs);
            }
        }

        /// <summary>
        /// Read the activity timestamp of the cluster component with volatile semantics.
        /// </summary>
        /// <returns> the activity timestamp of the cluster component with volatile semantics. </returns>
        public long ActivityTimestampVolatile()
        {
            return markFile.TimestampVolatile();
        }

        /// <summary>
        /// The encoder for writing the <seealso cref="MarkFile"/> header.
        /// </summary>
        /// <returns> the encoder for writing the <seealso cref="MarkFile"/> header. </returns>
        public MarkFileHeaderEncoder Encoder()
        {
            return headerEncoder;
        }

        /// <summary>
        /// The decoder for reading the <seealso cref="MarkFile"/> header.
        /// </summary>
        /// <returns> the decoder for reading the <seealso cref="MarkFile"/> header. </returns>
        public MarkFileHeaderDecoder Decoder()
        {
            return headerDecoder;
        }

       
        public UnsafeBuffer Buffer => buffer;

        /// <summary>
        /// The direct buffer which wraps the region of the <seealso cref="MarkFile"/> which contains the error log.
        /// </summary>
        /// <returns> the direct buffer which wraps the region of the <seealso cref="MarkFile"/> which contains the error log. </returns>
        public UnsafeBuffer ErrorBuffer => errorBuffer;

        private static void SaveExistingErrors(FileInfo markFile, IAtomicBuffer errorBuffer, ClusterComponentType type, TextWriter logger)
        {
            var str = new MemoryStream();
            var writer = new StreamWriter(str);
            var observations = Aeron.Aeron.Context.PrintErrorLog(errorBuffer, writer);
            writer.Flush();
            str.Seek(0, SeekOrigin.Begin);

            if (observations > 0)
            {
                var errorLogFilename = Path.Combine(markFile.DirectoryName,
                    type + "-" + DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-fff") + "-error.log");

                logger?.WriteLine("WARNING: existing errors saved to: " + errorLogFilename);

                using (var file = new FileStream(errorLogFilename, FileMode.CreateNew))
                {
                    str.CopyTo(file);
                }
            }

            writer.Close();
        }

        /// <summary>
        /// Check if the header length is sufficient for encoding the provided properties.
        /// </summary>
        /// <param name="aeronDirectory"> for the media driver. </param>
        /// <param name="controlChannel"> for the consensus module. </param>
        /// <param name="ingressChannel"> from the cluster clients. </param>
        /// <param name="serviceName">    for the application service. </param>
        /// <param name="authenticator">  for the application service. </param>
        public static void CheckHeaderLength(
            string aeronDirectory,
            string controlChannel,
            string ingressChannel,
            string serviceName,
            string authenticator)
        {
            var length =
                MarkFileHeaderEncoder.BLOCK_LENGTH +
                5 * VarAsciiEncodingEncoder.LengthEncodingLength() +
                (aeronDirectory?.Length ?? 0) +
                (controlChannel?.Length ?? 0) +
                (ingressChannel?.Length ?? 0) +
                (serviceName?.Length ?? 0) +
                (authenticator?.Length ?? 0);

            if (length > HEADER_LENGTH)
            {
                throw new ClusterException($"ClusterMarkFile headerLength={length} > headerLengthCapacity={HEADER_LENGTH}.");
            }
        }

        /// <summary>
        /// The filename to be used for the mark file given a service id.
        /// </summary>
        /// <param name="serviceId"> of the service the <seealso cref="ClusterMarkFile"/> represents. </param>
        /// <returns> the filename to be used for the mark file given a service id. </returns>
        public static string MarkFilenameForService(int serviceId)
        {
            return SERVICE_FILENAME_PREFIX + serviceId + FILE_EXTENSION;
        }
        
        /// <summary>
        /// The control properties for communicating between the consensus module and the services.
        /// </summary>
        /// <returns> the control properties for communicating between the consensus module and the services. </returns>
        public ClusterNodeControlProperties LoadControlProperties()
        {
            MarkFileHeaderDecoder decoder = new MarkFileHeaderDecoder();
            decoder.Wrap(
                headerDecoder.Buffer(),
                headerDecoder.InitialOffset(),
                MarkFileHeaderDecoder.BLOCK_LENGTH,
                MarkFileHeaderDecoder.SCHEMA_VERSION);

            return new ClusterNodeControlProperties(
                decoder.MemberId(),
                decoder.ServiceStreamId(),
                decoder.ConsensusModuleStreamId(),
                decoder.AeronDirectory(),
                decoder.ControlChannel());
        }
        
        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override string ToString()
        {
            return "ClusterMarkFile{" +
                   "semanticVersion=" + SemanticVersion.ToString(SEMANTIC_VERSION) +
                   ", markFile=" + markFile.FileName() +
                   '}';
        }
    }
}