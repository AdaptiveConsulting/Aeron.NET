using System;
using System.Diagnostics;
using System.IO;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Errors;
using Adaptive.Agrona.Util;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs.Mark;
using Adaptive.Cluster.Service;

namespace Adaptive.Cluster
{
    /// <summary>
    /// Used to indicate if a cluster component is running and what configuration it is using. Errors encountered by
    /// the service are recorded in within this file by a <see cref="DistinctErrorLog"/>
    /// </summary>
    public class ClusterMarkFile : IDisposable
    {
        /// <summary>
        /// Major version.
        /// </summary>
        public const int MAJOR_VERSION = 0;
        /// <summary>
        /// Minor version.
        /// </summary>
        public const int MINOR_VERSION = 3;
        /// <summary>
        /// Patch version.
        /// </summary>
        public const int PATCH_VERSION = 0;
        /// <summary>
        /// Full semantic version.
        /// </summary>
        public static readonly int SEMANTIC_VERSION =
            SemanticVersion.Compose(MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);
        /// <summary>
        /// Length of the <code>header</code> section.
        /// </summary>
        public const int HEADER_LENGTH = 8 * 1024;
        /// <summary>
        /// Special version to indicate that component failed to start.
        /// </summary>
        public const int VERSION_FAILED = -1;
        /// <summary>
        /// Min length for the error log buffer.
        /// </summary>
        public const int ERROR_BUFFER_MIN_LENGTH = 1024 * 1024;
        /// <summary>
        /// File extension used by the mark file.
        /// </summary>
        public const int ERROR_BUFFER_MAX_LENGTH = int.MaxValue - HEADER_LENGTH;
        /// <summary>
        /// Special version to indicate that component failed to start.
        /// </summary>
        public const string FILE_EXTENSION = ".dat";
        /// <summary>
        /// File extension used by the link file.
        /// </summary>
        public const string LINK_FILE_EXTENSION = ".lnk";
        /// <summary>
        /// Mark file name.
        /// </summary>
        public const string FILENAME = "cluster-mark" + FILE_EXTENSION;
        /// <summary>
        /// Link file name.
        /// </summary>
        public const string LINK_FILENAME = "cluster-mark" + LINK_FILE_EXTENSION;
        /// <summary>
        /// Service mark file name.
        /// </summary>
        public const string SERVICE_FILENAME_PREFIX = "cluster-mark-service-";
        
        private static readonly UnsafeBuffer EMPTY_BUFFER = new UnsafeBuffer((IntPtr)0, 0);

        private static readonly int HEADER_OFFSET = MessageHeaderDecoder.ENCODED_LENGTH;

        private readonly MarkFileHeaderDecoder headerDecoder = new MarkFileHeaderDecoder();
        private readonly MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder();
        private readonly MarkFile markFile;
        private readonly UnsafeBuffer buffer;
        private readonly UnsafeBuffer errorBuffer;
        private readonly int headerOffset;

        /// <summary>
        /// Create new <seealso cref="MarkFile"/> for a cluster component but check if an existing service is active.
        /// </summary>
        /// <param name="file">              full qualified file to the <seealso cref="MarkFile"/>. </param>
        /// <param name="type">              of cluster component the <seealso cref="MarkFile"/> represents. </param>
        /// <param name="errorBufferLength"> for storing the error log. </param>
        /// <param name="epochClock">        for checking liveness against. </param>
        /// <param name="timeoutMs">         for the activity check on an existing <seealso cref="MarkFile"/>. </param>
        /// <param name="filePageSize">      for aligning file length to.</param>
        public ClusterMarkFile(
            FileInfo file,
            ClusterComponentType type,
            int errorBufferLength,
            IEpochClock epochClock,
            long timeoutMs,
            int filePageSize)
        {
            if (errorBufferLength < ERROR_BUFFER_MIN_LENGTH || errorBufferLength > ERROR_BUFFER_MAX_LENGTH)
            {
                throw new ArgumentException("Invalid errorBufferLength: " + errorBufferLength);
            }
            
            LogBufferDescriptor.CheckPageSize(filePageSize);

            bool markFileExists = file.Exists;
            int totalFileLength = BitUtil.Align(HEADER_LENGTH + errorBufferLength, filePageSize);

            MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

            long candidateTermId;
            if (markFileExists)
            {
                int currentHeaderOffset = HeaderOffset(file);
                MarkFile existingMarkFile = new MarkFile(file, true,
                    currentHeaderOffset + MarkFileHeaderDecoder.VersionEncodingOffset(),
                    currentHeaderOffset + MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(), 
                    totalFileLength, 
                    timeoutMs,
                    epochClock,
                    (version) =>
                    {
                        if (VERSION_FAILED == version)
                        {
                            Console.Error.WriteLine("mark file version -1 indicates error on previous startup.");
                        }
                        else if (SemanticVersion.Major(version) != MAJOR_VERSION)
                        {
                            throw new ClusterException("mark file major version " + SemanticVersion.Major(version) +
                                                       " does not match software: " + MAJOR_VERSION);
                        }
                    }, null);
                UnsafeBuffer existingBuffer = existingMarkFile.Buffer();

                if (0 != currentHeaderOffset)
                {
                    headerDecoder.WrapAndApplyHeader(existingBuffer, 0, messageHeaderDecoder);
                }
                else
                {
                    headerDecoder.Wrap(
                        existingBuffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);
                }

                ClusterComponentType existingType = headerDecoder.ComponentType();
                if (existingType != ClusterComponentType.UNKNOWN && existingType != type)
                {
                    if (existingType != ClusterComponentType.BACKUP || ClusterComponentType.CONSENSUS_MODULE != type)
                    {
                        throw new ClusterException("existing Mark file type " + existingType +
                                                   " not same as required type " + type);
                    }
                }

                int existingErrorBufferLength = headerDecoder.ErrorBufferLength();
                var headerLength = headerDecoder.HeaderLength();
                UnsafeBuffer existingErrorBuffer =
                    new UnsafeBuffer(existingBuffer, headerLength, existingErrorBufferLength);

                SaveExistingErrors(file, existingErrorBuffer, type, Aeron.Aeron.Context.FallbackLogger());
                existingErrorBuffer.SetMemory(0, existingErrorBufferLength, 0);

                candidateTermId = headerDecoder.CandidateTermId();

                if (0 != currentHeaderOffset)
                {
                    markFile = existingMarkFile;
                    buffer = existingBuffer;
                }
                else
                {
                    headerDecoder.Wrap(EMPTY_BUFFER, 0, 0, 0);
                    CloseHelper.Dispose(existingMarkFile);
                    markFile = new MarkFile(
                        file, 
                        false,
                        HEADER_OFFSET + MarkFileHeaderDecoder.VersionEncodingOffset(),
                        HEADER_OFFSET + MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(), 
                        totalFileLength,
                        timeoutMs, 
                        epochClock, 
                        null, 
                        null);
                    buffer = existingMarkFile.Buffer();
                    buffer.SetMemory(0, headerLength, 0);
                }
            }
            else
            {
                markFile = new MarkFile(file, false, HEADER_OFFSET + MarkFileHeaderDecoder.VersionEncodingOffset(),
                    HEADER_OFFSET + MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(), totalFileLength, timeoutMs,
                    epochClock, null, null);
                buffer = markFile.Buffer();
                candidateTermId = Aeron.Aeron.NULL_VALUE;
            }

            headerOffset = HEADER_OFFSET;

            errorBuffer = new UnsafeBuffer(buffer, HEADER_LENGTH, errorBufferLength);

            headerEncoder
                .WrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                .ComponentType(type)
                .StartTimestamp(epochClock.Time())
                .Pid(Process.GetCurrentProcess().Id)
                .CandidateTermId(candidateTermId)
                .HeaderLength(HEADER_LENGTH
                ).ErrorBufferLength(errorBufferLength);

            headerDecoder.WrapAndApplyHeader(buffer, 0, messageHeaderDecoder);
        }


        /// <summary>
        /// Construct to read the status of an existing <seealso cref="MarkFile"/> for a cluster component.
        /// </summary>
        /// <param name="directory">  in which the mark file exists. </param>
        /// <param name="filename">   for the <seealso cref="MarkFile"/>. </param>
        /// <param name="epochClock"> to be used for checking liveness. </param>
        /// <param name="timeoutMs">  to wait for file to exist. </param>
        /// <param name="logger">     to which debug information will be written if an issue occurs. </param>
        public ClusterMarkFile(
            DirectoryInfo directory,
            string filename,
            IEpochClock epochClock,
            long timeoutMs,
            Action<string> logger) : this(OpenExistingMarkFile(directory,
            filename,
            epochClock,
            timeoutMs,
            logger))
        {
        }

        internal ClusterMarkFile(MarkFile markFile)
        {
            this.markFile = markFile;
            buffer = markFile.Buffer();

            headerOffset = HeaderOffset(buffer);
            if (0 != headerOffset)
            {
                headerEncoder.Wrap(buffer, headerOffset);
                headerDecoder.WrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
            }
            else
            {
                headerEncoder.Wrap(buffer, 0);
                headerDecoder.Wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);
            }

            errorBuffer = new UnsafeBuffer(buffer, headerDecoder.HeaderLength(), headerDecoder.ErrorBufferLength());
        }

        /// <summary>
        /// Get the parent directory containing the mark file.
        /// </summary>
        /// <returns> parent directory of the mark file. </returns>
        /// <seealso cref="MarkFile.ParentDirectory()"/>
        public DirectoryInfo ParentDirectory()
        {
            return markFile.ParentDirectory();
        }


        /// <summary>
        /// Determines if this path name matches the service mark file name pattern
        /// </summary>
        /// <param name="path">       to examine. </param>
        /// <returns> true if the name matches. </returns>
        public static bool IsServiceMarkFile(FileInfo path)
        {
            string fileName = path.Name;
            return fileName.StartsWith(SERVICE_FILENAME_PREFIX, StringComparison.Ordinal) &&
                   fileName.EndsWith(FILE_EXTENSION, StringComparison.Ordinal);
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
            if (!markFile.IsClosed())
            {
                headerEncoder.Wrap(EMPTY_BUFFER, 0);
                headerDecoder.Wrap(EMPTY_BUFFER, 0, 0, 0);
                errorBuffer.Wrap(0, 0);
                CloseHelper.Dispose(markFile);
            }
        }

        /// <summary>
        /// Check if the <seealso cref="MarkFile"/> is closed.
        /// </summary>
        /// <returns> true if the <seealso cref="MarkFile"/> is closed. </returns>
        public bool IsClosed
        {
            get { return markFile.IsClosed(); }
        }

        /// <summary>
        /// Get the current value of a candidate term id if a vote is placed in an election with volatile semantics.
        /// </summary>
        /// <returns> the current candidate term id within an election after voting or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if
        /// no voting phase of an election is currently active. </returns>
        public long CandidateTermId()
        {
            return markFile.IsClosed()
                ? Aeron.Aeron.NULL_VALUE
                : buffer.GetLongVolatile(headerOffset + MarkFileHeaderDecoder.CandidateTermIdEncodingOffset());
        }

        /// <summary>
        /// Cluster member id either assigned statically or as the result of dynamic membership join.
        /// </summary>
        /// <returns> cluster member id either assigned statically or as the result of dynamic membership join. </returns>
        public int MemberId()
        {
            return markFile.IsClosed() ? Aeron.Aeron.NULL_VALUE : headerDecoder.MemberId();
        }

        /// <summary>
        /// Member id assigned as part of dynamic join of a cluster.
        /// </summary>
        /// <param name="memberId"> assigned as part of dynamic join of a cluster. </param>
        public void MemberId(int memberId)
        {
            if (!markFile.IsClosed())
            {
                buffer.PutInt(MarkFileHeaderEncoder.MemberIdEncodingOffset(), memberId);
            }
        }

        /// <summary>
        /// Identity of the cluster instance so multiple clusters can run on the same driver.
        /// </summary>
        /// <returns> id of the cluster instance so multiple clusters can run on the same driver. </returns>
        public int ClusterId()
        {
            return markFile.IsClosed() ? Aeron.Aeron.NULL_VALUE : headerDecoder.ClusterId();
        }

        /// <summary>
        /// Identity of the cluster instance so multiple clusters can run on the same driver.
        /// </summary>
        /// <param name="clusterId"> of the cluster instance so multiple clusters can run on the same driver. </param>
        public void ClusterId(int clusterId)
        {
            if (!markFile.IsClosed())
            {
                buffer.PutInt(MarkFileHeaderEncoder.ClusterIdEncodingOffset(), clusterId);
            }
        }

        /// <summary>
        /// Signal the cluster component has concluded successfully and ready to start.
        /// </summary>
        public void SignalReady()
        {
            if (!markFile.IsClosed())
            {
                markFile.SignalReady(SEMANTIC_VERSION);
            }
        }

        /// <summary>
        /// Signal the cluster component has failed to conclude and cannot start.
        /// </summary>
        public void SignalFailedStart()
        {
            if (!markFile.IsClosed())
            {
                markFile.SignalReady(VERSION_FAILED);
            }
        }

        /// <summary>
        /// Update the activity timestamp as a proof of life.
        /// </summary>
        /// <param name="nowMs"> activity timestamp as a proof of life. </param>
        public void UpdateActivityTimestamp(long nowMs)
        {
            if (!markFile.IsClosed())
            {
                markFile.TimestampRelease(nowMs);
            }
        }

        /// <summary>
        /// Read the activity timestamp of the cluster component with volatile semantics.
        /// </summary>
        /// <returns> the activity timestamp of the cluster component with volatile semantics. </returns>
        public long ActivityTimestampVolatile()
        {
            return markFile.IsClosed() ? Aeron.Aeron.NULL_VALUE : markFile.TimestampVolatile();
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

        private static void SaveExistingErrors(FileInfo markFile, IAtomicBuffer errorBuffer, ClusterComponentType type,
            TextWriter logger)
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
                HEADER_OFFSET +
                MarkFileHeaderEncoder.BLOCK_LENGTH +
                5 * VarAsciiEncodingEncoder.LengthEncodingLength() +
                (aeronDirectory?.Length ?? 0) +
                (controlChannel?.Length ?? 0) +
                (ingressChannel?.Length ?? 0) +
                (serviceName?.Length ?? 0) +
                (authenticator?.Length ?? 0);

            if (length > HEADER_LENGTH)
            {
                throw new ClusterException(
                    $"ClusterMarkFile headerLength={length} > headerLengthCapacity={HEADER_LENGTH}.");
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
        /// The filename to be used for the link file given a service id.
        /// </summary>
        /// <param name="serviceId"> of the service the <seealso cref="ClusterMarkFile"/> represents. </param>
        /// <returns> the filename to be used for the link file given a service id. </returns>
        public static string LinkFilenameForService(int serviceId)
        {
            return SERVICE_FILENAME_PREFIX + serviceId + LINK_FILE_EXTENSION;
        }

        /// <summary>
        /// The control properties for communicating between the consensus module and the services.
        /// </summary>
        /// <returns> the control properties for communicating between the consensus module and the services or <code>null</code>
        /// if mark file was already closed. </returns>
        public ClusterNodeControlProperties LoadControlProperties()
        {
            if (!markFile.IsClosed())
            {
                headerDecoder.SbeRewind();
                return new ClusterNodeControlProperties(
                    headerDecoder.MemberId(),
                    headerDecoder.ServiceStreamId(),
                    headerDecoder.ConsensusModuleStreamId(),
                    headerDecoder.AeronDirectory(),
                    headerDecoder.ControlChannel());
            }

            return null;
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

        /// <summary>
        /// Forces any changes made to the mark file's content to be written to the storage device containing the mapped
        /// file.
        /// </summary>
        /// <remarks>Since 1.44.0</remarks>
        public void Force()
        {
            if (!markFile.IsClosed())
            {
                MappedByteBuffer mappedByteBuffer = markFile.MappedByteBuffer();
                mappedByteBuffer.Force();
            }
        }

        private static int HeaderOffset(FileInfo file)
        {
            MappedByteBuffer mappedByteBuffer = IoUtil.MapExistingFile(file, FILENAME);
            try
            {
                UnsafeBuffer unsafeBuffer = new UnsafeBuffer(mappedByteBuffer, 0, HEADER_OFFSET);
                return HeaderOffset(unsafeBuffer);
            }
            finally
            {
                IoUtil.Unmap(mappedByteBuffer);
            }
        }

        private static int HeaderOffset(UnsafeBuffer headerBuffer)
        {
            MessageHeaderDecoder decoder = new MessageHeaderDecoder();
            decoder.Wrap(headerBuffer, 0);
            return MarkFileHeaderDecoder.TEMPLATE_ID == decoder.TemplateId() &&
                   MarkFileHeaderDecoder.SCHEMA_ID == decoder.SchemaId()
                ? HEADER_OFFSET
                : 0;
        }

        private static MarkFile OpenExistingMarkFile(DirectoryInfo directory, string filename, IEpochClock epochClock,
            long timeoutMs, Action<string> logger)
        {
            int headerOffset = HeaderOffset(new FileInfo(Path.Combine(directory.FullName, filename)));
            return new MarkFile(
                directory,
                filename,
                headerOffset + MarkFileHeaderDecoder.VersionEncodingOffset(),
                headerOffset + MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(),
                timeoutMs,
                epochClock,
                (version) =>
                {
                    if (SemanticVersion.Major(version) != MAJOR_VERSION)
                    {
                        throw new ClusterException("mark file major version " + SemanticVersion.Major(version) +
                                                   " does not match software: " + MAJOR_VERSION);
                    }
                },
                logger);
        }
    }
}