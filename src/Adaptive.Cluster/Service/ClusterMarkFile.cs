/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    /// Used to indicate if a cluster component is running and what configuration it is using. Errors encountered by the
    /// service are recorded in within this file by a <see cref="DistinctErrorLog"/>
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
        public static readonly int SEMANTIC_VERSION = SemanticVersion.Compose(
            MAJOR_VERSION,
            MINOR_VERSION,
            PATCH_VERSION
        );

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

        private static readonly UnsafeBuffer EmptyBuffer = new UnsafeBuffer((IntPtr)0, 0);

        private static readonly int HeaderFieldOffset = MessageHeaderDecoder.ENCODED_LENGTH;

        private readonly MarkFileHeaderDecoder _headerDecoder = new MarkFileHeaderDecoder();
        private readonly MarkFileHeaderEncoder _headerEncoder = new MarkFileHeaderEncoder();
        private readonly MarkFile _markFile;
        private readonly UnsafeBuffer _buffer;
        private readonly UnsafeBuffer _errorBuffer;
        private readonly int _headerOffset;

        /// <summary>
        /// Create new <seealso cref="MarkFile"/> for a cluster component but check if an existing service is active.
        /// </summary>
        /// <param name="file">              full qualified file to the <seealso cref="MarkFile"/>. </param>
        /// <param name="type">              of cluster component the <seealso cref="MarkFile"/> represents. </param>
        /// <param name="errorBufferLength"> for storing the error log. </param>
        /// <param name="epochClock">        for checking liveness against. </param>
        /// <param name="timeoutMs">         for the activity check on an existing <seealso cref="MarkFile"/>. </param>
        /// <param name="filePageSize">      for aligning file length to.</param>
        // Upstream: io.aeron.cluster.service.ClusterMarkFile ctor is ~142 lines.
        // Java Checkstyle MethodLength scopes to METHOD_DEF only (no CTOR_DEF), so
        // upstream needs no annotation; Sonar S138 covers ctors.
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S138:Functions should not have too many lines",
            Justification = "Upstream Java parity; ctor mirrors upstream which Checkstyle does not flag."
        )]
        public ClusterMarkFile(
            FileInfo file,
            ClusterComponentType type,
            int errorBufferLength,
            IEpochClock epochClock,
            long timeoutMs,
            int filePageSize
        )
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
                MarkFile existingMarkFile = new MarkFile(
                    file,
                    true,
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
                            throw new ClusterException(
                                "mark file major version "
                                    + SemanticVersion.Major(version)
                                    + " does not match software: "
                                    + MAJOR_VERSION
                            );
                        }
                    },
                    null
                );
                UnsafeBuffer existingBuffer = existingMarkFile.Buffer();

                if (0 != currentHeaderOffset)
                {
                    _headerDecoder.WrapAndApplyHeader(existingBuffer, 0, messageHeaderDecoder);
                }
                else
                {
                    _headerDecoder.Wrap(
                        existingBuffer,
                        0,
                        MarkFileHeaderDecoder.BLOCK_LENGTH,
                        MarkFileHeaderDecoder.SCHEMA_VERSION
                    );
                }

                ClusterComponentType existingType = _headerDecoder.ComponentType();
                if (
                    existingType != ClusterComponentType.UNKNOWN
                    && existingType != type
                    && (existingType != ClusterComponentType.BACKUP || ClusterComponentType.CONSENSUS_MODULE != type)
                )
                {
                    throw new ClusterException(
                        "existing Mark file type " + existingType + " not same as required type " + type
                    );
                }

                int existingErrorBufferLength = _headerDecoder.ErrorBufferLength();
                var headerLength = _headerDecoder.HeaderLength();
                UnsafeBuffer existingErrorBuffer = new UnsafeBuffer(
                    existingBuffer,
                    headerLength,
                    existingErrorBufferLength
                );

                SaveExistingErrors(file, existingErrorBuffer, type, Aeron.Aeron.Context.FallbackLogger());
                existingErrorBuffer.SetMemory(0, existingErrorBufferLength, 0);

                candidateTermId = _headerDecoder.CandidateTermId();

                if (0 != currentHeaderOffset)
                {
                    _markFile = existingMarkFile;
                    _buffer = existingBuffer;
                }
                else
                {
                    _headerDecoder.Wrap(EmptyBuffer, 0, 0, 0);
                    CloseHelper.Dispose(existingMarkFile);
                    _markFile = new MarkFile(
                        file,
                        false,
                        HeaderFieldOffset + MarkFileHeaderDecoder.VersionEncodingOffset(),
                        HeaderFieldOffset + MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(),
                        totalFileLength,
                        timeoutMs,
                        epochClock,
                        null,
                        null
                    );
                    _buffer = existingMarkFile.Buffer();
                    _buffer.SetMemory(0, headerLength, 0);
                }
            }
            else
            {
                _markFile = new MarkFile(
                    file,
                    false,
                    HeaderFieldOffset + MarkFileHeaderDecoder.VersionEncodingOffset(),
                    HeaderFieldOffset + MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(),
                    totalFileLength,
                    timeoutMs,
                    epochClock,
                    null,
                    null
                );
                _buffer = _markFile.Buffer();
                candidateTermId = Aeron.Aeron.NULL_VALUE;
            }

            _headerOffset = HeaderFieldOffset;

            _errorBuffer = new UnsafeBuffer(_buffer, HEADER_LENGTH, errorBufferLength);

            _headerEncoder
                .WrapAndApplyHeader(_buffer, 0, new MessageHeaderEncoder())
                .ComponentType(type)
                .StartTimestamp(epochClock.Time())
                .Pid(Process.GetCurrentProcess().Id)
                .CandidateTermId(candidateTermId)
                .HeaderLength(HEADER_LENGTH)
                .ErrorBufferLength(errorBufferLength);

            _headerDecoder.WrapAndApplyHeader(_buffer, 0, messageHeaderDecoder);
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
            Action<string> logger
        )
            : this(OpenExistingMarkFile(directory, filename, epochClock, timeoutMs, logger)) { }

        internal ClusterMarkFile(MarkFile markFile)
        {
            this._markFile = markFile;
            _buffer = markFile.Buffer();

            _headerOffset = HeaderOffset(_buffer);
            if (0 != _headerOffset)
            {
                _headerEncoder.Wrap(_buffer, _headerOffset);
                _headerDecoder.WrapAndApplyHeader(_buffer, 0, new MessageHeaderDecoder());
            }
            else
            {
                _headerEncoder.Wrap(_buffer, 0);
                _headerDecoder.Wrap(
                    _buffer,
                    0,
                    MarkFileHeaderDecoder.BLOCK_LENGTH,
                    MarkFileHeaderDecoder.SCHEMA_VERSION
                );
            }

            _errorBuffer = new UnsafeBuffer(_buffer, _headerDecoder.HeaderLength(), _headerDecoder.ErrorBufferLength());
        }

        /// <summary>
        /// Get the parent directory containing the mark file.
        /// </summary>
        /// <returns> parent directory of the mark file. </returns>
        /// <seealso cref="MarkFile.ParentDirectory()"/>
        public DirectoryInfo ParentDirectory()
        {
            return _markFile.ParentDirectory();
        }

        /// <summary>
        /// Determines if this path name matches the service mark file name pattern
        /// </summary>
        /// <param name="path">       to examine. </param>
        /// <returns> true if the name matches. </returns>
        public static bool IsServiceMarkFile(FileInfo path)
        {
            string fileName = path.Name;
            return fileName.StartsWith(SERVICE_FILENAME_PREFIX, StringComparison.Ordinal)
                && fileName.EndsWith(FILE_EXTENSION, StringComparison.Ordinal);
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
            if (!_markFile.IsClosed())
            {
                _headerEncoder.Wrap(EmptyBuffer, 0);
                _headerDecoder.Wrap(EmptyBuffer, 0, 0, 0);
                _errorBuffer.Wrap(0, 0);
                CloseHelper.Dispose(_markFile);
            }
        }

        /// <summary>
        /// Check if the <seealso cref="MarkFile"/> is closed.
        /// </summary>
        /// <returns> true if the <seealso cref="MarkFile"/> is closed. </returns>
        public bool IsClosed
        {
            get { return _markFile.IsClosed(); }
        }

        /// <summary>
        /// Get the current value of a candidate term id if a vote is placed in an election with volatile semantics.
        /// </summary>
        /// <returns> the current candidate term id within an election after voting or
        /// <seealso cref="Aeron.Aeron.NULL_VALUE"/> if
        /// no voting phase of an election is currently active. </returns>
        public long CandidateTermId()
        {
            return _markFile.IsClosed()
                ? Aeron.Aeron.NULL_VALUE
                : _buffer.GetLongVolatile(_headerOffset + MarkFileHeaderDecoder.CandidateTermIdEncodingOffset());
        }

        /// <summary>
        /// Cluster member id.
        /// </summary>
        /// <returns> cluster member id. </returns>
        public int MemberId()
        {
            return _markFile.IsClosed() ? Aeron.Aeron.NULL_VALUE : _headerDecoder.MemberId();
        }

        /// <summary>
        /// Member id assigned as part of active join to the log in a clustered service.
        /// </summary>
        /// <param name="memberId"> assigned as part of active join to the log in a clustered service. </param>
        public void MemberId(int memberId)
        {
            if (!_markFile.IsClosed())
            {
                _buffer.PutInt(MarkFileHeaderEncoder.MemberIdEncodingOffset(), memberId);
            }
        }

        /// <summary>
        /// Identity of the cluster instance so multiple clusters can run on the same driver.
        /// </summary>
        /// <returns> id of the cluster instance so multiple clusters can run on the same driver. </returns>
        public int ClusterId()
        {
            return _markFile.IsClosed() ? Aeron.Aeron.NULL_VALUE : _headerDecoder.ClusterId();
        }

        /// <summary>
        /// Identity of the cluster instance so multiple clusters can run on the same driver.
        /// </summary>
        /// <param name="clusterId"> of the cluster instance so multiple clusters can run on the same driver. </param>
        public void ClusterId(int clusterId)
        {
            if (!_markFile.IsClosed())
            {
                _buffer.PutInt(MarkFileHeaderEncoder.ClusterIdEncodingOffset(), clusterId);
            }
        }

        /// <summary>
        /// Signal the cluster component has concluded successfully and ready to start.
        /// </summary>
        public void SignalReady()
        {
            if (!_markFile.IsClosed())
            {
                _markFile.SignalReady(SEMANTIC_VERSION);
            }
        }

        /// <summary>
        /// Signal the cluster component has failed to conclude and cannot start.
        /// </summary>
        public void SignalFailedStart()
        {
            if (!_markFile.IsClosed())
            {
                _markFile.SignalReady(VERSION_FAILED);
            }
        }

        /// <summary>
        /// Update the activity timestamp as a proof of life.
        /// </summary>
        /// <param name="nowMs"> activity timestamp as a proof of life. </param>
        public void UpdateActivityTimestamp(long nowMs)
        {
            if (!_markFile.IsClosed())
            {
                _markFile.TimestampRelease(nowMs);
            }
        }

        /// <summary>
        /// Read the activity timestamp of the cluster component with volatile semantics.
        /// </summary>
        /// <returns> the activity timestamp of the cluster component with volatile semantics. </returns>
        public long ActivityTimestampVolatile()
        {
            return _markFile.IsClosed() ? Aeron.Aeron.NULL_VALUE : _markFile.TimestampVolatile();
        }

        /// <summary>
        /// The encoder for writing the <seealso cref="MarkFile"/> header.
        /// </summary>
        /// <returns> the encoder for writing the <seealso cref="MarkFile"/> header. </returns>
        public MarkFileHeaderEncoder Encoder()
        {
            return _headerEncoder;
        }

        /// <summary>
        /// The decoder for reading the <seealso cref="MarkFile"/> header.
        /// </summary>
        /// <returns> the decoder for reading the <seealso cref="MarkFile"/> header. </returns>
        public MarkFileHeaderDecoder Decoder()
        {
            return _headerDecoder;
        }

        public UnsafeBuffer Buffer => _buffer;

        /// <summary>
        /// The direct buffer which wraps the region of the <seealso cref="MarkFile"/> which contains the error log.
        /// </summary>
        /// <returns> the direct buffer which wraps the region of the <seealso cref="MarkFile"/> which contains the
        /// error log. </returns>
        public UnsafeBuffer ErrorBuffer => _errorBuffer;

        private static void SaveExistingErrors(
            FileInfo markFile,
            IAtomicBuffer errorBuffer,
            ClusterComponentType type,
            TextWriter logger
        )
        {
            var str = new MemoryStream();
            var writer = new StreamWriter(str);
            var observations = Aeron.Aeron.Context.PrintErrorLog(errorBuffer, writer);
            writer.Flush();
            str.Seek(0, SeekOrigin.Begin);

            if (observations > 0)
            {
                var errorLogFilename = Path.Combine(
                    markFile.DirectoryName,
                    type + "-" + DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-fff") + "-error.log"
                );

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
            string authenticator
        )
        {
            var length =
                HeaderFieldOffset
                + MarkFileHeaderEncoder.BLOCK_LENGTH
                + 5 * VarAsciiEncodingEncoder.LengthEncodingLength()
                + (aeronDirectory?.Length ?? 0)
                + (controlChannel?.Length ?? 0)
                + (ingressChannel?.Length ?? 0)
                + (serviceName?.Length ?? 0)
                + (authenticator?.Length ?? 0);

            if (length > HEADER_LENGTH)
            {
                throw new ClusterException(
                    $"ClusterMarkFile headerLength={length} > headerLengthCapacity={HEADER_LENGTH}."
                );
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
        /// <returns> the control properties for communicating between the consensus module and the services or
        /// <code>null</code>
        /// if mark file was already closed. </returns>
        public ClusterNodeControlProperties LoadControlProperties()
        {
            if (!_markFile.IsClosed())
            {
                _headerDecoder.SbeRewind();
                return new ClusterNodeControlProperties(
                    _headerDecoder.MemberId(),
                    _headerDecoder.ServiceStreamId(),
                    _headerDecoder.ConsensusModuleStreamId(),
                    _headerDecoder.AeronDirectory(),
                    _headerDecoder.ControlChannel()
                );
            }

            return null;
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override string ToString()
        {
            return "ClusterMarkFile{"
                + "semanticVersion="
                + SemanticVersion.ToString(SEMANTIC_VERSION)
                + ", markFile="
                + _markFile.FileName()
                + '}';
        }

        /// <summary>
        /// Forces any changes made to the mark file's content to be written to the storage device containing the mapped
        /// file.
        /// </summary>
        /// <remarks>Since 1.44.0</remarks>
        public void Force()
        {
            if (!_markFile.IsClosed())
            {
                MappedByteBuffer mappedByteBuffer = _markFile.MappedByteBuffer();
                mappedByteBuffer.Force();
            }
        }

        private static int HeaderOffset(FileInfo file)
        {
            MappedByteBuffer mappedByteBuffer = IoUtil.MapExistingFile(file, FILENAME);
            try
            {
                UnsafeBuffer unsafeBuffer = new UnsafeBuffer(mappedByteBuffer, 0, HeaderFieldOffset);
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
            return
                MarkFileHeaderDecoder.TEMPLATE_ID == decoder.TemplateId()
                && MarkFileHeaderDecoder.SCHEMA_ID == decoder.SchemaId()
                ? HeaderFieldOffset
                : 0;
        }

        private static MarkFile OpenExistingMarkFile(
            DirectoryInfo directory,
            string filename,
            IEpochClock epochClock,
            long timeoutMs,
            Action<string> logger
        )
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
                        throw new ClusterException(
                            "mark file major version "
                                + SemanticVersion.Major(version)
                                + " does not match software: "
                                + MAJOR_VERSION
                        );
                    }
                },
                logger
            );
        }
    }
}
