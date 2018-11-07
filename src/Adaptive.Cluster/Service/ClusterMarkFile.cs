﻿using System;
using System.Diagnostics;
using System.IO;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Errors;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs.Mark;

namespace Adaptive.Cluster
{
    
    /// <summary>
    /// Used to indicate if a cluster service is running and what configuration it is using. Errors encountered by
    /// the service are recorded in within this file by a <see cref="DistinctErrorLog"/>
    /// </summary>
    public class ClusterMarkFile : IDisposable
    {
        public const string FILE_EXTENSION = ".dat";
        public const string FILENAME = "cluster-mark" + FILE_EXTENSION;
        public const string SERVICE_FILE_NAME_PREFIX = "cluster-mark-service-";
        public const string SERVICE_FILE_NAME_FORMAT = SERVICE_FILE_NAME_PREFIX + "{0}" + FILE_EXTENSION;
        public const int HEADER_LENGTH = 8 * 1024;
        public const int VERSION_READY = MarkFileHeaderDecoder.SCHEMA_VERSION;
        public const int VERSION_FAILED = -1;

        private readonly MarkFileHeaderDecoder headerDecoder = new MarkFileHeaderDecoder();
        private readonly MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder();
        private readonly MarkFile markFile;
        private readonly UnsafeBuffer buffer;
        private readonly UnsafeBuffer errorBuffer;

        public ClusterMarkFile(
            FileInfo file,
            ClusterComponentType type,
            int errorBufferLength,
            IEpochClock epochClock,
            long timeoutMs)
        {
            var markFileExists = file.Exists;

            markFile = new MarkFile(
                file,
                markFileExists,
                MarkFileHeaderDecoder.VersionEncodingOffset(),
                MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(),
                HEADER_LENGTH + errorBufferLength,
                timeoutMs,
                epochClock,
                (version) =>
                {
                    if (version != MarkFileHeaderDecoder.SCHEMA_VERSION)
                    {
                        if (VERSION_FAILED == version && markFileExists)
                        {
                            Console.WriteLine("mark file version -1 indicates error on previous startup.");
                        }
                        else
                        {
                            throw new ClusterException("mark file version " + version + " does not match software:" +
                                                       MarkFileHeaderDecoder.SCHEMA_VERSION);
                        }
                    }
                },
                null);    

            buffer = markFile.Buffer();
            errorBuffer = new UnsafeBuffer(buffer, HEADER_LENGTH, errorBufferLength);


            headerEncoder.Wrap(buffer, 0);
            headerDecoder.Wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);

            if (markFileExists)
            {
                var existingErrorBuffer = new UnsafeBuffer(
                    buffer, headerDecoder.HeaderLength(), headerDecoder.ErrorBufferLength());

                SaveExistingErrors(file, existingErrorBuffer, Console.Error);

                errorBuffer.SetMemory(0, errorBufferLength, 0);
            }
            else
            {
                headerEncoder.CandidateTermId(Aeron.Aeron.NULL_VALUE);
            }

            var existingType = headerDecoder.ComponentType();

            if (existingType != ClusterComponentType.NULL && existingType != type)
            {
                throw new InvalidOperationException("existing Mark file type " + existingType + " not same as required type " + type);
            }

            headerEncoder.ComponentType(type);
            headerEncoder.HeaderLength(HEADER_LENGTH);
            headerEncoder.ErrorBufferLength(errorBufferLength);
            headerEncoder.Pid(Process.GetCurrentProcess().Id);
            headerEncoder.StartTimestamp(epochClock.Time());
        }

        public ClusterMarkFile(DirectoryInfo directory, string filename, IEpochClock epochClock, long timeoutMs, Action<string> logger)
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
                    if (version != MarkFileHeaderDecoder.SCHEMA_VERSION)
                    {
                        throw new ArgumentException("mark file version " + version + " does not match software:" + MarkFileHeaderDecoder.SCHEMA_VERSION);
                    }
                },
                logger);

            buffer = markFile.Buffer();
            headerDecoder.Wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);
            errorBuffer = new UnsafeBuffer(buffer, headerDecoder.HeaderLength(), headerDecoder.ErrorBufferLength());
        }

        public void Dispose()
        {
            markFile?.Dispose();
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
        /// Record the fact that a node has voted in a current election for a candidate so it can survive a restart.
        /// </summary>
        /// <param name="candidateTermId"> to record that a vote has taken place. </param>
        public void CandidateTermId(long candidateTermId)
        {
            buffer.PutLongVolatile(MarkFileHeaderEncoder.CandidateTermIdEncodingOffset(), candidateTermId);
            markFile.MappedByteBuffer().Flush();
        }

        public int MemberId()
        {
            return buffer.GetIntVolatile(MarkFileHeaderDecoder.MemberIdEncodingOffset());
        }

        public void MemberId(int memberId)
        {
            buffer.PutIntVolatile(MarkFileHeaderEncoder.MemberIdEncodingOffset(), memberId);
            markFile.MappedByteBuffer().Flush();
        }

        public void SignalReady()
        {
            markFile.SignalReady(MarkFileHeaderDecoder.SCHEMA_VERSION);
            markFile.MappedByteBuffer().Flush();
        }
        
        public void SignalFailedStart()
        {
            markFile.SignalReady(VERSION_FAILED);
            markFile.MappedByteBuffer().Flush();
        }

        public void UpdateActivityTimestamp(long nowMs)
        {
            markFile.TimestampOrdered(nowMs);
        }

        public long ActivityTimestampVolatile()
        {
            return markFile.TimestampVolatile();
        }

        public MarkFileHeaderEncoder Encoder()
        {
            return headerEncoder;
        }

        public MarkFileHeaderDecoder Decoder()
        {
            return headerDecoder;
        }

        public UnsafeBuffer Buffer => buffer;

        public UnsafeBuffer ErrorBuffer => errorBuffer;

        private static void SaveExistingErrors(FileInfo markFile, IAtomicBuffer errorBuffer, TextWriter logger)
        {
            var str = new MemoryStream();
            var writer = new StreamWriter(str);
            var observations = SaveErrorLog(writer, errorBuffer);
            writer.Flush();
            str.Seek(0, SeekOrigin.Begin);

            if (observations > 0)
            {
                var errorLogFilename = Path.Combine(markFile.DirectoryName, DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-fff") + "-error.log");

                logger?.WriteLine("WARNING: Existing errors saved to: " + errorLogFilename);

                using (var file = new FileStream(errorLogFilename, FileMode.CreateNew))
                {
                    str.CopyTo(file);
                }
            }
            
            writer.Close();
        }

        private static int SaveErrorLog(TextWriter writer, IAtomicBuffer errorBuffer)
        {
            void ErrorConsumer(int count, long firstTimestamp, long lastTimestamp, string ex)
            {
                writer.WriteLine($"***{writer.NewLine}{count} observations from {firstTimestamp} to {lastTimestamp} for:{writer.NewLine} {ex}");
            }

            var distinctErrorCount = ErrorLogReader.Read(errorBuffer, ErrorConsumer);

            writer.WriteLine($"{writer.NewLine}{distinctErrorCount} distinct errors observed.");

            return distinctErrorCount;
        }

        public static void CheckHeaderLength(
            string aeronDirectory,
            string archiveChannel,
            string serviceControlChannel,
            string ingressChannel,
            string serviceName,
            string authenticator)
        {
            if (aeronDirectory == null) throw new ArgumentNullException(nameof(aeronDirectory));
            if (archiveChannel == null) throw new ArgumentNullException(nameof(archiveChannel));
            if (serviceControlChannel == null) throw new ArgumentNullException(nameof(serviceControlChannel));

            var lengthRequired =
                MarkFileHeaderEncoder.BLOCK_LENGTH +
                6 * VarAsciiEncodingEncoder.LengthEncodingLength() +
                aeronDirectory.Length +
                archiveChannel.Length +
                serviceControlChannel.Length +
                (ingressChannel?.Length ?? 0) +
                (serviceName?.Length ?? 0) +
                (authenticator?.Length ?? 0);

            if (lengthRequired > HEADER_LENGTH)
            {
                throw new ClusterException($"MarkFile length required {lengthRequired} great than {HEADER_LENGTH}.");
            }
        }

        public static string MarkFilenameForService(int serviceId)
        {
            return string.Format(SERVICE_FILE_NAME_FORMAT, serviceId);
        }
    }
}