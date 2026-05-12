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
using System.IO;
using System.Text;
using System.Threading;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Interface for managing Command-n-Control files.
    ///
    /// The assumptions are: (1) the version field is an int in size, (2) the timestamp field is a long in size, and (3)
    /// the version field comes before the timestamp field.
    /// </summary>
    public class MarkFile : IDisposable
    {
        private readonly int _versionFieldOffset;
        private readonly int _timestampFieldOffset;

        private readonly DirectoryInfo _parentDir;
        private readonly FileInfo _markFile;
        private readonly MappedByteBuffer _mappedBuffer;
        private readonly UnsafeBuffer _buffer;

        private volatile bool _isClosed = false;

        /// <summary>
        /// Create a CnC directory and file if none present. Checking if an active CnC file exists and is active. Old
        /// CnC file is deleted and recreated if not active.
        ///
        /// Total length of CnC file will be mapped until <seealso cref="Dispose"/> is called.
        /// </summary>
        /// <param name="directory">             for the CnC file </param>
        /// <param name="filename">              of the CnC file </param>
        /// <param name="warnIfDirectoryExists"> for logging purposes </param>
        /// <param name="dirDeleteOnStart">      if desired </param>
        /// <param name="versionFieldOffset">    to use for version field access </param>
        /// <param name="timestampFieldOffset">  to use for timestamp field access </param>
        /// <param name="totalFileLength">       to allocate when creating new CnC file </param>
        /// <param name="timeoutMs">             for the activity check (in milliseconds) </param>
        /// <param name="epochClock">            to use for time checks </param>
        /// <param name="versionCheck">          to use for existing CnC file and version field </param>
        /// <param name="logger">                to use to signal progress or null </param>
        public MarkFile(
            DirectoryInfo directory,
            string filename,
            bool warnIfDirectoryExists,
            bool dirDeleteOnStart,
            int versionFieldOffset,
            int timestampFieldOffset,
            int totalFileLength,
            long timeoutMs,
            IEpochClock epochClock,
            Action<int> versionCheck,
            Action<string> logger
        )
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            EnsureDirectoryExists(
                directory,
                filename,
                warnIfDirectoryExists,
                dirDeleteOnStart,
                versionFieldOffset,
                timestampFieldOffset,
                timeoutMs,
                epochClock,
                versionCheck,
                logger
            );

            _parentDir = directory;
            _markFile = new FileInfo(Path.Combine(directory.Name, filename));
            _mappedBuffer = MapNewFile(_markFile, totalFileLength);
            _buffer = new UnsafeBuffer(_mappedBuffer.Pointer, totalFileLength);
            _versionFieldOffset = versionFieldOffset;
            _timestampFieldOffset = timestampFieldOffset;
        }

        /// <summary>
        /// Create a CnC file if none present. Checking if an active CnC file exists and is active. Existing CnC file is
        /// used if not active.
        ///
        /// Total length of CnC file will be mapped until <seealso cref="Dispose"/> is called.
        /// </summary>
        /// <param name="markFile">               to use </param>
        /// <param name="shouldPreExist">        or not </param>
        /// <param name="versionFieldOffset">    to use for version field access </param>
        /// <param name="timestampFieldOffset">  to use for timestamp field access </param>
        /// <param name="totalFileLength">       to allocate when creating new CnC file </param>
        /// <param name="timeoutMs">             for the activity check (in milliseconds) </param>
        /// <param name="epochClock">            to use for time checks </param>
        /// <param name="versionCheck">          to use for existing CnC file and version field </param>
        /// <param name="logger">                to use to signal progress or null </param>
        public MarkFile(
            FileInfo markFile,
            bool shouldPreExist,
            int versionFieldOffset,
            int timestampFieldOffset,
            int totalFileLength,
            long timeoutMs,
            IEpochClock epochClock,
            Action<int> versionCheck,
            Action<string> logger
        )
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            _parentDir = markFile.Directory;
            _markFile = markFile;
            _mappedBuffer = mapNewOrExistingMarkFile(
                markFile,
                shouldPreExist,
                versionFieldOffset,
                timestampFieldOffset,
                totalFileLength,
                timeoutMs,
                epochClock,
                versionCheck,
                logger
            );

            _buffer = new UnsafeBuffer(_mappedBuffer.Pointer, totalFileLength);
            _versionFieldOffset = versionFieldOffset;
            _timestampFieldOffset = timestampFieldOffset;
        }

        /// <summary>
        /// Map a pre-existing CnC file if one present and is active.
        ///
        /// Total length of CnC file will be mapped until <seealso cref="Dispose"/> is called.
        /// </summary>
        /// <param name="directory">             for the CnC file </param>
        /// <param name="filename">              of the CnC file </param>
        /// <param name="versionFieldOffset">    to use for version field access </param>
        /// <param name="timestampFieldOffset">  to use for timestamp field access </param>
        /// <param name="timeoutMs"> for the activity check (in milliseconds) and for how long to wait for file to exist
        /// </param>
        /// <param name="epochClock">            to use for time checks </param>
        /// <param name="versionCheck">          to use for existing CnC file and version field </param>
        /// <param name="logger">                to use to signal progress or null </param>
        public MarkFile(
            DirectoryInfo directory,
            string filename,
            int versionFieldOffset,
            int timestampFieldOffset,
            long timeoutMs,
            IEpochClock epochClock,
            Action<int> versionCheck,
            Action<string> logger
        )
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            _parentDir = directory;
            _markFile = new FileInfo(Path.Combine(directory.FullName, filename));
            _mappedBuffer = MapExistingMarkFile(
                _markFile,
                versionFieldOffset,
                timestampFieldOffset,
                timeoutMs,
                epochClock,
                versionCheck,
                logger
            );
            _buffer = new UnsafeBuffer(_mappedBuffer);
            _versionFieldOffset = versionFieldOffset;
            _timestampFieldOffset = timestampFieldOffset;
        }

        /// <summary>
        /// Manage a CnC file given a mapped file and offsets of version and timestamp.
        ///
        /// If mappedCncBuffer is not null, then it will be unmapped upon <seealso cref="Dispose"/> .
        /// </summary>
        /// <param name="mappedBuffer">      for the CnC fields </param>
        /// <param name="versionFieldOffset">   for the version field </param>
        /// <param name="timestampFieldOffset"> for the timestamp field </param>
        public MarkFile(MappedByteBuffer mappedBuffer, int versionFieldOffset, int timestampFieldOffset)
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            _parentDir = null;
            _markFile = null;
            _mappedBuffer = mappedBuffer;
            _buffer = new UnsafeBuffer(mappedBuffer);
            _versionFieldOffset = versionFieldOffset;
            _timestampFieldOffset = timestampFieldOffset;
        }

        /// <summary>
        /// Manage a CnC file given a buffer and offsets of version and timestamp.
        /// </summary>
        /// <param name="buffer">            for the CnC fields </param>
        /// <param name="versionFieldOffset">   for the version field </param>
        /// <param name="timestampFieldOffset"> for the timestamp field </param>
        public MarkFile(UnsafeBuffer buffer, int versionFieldOffset, int timestampFieldOffset)
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            _parentDir = null;
            _markFile = null;
            _mappedBuffer = null;
            _buffer = buffer;
            _versionFieldOffset = versionFieldOffset;
            _timestampFieldOffset = timestampFieldOffset;
        }

        public bool IsClosed()
        {
            return _isClosed;
        }

        public void Dispose()
        {
            if (!_isClosed)
            {
                if (null != _mappedBuffer)
                {
                    IoUtil.Unmap(_mappedBuffer);
                }

                _isClosed = true;
            }
        }

        public void SignalReady(int version)
        {
            _buffer.PutIntOrdered(_versionFieldOffset, version);
        }

        public int VersionVolatile()
        {
            return _buffer.GetIntVolatile(_versionFieldOffset);
        }

        public int VersionWeak()
        {
            return _buffer.GetInt(_versionFieldOffset);
        }

        public void TimestampOrdered(long timestamp)
        {
            _buffer.PutLongOrdered(_timestampFieldOffset, timestamp);
        }

        public void TimestampRelease(long timestamp)
        {
            _buffer.PutLongRelease(_timestampFieldOffset, timestamp);
        }

        public long TimestampVolatile()
        {
            return _buffer.GetLongVolatile(_timestampFieldOffset);
        }

        public long TimestampWeak()
        {
            return _buffer.GetLong(_timestampFieldOffset);
        }

        public void DeleteDirectory(bool ignoreFailures)
        {
            IoUtil.Delete(_parentDir, ignoreFailures);
        }

        public DirectoryInfo ParentDirectory()
        {
            return _parentDir;
        }

        public FileInfo FileName()
        {
            return _markFile;
        }

        public MappedByteBuffer MappedByteBuffer()
        {
            return _mappedBuffer;
        }

        public UnsafeBuffer Buffer()
        {
            return _buffer;
        }

        public static void EnsureDirectoryExists(
            DirectoryInfo directory,
            string filename,
            bool warnIfDirectoryExists,
            bool dirDeleteOnStart,
            int versionFieldOffset,
            int timestampFieldOffset,
            long timeoutMs,
            IEpochClock epochClock,
            Action<int> versionCheck,
            Action<string> logger
        )
        {
            FileInfo cncFile = new FileInfo(Path.Combine(directory.FullName, filename));

            if (directory.Exists)
            {
                if (warnIfDirectoryExists && null != logger)
                {
                    logger("WARNING: " + directory + " already exists.");
                }

                if (!dirDeleteOnStart)
                {
                    int offset = Math.Min(versionFieldOffset, timestampFieldOffset);
                    int length = Math.Max(versionFieldOffset, timestampFieldOffset) + BitUtil.SIZE_OF_LONG - offset;
                    MappedByteBuffer cncByteBuffer = MapExistingFile(cncFile, logger, offset, length);

                    try
                    {
                        if (
                            IsActive(
                                cncByteBuffer,
                                epochClock,
                                timeoutMs,
                                versionFieldOffset,
                                timestampFieldOffset,
                                versionCheck,
                                logger
                            )
                        )
                        {
                            throw new System.InvalidOperationException("active mark file detected");
                        }
                    }
                    finally
                    {
                        IoUtil.Unmap(cncByteBuffer);
                    }
                }

                IoUtil.Delete(directory, false);
            }

            IoUtil.EnsureDirectoryExists(directory, directory.ToString());
        }

        public static MappedByteBuffer MapExistingMarkFile(
            FileInfo markFile,
            int versionFieldOffset,
            int timestampFieldOffset,
            long timeoutMs,
            IEpochClock epochClock,
            Action<int> versionCheck,
            Action<string> logger
        )
        {
            long startTimeMs = epochClock.Time();

            while (true)
            {
                while (!markFile.Exists)
                {
                    if (epochClock.Time() > (startTimeMs + timeoutMs))
                    {
                        throw new InvalidOperationException("CnC file not found: " + markFile.FullName);
                    }

                    Sleep(16);
                }

                MappedByteBuffer cncByteBuffer = MapExistingFile(markFile, logger);
                UnsafeBuffer cncBuffer = new UnsafeBuffer(cncByteBuffer);

                int cncVersion;
                while (0 == (cncVersion = cncBuffer.GetIntVolatile(versionFieldOffset)))
                {
                    if (epochClock.Time() > (startTimeMs + timeoutMs))
                    {
                        throw new InvalidOperationException("CnC file is created but not initialised.");
                    }

                    Sleep(1);
                }

                versionCheck(cncVersion);

                while (0 == cncBuffer.GetLongVolatile(timestampFieldOffset))
                {
                    if (epochClock.Time() > (startTimeMs + timeoutMs))
                    {
                        throw new InvalidOperationException("No non-0 timestamp detected.");
                    }

                    Sleep(1);
                }

                return cncByteBuffer;
            }
        }

        public static MappedByteBuffer mapNewOrExistingMarkFile(
            FileInfo markFile,
            bool shouldPreExist,
            int versionFieldOffset,
            int timestampFieldOffset,
            long totalFileLength,
            long timeoutMs,
            IEpochClock epochClock,
            Action<int> versionCheck,
            Action<string> logger
        )
        {
            MappedByteBuffer cncByteBuffer = null;

            try
            {
                cncByteBuffer = IoUtil.MapNewOrExistingFile(markFile, totalFileLength);

                UnsafeBuffer cncBuffer = new UnsafeBuffer(cncByteBuffer);

                if (shouldPreExist)
                {
                    int cncVersion = cncBuffer.GetIntVolatile(versionFieldOffset);

                    if (null != logger)
                    {
                        logger("INFO: Mark file exists: " + markFile);
                    }

                    versionCheck(cncVersion);

                    long timestamp = cncBuffer.GetLongVolatile(timestampFieldOffset);
                    long now = epochClock.Time();
                    long timestampAge = now - timestamp;

                    if (null != logger)
                    {
                        logger("INFO: heartbeat is (ms): " + timestampAge);
                    }

                    if (timestampAge < timeoutMs)
                    {
                        throw new InvalidOperationException("Active mark file detected");
                    }
                }
            }
            catch (Exception)
            {
                if (null != cncByteBuffer)
                {
                    IoUtil.Unmap(cncByteBuffer);
                }

                throw;
            }

            return cncByteBuffer;
        }

        public static MappedByteBuffer MapExistingFile(
            FileInfo cncFile,
            Action<string> logger,
            long offset,
            long length
        )
        {
            if (cncFile.Exists)
            {
                if (null != logger)
                {
                    logger("INFO: Mark file exists: " + cncFile);
                }

                return IoUtil.MapExistingFile(cncFile, offset, length);
            }

            return null;
        }

        public static MappedByteBuffer MapExistingFile(FileInfo cncFile, Action<string> logger)
        {
            if (cncFile.Exists)
            {
                if (null != logger)
                {
                    logger("INFO: Mark file exists: " + cncFile);
                }

                return IoUtil.MapExistingFile(cncFile, cncFile.ToString());
            }

            return null;
        }

        public static MappedByteBuffer MapNewFile(FileInfo cncFile, long length)
        {
            return IoUtil.MapNewFile(cncFile, length);
        }

        public static bool IsActive(
            MappedByteBuffer cncByteBuffer,
            IEpochClock epochClock,
            long timeoutMs,
            int versionFieldOffset,
            int timestampFieldOffset,
            Action<int> versionCheck,
            Action<string> logger
        )
        {
            if (null == cncByteBuffer)
            {
                return false;
            }

            UnsafeBuffer cncBuffer = new UnsafeBuffer(cncByteBuffer);

            long startTimeMs = epochClock.Time();
            int cncVersion;
            while (0 == (cncVersion = cncBuffer.GetIntVolatile(versionFieldOffset)))
            {
                if (epochClock.Time() > (startTimeMs + timeoutMs))
                {
                    throw new System.InvalidOperationException("Mark file is created but not initialised.");
                }

                Sleep(1);
            }

            versionCheck(cncVersion);

            long timestamp = cncBuffer.GetLongVolatile(timestampFieldOffset);
            long now = epochClock.Time();
            long timestampAge = now - timestamp;

            if (null != logger)
            {
                logger("INFO: heartbeat is (ms): " + timestampAge);
            }

            return timestampAge <= timeoutMs;
        }

        private static void ValidateOffsets(int versionFieldOffset, int timestampFieldOffset)
        {
            if ((versionFieldOffset + BitUtil.SIZE_OF_INT) > timestampFieldOffset)
            {
                throw new ArgumentException("version field must precede the timestamp field");
            }
        }

        /// <summary>
        /// Ensure a link file exists if required for the actual mark file. A link file will contain the pathname of the
        /// actual mark file's parent directory. This is useful if the mark file should be stored on a different storage
        /// medium to the directory of the service. This will create a file with name of <paramref name="linkFilename"/>
        /// in the <paramref name="serviceDir"/> . If <paramref name="actualFile"/> is an immediate child of
        /// <paramref name="serviceDir"/> then any file with the name of
        /// <paramref name="linkFilename"/> will be deleted from the <paramref name="serviceDir"/> (so that links won't
        /// be present if not required).
        /// </summary>
        /// <param name="serviceDir">Directory where the mark file would normally be stored (e.g. archiveDir,
        /// clusterDir).</param>
        /// <param name="actualFile">Location of actual mark file, e.g. /dev/shm/service/node0/archive-mark.dat</param>
        /// <param name="linkFilename">Short name that should be used for the link file, e.g. archive-mark.lnk</param>
        public static void EnsureMarkFileLink(DirectoryInfo serviceDir, FileInfo actualFile, string linkFilename)
        {
            string serviceDirPath;
            string markFileParentPath;

            try
            {
                serviceDirPath = serviceDir.FullName;
            }
            catch (Exception)
            {
                throw new ArgumentException("Failed to resolve canonical path for serviceDir=" + serviceDir);
            }

            try
            {
                markFileParentPath = actualFile.Directory.FullName;
            }
            catch (Exception)
            {
                throw new ArgumentException(
                    "Failed to resolve canonical path for markFile parent dir of " + actualFile
                );
            }

            string linkFilePath = Path.Combine(serviceDirPath, linkFilename);
            if (serviceDirPath.Equals(markFileParentPath, StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    if (File.Exists(linkFilePath))
                    {
                        File.Delete(linkFilePath);
                    }
                }
                catch (IOException ex)
                {
                    throw new Exception("Failed to remove old link file", ex);
                }
            }
            else
            {
                try
                {
                    File.WriteAllText(linkFilePath, markFileParentPath, Encoding.ASCII);
                }
                catch (IOException ex)
                {
                    throw new Exception("Failed to create link for mark file directory", ex);
                }
            }
        }

        internal static void Sleep(int durationMs)
        {
            try
            {
                Thread.Sleep(durationMs);
            }
            catch (ThreadInterruptedException)
            {
                Thread.CurrentThread.Interrupt();
            }
        }
    }
}
