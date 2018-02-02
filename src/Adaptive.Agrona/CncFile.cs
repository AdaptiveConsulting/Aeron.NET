using System;
using System.IO;
using System.Threading;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Interface for managing Command-n-Control files.
    /// 
    /// The assumptions are: (1) the version field is an int in size, (2) the timestamp field is a long in size,
    /// and (3) the version field comes before the timestamp field.
    /// </summary>
    public class CncFile : IDisposable
    {
        private readonly int versionFieldOffset;
        private readonly int timestampFieldOffset;

        private readonly DirectoryInfo cncDir;
        private readonly FileInfo cncFile;
        private readonly MappedByteBuffer mappedCncBuffer;
        private readonly UnsafeBuffer cncBuffer;

        private volatile bool isClosed = false;

        /// <summary>
        /// Create a CnC directory and file if none present. Checking if an active CnC file exists and is active. Old CnC
        /// file is deleted and recreated if not active.
        /// 
        /// Total length of CnC file will be mapped until <seealso cref="#close()"/> is called.
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
        public CncFile(DirectoryInfo directory, string filename, bool warnIfDirectoryExists, bool dirDeleteOnStart,
            int versionFieldOffset, int timestampFieldOffset, int totalFileLength, long timeoutMs,
            IEpochClock epochClock, Action<int> versionCheck, Action<string> logger)
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            EnsureDirectoryExists(directory, filename, warnIfDirectoryExists, dirDeleteOnStart, versionFieldOffset,
                timestampFieldOffset, timeoutMs, epochClock, versionCheck, logger);

            this.cncDir = directory;
            this.cncFile = new FileInfo(Path.Combine(directory.Name, filename));
            this.mappedCncBuffer = MapNewFile(cncFile, totalFileLength);
            this.cncBuffer = new UnsafeBuffer(mappedCncBuffer.Pointer, totalFileLength);
            this.versionFieldOffset = versionFieldOffset;
            this.timestampFieldOffset = timestampFieldOffset;
        }

        /// <summary>
        /// Create a CnC file if none present. Checking if an active CnC file exists and is active. Existing CnC file
        /// is used if not active.
        /// 
        /// Total length of CnC file will be mapped until <seealso cref="#close()"/> is called.
        /// </summary>
        /// <param name="cncFile">               to use </param>
        /// <param name="shouldPreExist">        or not </param>
        /// <param name="versionFieldOffset">    to use for version field access </param>
        /// <param name="timestampFieldOffset">  to use for timestamp field access </param>
        /// <param name="totalFileLength">       to allocate when creating new CnC file </param>
        /// <param name="timeoutMs">             for the activity check (in milliseconds) </param>
        /// <param name="epochClock">            to use for time checks </param>
        /// <param name="versionCheck">          to use for existing CnC file and version field </param>
        /// <param name="logger">                to use to signal progress or null </param>
        public CncFile(FileInfo cncFile, bool shouldPreExist, int versionFieldOffset, int timestampFieldOffset,
            int totalFileLength, long timeoutMs, IEpochClock epochClock, Action<int> versionCheck,
            Action<string> logger)
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            this.cncDir = cncFile.Directory;
            this.cncFile = cncFile;
            this.mappedCncBuffer = MapNewOrExistingCncFile(cncFile, shouldPreExist, versionFieldOffset,
                timestampFieldOffset, totalFileLength, timeoutMs, epochClock, versionCheck, logger);

            this.cncBuffer = new UnsafeBuffer(mappedCncBuffer.Pointer, totalFileLength);
            this.versionFieldOffset = versionFieldOffset;
            this.timestampFieldOffset = timestampFieldOffset;
        }

        /// <summary>
        /// Map a pre-existing CnC file if one present and is active.
        /// 
        /// Total length of CnC file will be mapped until <seealso cref="#close()"/> is called.
        /// </summary>
        /// <param name="directory">             for the CnC file </param>
        /// <param name="filename">              of the CnC file </param>
        /// <param name="versionFieldOffset">    to use for version field access </param>
        /// <param name="timestampFieldOffset">  to use for timestamp field access </param>
        /// <param name="timeoutMs">             for the activity check (in milliseconds) and for how long to wait for file to exist </param>
        /// <param name="epochClock">            to use for time checks </param>
        /// <param name="versionCheck">          to use for existing CnC file and version field </param>
        /// <param name="logger">                to use to signal progress or null </param>
        public CncFile(DirectoryInfo directory, string filename, int versionFieldOffset, int timestampFieldOffset,
            long timeoutMs, IEpochClock epochClock, Action<int> versionCheck, Action<string> logger)
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            this.cncDir = directory;
            this.cncFile = new FileInfo(Path.Combine(directory.FullName, filename));
            this.mappedCncBuffer = MapExistingCncFile(cncFile, versionFieldOffset, timestampFieldOffset, timeoutMs,
                epochClock, versionCheck, logger);
            this.cncBuffer = new UnsafeBuffer(mappedCncBuffer);
            this.versionFieldOffset = versionFieldOffset;
            this.timestampFieldOffset = timestampFieldOffset;
        }

        /// <summary>
        /// Manage a CnC file given a mapped file and offsets of version and timestamp.
        /// 
        /// If mappedCncBuffer is not null, then it will be unmapped upon <seealso cref="#close()"/>.
        /// </summary>
        /// <param name="mappedCncBuffer">      for the CnC fields </param>
        /// <param name="versionFieldOffset">   for the version field </param>
        /// <param name="timestampFieldOffset"> for the timestamp field </param>
        public CncFile(MappedByteBuffer mappedCncBuffer, int versionFieldOffset, int timestampFieldOffset)
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            this.cncDir = null;
            this.cncFile = null;
            this.mappedCncBuffer = mappedCncBuffer;
            this.cncBuffer = new UnsafeBuffer(mappedCncBuffer);
            this.versionFieldOffset = versionFieldOffset;
            this.timestampFieldOffset = timestampFieldOffset;
        }

        /// <summary>
        /// Manage a CnC file given a buffer and offsets of version and timestamp.
        /// </summary>
        /// <param name="cncBuffer">            for the CnC fields </param>
        /// <param name="versionFieldOffset">   for the version field </param>
        /// <param name="timestampFieldOffset"> for the timestamp field </param>
        public CncFile(UnsafeBuffer cncBuffer, int versionFieldOffset, int timestampFieldOffset)
        {
            ValidateOffsets(versionFieldOffset, timestampFieldOffset);

            this.cncDir = null;
            this.cncFile = null;
            this.mappedCncBuffer = null;
            this.cncBuffer = cncBuffer;
            this.versionFieldOffset = versionFieldOffset;
            this.timestampFieldOffset = timestampFieldOffset;
        }

        public virtual bool IsClosed()
        {
            return isClosed;
        }

        public void Dispose()
        {
            if (!isClosed)
            {
                if (null != mappedCncBuffer)
                {
                    IoUtil.Unmap(mappedCncBuffer);
                }

                isClosed = true;
            }
        }

        public virtual void SignalCncReady(int version)
        {
            cncBuffer.PutIntOrdered(versionFieldOffset, version);
        }

        public virtual int VersionVolatile()
        {
            return cncBuffer.GetIntVolatile(versionFieldOffset);
        }

        public virtual int VersionWeak()
        {
            return cncBuffer.GetInt(versionFieldOffset);
        }

        public virtual void TimestampOrdered(long timestamp)
        {
            cncBuffer.PutLongOrdered(timestampFieldOffset, timestamp);
        }

        public virtual long TimestampVolatile()
        {
            return cncBuffer.GetLongVolatile(timestampFieldOffset);
        }

        public virtual long TimestampWeak()
        {
            return cncBuffer.GetLong(timestampFieldOffset);
        }

        public virtual void DeleteDirectory(bool ignoreFailures)
        {
            IoUtil.Delete(cncDir, ignoreFailures);
        }

        public virtual DirectoryInfo CncDirectory()
        {
            return cncDir;
        }

        public virtual FileInfo CncFileName()
        {
            return cncFile;
        }

        public virtual MappedByteBuffer MappedByteBuffer()
        {
            return mappedCncBuffer;
        }

        public virtual UnsafeBuffer Buffer()
        {
            return cncBuffer;
        }

        public static void EnsureDirectoryExists(DirectoryInfo directory, string filename, bool warnIfDirectoryExists,
            bool dirDeleteOnStart, int versionFieldOffset, int timestampFieldOffset, long timeoutMs,
            IEpochClock epochClock, Action<int> versionCheck, Action<string> logger)
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
                        if (IsActive(cncByteBuffer, epochClock, timeoutMs, versionFieldOffset, timestampFieldOffset,
                            versionCheck, logger))
                        {
                            throw new System.InvalidOperationException("Active CnC file detected");
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

        public static MappedByteBuffer MapExistingCncFile(FileInfo cncFile, int versionFieldOffset,
            int timestampFieldOffset, long timeoutMs, IEpochClock epochClock, Action<int> versionCheck,
            Action<string> logger)
        {
            long startTimeMs = epochClock.Time();

            while (true)
            {
                while (!cncFile.Exists)
                {
                    if (epochClock.Time() > (startTimeMs + timeoutMs))
                    {
                        throw new InvalidOperationException("CnC file not found: " + cncFile.FullName);
                    }

                    Sleep(16);
                }

                MappedByteBuffer cncByteBuffer = MapExistingFile(cncFile, logger);
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

        public static MappedByteBuffer MapNewOrExistingCncFile(FileInfo cncFile, bool shouldPreExist,
            int versionFieldOffset, int timestampFieldOffset, long totalFileLength, long timeoutMs,
            IEpochClock epochClock, Action<int> versionCheck, Action<string> logger)
        {
            MappedByteBuffer cncByteBuffer = null;

            try
            {
                cncByteBuffer = IoUtil.MapNewOrExixtingFile(cncFile, totalFileLength);

                UnsafeBuffer cncBuffer = new UnsafeBuffer(cncByteBuffer);

                if (shouldPreExist)
                {
                    int cncVersion = cncBuffer.GetIntVolatile(versionFieldOffset);

                    if (null != logger)
                    {
                        logger("INFO: CnC file exists: " + cncFile);
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
                        throw new System.InvalidOperationException("Active CnC file detected");
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

        public static MappedByteBuffer MapExistingFile(FileInfo cncFile, Action<string> logger, long offset,
            long length)
        {
            if (cncFile.Exists)
            {
                if (null != logger)
                {
                    logger("INFO: CnC file exists: " + cncFile);
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
                    logger("INFO: CnC file exists: " + cncFile);
                }

                return IoUtil.MapExistingFile(cncFile, cncFile.ToString());
            }

            return null;
        }

        public static MappedByteBuffer MapNewFile(FileInfo cncFile, long length)
        {
            return IoUtil.MapNewFile(cncFile, length);
        }

        public static bool IsActive(MappedByteBuffer cncByteBuffer, IEpochClock epochClock, long timeoutMs,
            int versionFieldOffset, int timestampFieldOffset, Action<int> versionCheck, Action<string> logger)
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
                    throw new System.InvalidOperationException("CnC file is created but not initialised.");
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

        internal static void Sleep(int durationMs)
        {
            try
            {
                Thread.Sleep(durationMs);
            }
            catch (ThreadInterruptedException)
            {
            }
        }
    }
}