using System;
using System.IO;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Errors;
using Adaptive.Agrona.Concurrent.RingBuffer;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// This class provides the Media Driver and client with common configuration for the Aeron directory.
    /// <para>
    /// Properties
    /// <ul>
    /// <li><code>aeron.dir</code>: Use value as directory name for Aeron buffers and status.</li>
    /// </ul>
    /// </para>
    /// </summary>
    public class CommonContext : IDisposable
    {
        /// <summary>
        /// The top level Aeron directory used for communication between a Media Driver and client.
        /// </summary>
        public const string AERON_DIR_PROP_NAME = "aeron.dir";

        /// <summary>
        /// The value of the top level Aeron directory unless overridden by <seealso cref="AeronDirectoryName(String)"/>
        /// </summary>
        public static readonly string AERON_DIR_PROP_DEFAULT;

        /// <summary>
        /// URI used for IPC <seealso cref="Publication"/>s and <seealso cref="Subscription"/>s
        /// </summary>
        public const string IPC_CHANNEL = "aeron:ipc";

        /// <summary>
        /// Timeout in which the driver is expected to respond.
        /// </summary>
        public const long DEFAULT_DRIVER_TIMEOUT_MS = 10000;

        protected long _driverTimeoutMs = DEFAULT_DRIVER_TIMEOUT_MS;
        private string _aeronDirectoryName;
        private FileInfo _cncFile;
        private UnsafeBuffer _countersMetaDataBuffer;
        protected UnsafeBuffer _countersValuesBuffer;

        static CommonContext()
        {
            string baseDirName = IoUtil.TmpDirName() + "aeron";

            // Use shared memory on Linux to avoid contention on the page cache.
            //if ("Linux".Equals(System.GetProperty("os.name"), StringComparison.CurrentCultureIgnoreCase))
            //{
            //    File devShmDir = new File("/dev/shm");

            //    if (devShmDir.Exists())
            //    {
            //        baseDirName = "/dev/shm/aeron";
            //    }
            //}

            AERON_DIR_PROP_DEFAULT = baseDirName + "-" + Environment.UserName;
        }

        /// <summary>
        /// Convert the default Aeron directory name to be a random name for use with embedded drivers.
        /// </summary>
        /// <returns> random directory name with default directory name as base </returns>
        public static string GenerateRandomDirName()
        {
            string randomDirName = Guid.NewGuid().ToString();

            return AERON_DIR_PROP_DEFAULT + "-" + randomDirName;
        }

        /// <summary>
        /// Create a new context with Aeron directory and delete on exit values based on the current system properties.
        /// </summary>
        public CommonContext()
        {
            // TODO
            _aeronDirectoryName = "todoMakeDirecotyrConfigurable"; // GetProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT);
        }

        /// <summary>
        /// This completes initialization of the CommonContext object. It is automatically called by subclasses.
        /// </summary>
        /// <returns> this Object for method chaining. </returns>
        public CommonContext Conclude()
        {
            _cncFile = new FileInfo(Path.Combine(_aeronDirectoryName, CncFileDescriptor.CNC_FILE));
            return this;
        }

        /// <summary>
        /// Get the top level Aeron directory used for communication between the client and Media Driver, and
        /// the location of the data buffers.
        /// </summary>
        /// <returns> The top level Aeron directory. </returns>
        public string AeronDirectoryName()
        {
            return _aeronDirectoryName;
        }

        /// <summary>
        /// Set the top level Aeron directory used for communication between the client and Media Driver, and the location
        /// of the data buffers.
        /// </summary>
        /// <param name="dirName"> New top level Aeron directory. </param>
        /// <returns> this Object for method chaining. </returns>
        public CommonContext AeronDirectoryName(string dirName)
        {
            _aeronDirectoryName = dirName;
            return this;
        }

        /// <summary>
        /// Create a new command and control file in the administration directory.
        /// </summary>
        /// <returns> The newly created File. </returns>
        public static FileInfo NewDefaultCncFile()
        {
            // TODO make this configurable 
            // return new FileInfo(GetProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT), CncFileDescriptor.CNC_FILE);
            return new FileInfo(Path.Combine(AERON_DIR_PROP_DEFAULT, CncFileDescriptor.CNC_FILE));
        }

        /// <summary>
        /// Get the buffer containing the counter meta data.
        /// </summary>
        /// <returns> The buffer storing the counter meta data. </returns>
        public UnsafeBuffer CountersMetaDataBuffer()
        {
            return _countersMetaDataBuffer;
        }

        /// <summary>
        /// Set the buffer containing the counter meta data.
        /// </summary>
        /// <param name="countersMetaDataBuffer"> The new counter meta data buffer. </param>
        /// <returns> this Object for method chaining. </returns>
        public CommonContext CountersMetaDataBuffer(UnsafeBuffer countersMetaDataBuffer)
        {
            this._countersMetaDataBuffer = countersMetaDataBuffer;
            return this;
        }

        /// <summary>
        /// Get the buffer containing the counters.
        /// </summary>
        /// <returns> The buffer storing the counters. </returns>
        public UnsafeBuffer CountersValuesBuffer()
        {
            return _countersValuesBuffer;
        }

        /// <summary>
        /// Set the buffer containing the counters
        /// </summary>
        /// <param name="countersValuesBuffer"> The new counters buffer. </param>
        /// <returns> this Object for method chaining. </returns>
        public CommonContext CountersValuesBuffer(UnsafeBuffer countersValuesBuffer)
        {
            this._countersValuesBuffer = countersValuesBuffer;
            return this;
        }

        /// <summary>
        /// Get the command and control file.
        /// </summary>
        /// <returns> The command and control file. </returns>
        public FileInfo CncFile()
        {
            return _cncFile;
        }

        /// <summary>
        /// Set the driver timeout in milliseconds
        /// </summary>
        /// <param name="driverTimeoutMs"> to indicate liveness of driver </param>
        /// <returns> driver timeout in milliseconds </returns>
        public CommonContext DriverTimeoutMs(long driverTimeoutMs)
        {
            this._driverTimeoutMs = driverTimeoutMs;
            return this;
        }

        /// <summary>
        /// Get the driver timeout in milliseconds.
        /// </summary>
        /// <returns> driver timeout in milliseconds. </returns>
        public long DriverTimeoutMs()
        {
            return _driverTimeoutMs;
        }

        /// <summary>
        /// Delete the current Aeron directory, throwing errors if not possible.
        /// </summary>
	    public void DeleteAeronDirectory()
        {
            Directory.Delete(_aeronDirectoryName, true);
        }

        /// <summary>
        /// Is a media driver active in the current Aeron directory?
        /// </summary>
        /// <param name="driverTimeoutMs"> for the driver liveness check </param>
        /// <param name="logHandler">      for feedback as liveness checked </param>
        /// <returns> true if a driver is active or false if not </returns>
        public bool IsDriverActive(long driverTimeoutMs, Action<string> logHandler)
        {
            if (Directory.Exists(_aeronDirectoryName))
            {
                var cncFilePath = Path.Combine(_aeronDirectoryName, CncFileDescriptor.CNC_FILE);

                logHandler($"INFO: Aeron directory {_aeronDirectoryName} exists");

                if (File.Exists(cncFilePath))
                {
                    MappedByteBuffer cncByteBuffer = null;

                    logHandler($"INFO: Aeron CnC file {cncFilePath} exists");

                    try
                    {
                        cncByteBuffer = IoUtil.MapExistingFile(cncFilePath, CncFileDescriptor.CNC_FILE);
                        var cncMetaDataBuffer = CncFileDescriptor.CreateMetaDataBuffer(cncByteBuffer);

                        var cncVersion = cncMetaDataBuffer.GetInt(CncFileDescriptor.CncVersionOffset(0));

                        if (CncFileDescriptor.CNC_VERSION != cncVersion)
                        {
                            throw new InvalidOperationException("aeron cnc file version not understood: version=" + cncVersion);
                        }

                        var toDriverBuffer = new ManyToOneRingBuffer(CncFileDescriptor.CreateToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

                        long timestamp = toDriverBuffer.ConsumerHeartbeatTime();
                        long now = UnixTimeConverter.CurrentUnixTimeMillis();
                        long diff = now - timestamp;

                        logHandler($"INFO: Aeron toDriver consumer heartbeat is {diff:D} ms old");

                        if (diff <= driverTimeoutMs)
                        {
                            return true;
                        }
                    }
                    finally
                    {
                        IoUtil.Unmap(cncByteBuffer);
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Read the error log to a given <seealso cref="StreamWriter"/>
        /// </summary>
        /// <param name="stream"> to write the error log contents to. </param>
        /// <returns> the number of observations from the error log </returns>
        public int SaveErrorLog(StreamWriter stream)
        {
            int result = 0;

            if (Directory.Exists(_aeronDirectoryName))
            {
                var cncFilePath = Path.Combine(_aeronDirectoryName, CncFileDescriptor.CNC_FILE);

                if (File.Exists(cncFilePath))
                {
                    MappedByteBuffer cncByteBuffer = null;

                    try
                    {
                        cncByteBuffer = IoUtil.MapExistingFile(cncFilePath, CncFileDescriptor.CNC_FILE);
                        var cncMetaDataBuffer = CncFileDescriptor.CreateMetaDataBuffer(cncByteBuffer);

                        int cncVersion = cncMetaDataBuffer.GetInt(CncFileDescriptor.CncVersionOffset(0));

                        if (CncFileDescriptor.CNC_VERSION != cncVersion)
                        {
                            throw new InvalidOperationException("aeron cnc file version not understood: version=" + cncVersion);
                        }

                        var buffer = CncFileDescriptor.CreateErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer);

                        int distinctErrorCount = ErrorLogReader.Read(
                            buffer, 
                            (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) 
                                => stream.Write("***\n{0} observations from {1} to {2} for:\n {3}\n", 
                                observationCount, 
                                UnixTimeConverter.FromUnixTimeMillis(firstObservationTimestamp).ToString("yyyy-MM-dd HH:mm:ss.SSSZ"),
                                UnixTimeConverter.FromUnixTimeMillis(lastObservationTimestamp).ToString("yyyy-MM-dd HH:mm:ss.SSSZ"),
                                encodedException));

                        stream.Write("\n{0} distinct errors observed.\n", distinctErrorCount);

                        result = distinctErrorCount;
                    }
                    finally
                    {
                        IoUtil.Unmap(cncByteBuffer);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Release resources used by the CommonContext.
        /// </summary>
        public virtual void Dispose()
        {
        }
    }
}