using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.ConstrainedExecution;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona.Concurrent;
using log4net;

namespace Adaptive.Aeron.Driver.Native.Tests
{
    public class AeronConnection : CriticalFinalizerObject, IDisposable
    {
        private static readonly ILog _log = LogManager.GetLogger(typeof(AeronConnection));

        public readonly Aeron Aeron;
        private readonly MediaDriver _driver;

        public bool IsRunning => !Aeron.IsClosed;

        public AeronConnection(MediaDriverConfig config)
        {
            _driver = MediaDriver.Start(config);

            var aeronContext = new Adaptive.Aeron.Aeron.Context()
                               .AvailableImageHandler(i => ImageAvailable?.Invoke(i))
                               .UnavailableImageHandler(i => ImageUnavailable?.Invoke(i))
                               .AeronDirectoryName(config.Dir)
                               .DriverTimeoutMs(Debugger.IsAttached ? 120 * 60 * 1000 : 30_000)
                               .ErrorHandler(OnError)
                               .ResourceLingerDurationNs(0)
                ;

            Aeron = Adaptive.Aeron.Aeron.Connect(aeronContext);
        }

        private void OnError(Exception exception)
        {
            _log.Error("Aeron connection error", exception);

            if (Aeron.IsClosed)
                return;

            switch (exception)
            {
                case AeronException _:
                case AgentTerminationException _:
                    _log.Error("Unrecoverable Media Driver error");
                    TerminatedUnexpectedly?.Invoke();
                    break;
            }
        }

        public event Action<Image>? ImageAvailable;
        public event Action<Image>? ImageUnavailable;

        public event Action? TerminatedUnexpectedly;

        public int GetNewClientStreamId()
        {
            while (true)
            {
                var streamId = _driver.GetNewClientStreamId();
                if (streamId != 0 && streamId != 1)
                {
                    return streamId;
                }
            }
        }

        public void Dispose()
        {
            Aeron.Dispose();
            _driver?.Dispose();
        }
    }
    
    public static class AeronUtils
    {
        private static readonly ILog _log = LogManager.GetLogger(typeof(AeronUtils));

        public const byte CurrentProtocolVersion = 1;

        public const string IpcChannel = "aeron:ipc";

        public static string RemoteChannel(string host, int port)
            => $"aeron:udp?endpoint={host}:{port}";

        public static string GroupIdToPath(string groupId)
        {
            if (string.IsNullOrEmpty(groupId))
                throw new ArgumentException("Empty group id");

            return Path.Combine(Path.GetTempPath(), $"Aeron-{groupId}");
        }

        public static AeronResultType InterpretPublicationOfferResult(long errorCode)
        {
            if (errorCode >= 0)
                return AeronResultType.Success;

            switch (errorCode)
            {
                case Publication.BACK_PRESSURED:
                case Publication.ADMIN_ACTION:
                    return AeronResultType.ShouldRetry;

                case Publication.NOT_CONNECTED:
                    _log.Error("Trying to send to an unconnected publication");
                    return AeronResultType.Error;

                case Publication.CLOSED:
                    _log.Error("Trying to send to a closed publication");
                    return AeronResultType.Error;

                case Publication.MAX_POSITION_EXCEEDED:
                    _log.Error("Max position exceeded");
                    return AeronResultType.Error;

                default:
                    _log.Error($"Unknown error code: {errorCode}");
                    return AeronResultType.Error;
            }
        }
        
        /// <summary>
        /// Get local IP address that OS chooses for the remote destination.
        /// This should be more reliable than iterating over local interfaces
        /// and trying to guess the right one.
        /// </summary>
        public static string GetLocalIPAddress(string serverHost, int serverPort)
        {
            using Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, 0);
            socket.Connect(serverHost, serverPort);
            if (socket.LocalEndPoint is IPEndPoint endPoint)
                return endPoint.Address.ToString();
            throw new InvalidOperationException("socket.LocalEndPoint is not IPEndPoint");
        }
    }
    
    public enum AeronResultType
    {
        Error,
        ShouldRetry,
        Success
    }
}