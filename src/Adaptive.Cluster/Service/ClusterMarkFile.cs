using System;
using System.Diagnostics;
using System.IO;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs.Mark;

namespace Adaptive.Cluster
{
   public class ClusterMarkFile : IDisposable
{
	public const string FILENAME = "cluster-mark.dat";
	public const int ALIGNMENT = 1024;

	private readonly MarkFileHeaderDecoder headerDecoder = new MarkFileHeaderDecoder();
	private readonly MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder();
	private readonly MarkFile markFile;
	private readonly UnsafeBuffer buffer;

	public ClusterMarkFile(FileInfo file, ClusterComponentType type, int totalFileLength, IEpochClock epochClock, long timeoutMs)
	{
		markFile = new MarkFile(file, file.Exists, MarkFileHeaderDecoder.VersionEncodingOffset(), MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(), totalFileLength, timeoutMs, epochClock, (version) =>
		{
			if (version != MarkFileHeaderDecoder.SCHEMA_VERSION)
			{
				throw new ArgumentException("Mark file version " + version + " does not match software:" + MarkFileHeaderDecoder.SCHEMA_VERSION);
			}
		}, null);

		buffer = markFile.Buffer();

		headerEncoder.Wrap(buffer, 0);
		headerDecoder.Wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);

		var existingType = headerDecoder.ComponentType();

		if (existingType != ClusterComponentType.NULL && existingType != type)
		{
			throw new InvalidOperationException("existing Mark file type " + existingType + " not same as required type " + type);
		}

		headerEncoder.ComponentType(type);
		headerEncoder.Pid(Process.GetCurrentProcess().Id);
	}

	public ClusterMarkFile(DirectoryInfo directory, string filename, IEpochClock epochClock, long timeoutMs, Action<string> logger)
	{
		markFile = new MarkFile(directory, filename, MarkFileHeaderDecoder.VersionEncodingOffset(), MarkFileHeaderDecoder.ActivityTimestampEncodingOffset(), timeoutMs, epochClock, (version) =>
		{
			if (version != MarkFileHeaderDecoder.SCHEMA_VERSION)
			{
				throw new ArgumentException("Mark file version " + version + " does not match software:" + MarkFileHeaderDecoder.SCHEMA_VERSION);
			}
		}, logger);

		buffer = markFile.Buffer();
		headerDecoder.Wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);
	}

	public void Dispose()
	{
		markFile?.Dispose();
	}

	public void SignalReady()
	{
		markFile.SignalReady(MarkFileHeaderDecoder.SCHEMA_VERSION);
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

	public static int AlignedTotalFileLength(int alignment, string aeronDirectory, string archiveChannel, string serviceControlChannel, string ingressChannel, string serviceName, string authenticator)
	{
		if (aeronDirectory == null) throw new ArgumentNullException(nameof(aeronDirectory));
		if (archiveChannel == null) throw new ArgumentNullException(nameof(archiveChannel));
		if (serviceControlChannel == null) throw new ArgumentNullException(nameof(serviceControlChannel));

		return BitUtil.Align(MarkFileHeaderEncoder.BLOCK_LENGTH + (6 * VarAsciiEncodingEncoder.LengthEncodingLength()) + aeronDirectory.Length + archiveChannel.Length + serviceControlChannel.Length + (null == ingressChannel ? 0 : ingressChannel.Length) + (null == serviceName ? 0 : serviceName.Length) + (null == authenticator ? 0 : authenticator.Length), alignment);
	}
}

}