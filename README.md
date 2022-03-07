# Aeron.NET
[![TravisCI](https://travis-ci.org/AdaptiveConsulting/Aeron.NET.svg?branch=master)](https://travis-ci.org/AdaptiveConsulting/Aeron.NET)
[![AppVeyor](https://ci.appveyor.com/api/projects/status/rqomfiby1rl7xe2y/branch/master?svg=true)](https://ci.appveyor.com/project/Adaptive/aeron-net/branch/master)
[![GitHub Actions](https://github.com/AdaptiveConsulting/Aeron.NET/workflows/build/badge.svg)](https://github.com/AdaptiveConsulting/Aeron.NET/actions?query=workflow%3Abuild)
[![NuGet](https://img.shields.io/nuget/v/Aeron.Client.svg)](https://www.nuget.org/packages/Aeron.Client/)

A .NET port of the [Aeron Client](https://github.com/real-logic/Aeron).

Aeron is an efficient reliable UDP unicast, UDP multicast, and IPC message transport.

Performance is the key focus. Aeron is designed to have the highest throughput with the lowest and most predictable latency possible of any messaging system. 
Aeron is designed not to perform any allocations after the initial set-up, this means less time will be spent garbage collecting and as a result, latency will be kept down.

### Getting Started
Aeron comes in two parts: the media driver and the client.

![Architecture Overview](images/Overview.png?raw=true "Overview")

#### Media Driver
The driver runs in its own process and communicates with the client directly via shared memory. It sends and receives messages across the network to other drivers or routes messages to other clients on the same host.

To run the media driver, you will need [Java 8 JRE](http://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html) installed. 

There is a nuget package that contains the driver:

    PM> Install-Package Aeron.Driver

It will create a directory in your project with a bat file to launch the driver.

Or if you've got the source, run:

    driver/start-media-driver.bat

#### Client  
The client can be [built from source](#building-from-source) or installed from nuget:
https://www.nuget.org/packages/Aeron.Client/

    PM> Install-Package Aeron.Client

### Usage
The full example is [here](src/Samples/Adaptive.Aeron.Samples.HelloWorld/HelloWorld.cs).

#### Publisher
Used to send messages to a specified channel & stream.
```csharp
const string channel = "aeron:ipc";
const int streamId = 42;

UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

using(Aeron aeron = Aeron.Connect())
using(Publication publisher = aeron.AddPublication(channel, streamId)) {
  int messageLength = buffer.PutStringWithoutLengthUtf8(0, "Hello World!");
  publisher.Offer(buffer, 0, messageLength);
}
```
#### Fragment Handler
A fragment handler is a delegate used for processing data that is has been received. The buffer will either contain a whole message or a fragment of a message to be reassembled.
```csharp
static void PrintMessage(IDirectBuffer buffer, int offset, int length, Header header)
{
  var message = buffer.GetStringWithoutLengthUtf8(offset, length);
  Console.WriteLine($"Message Received: '{message}'");
}
```

#### Subscriber
A subscriber is used to register interest in messages from a publisher on a specific channel & stream. It uses a fragment handler to process the received messages.
```csharp
using(Subscription subscriber = aeron.AddSubscription(channel, streamId)) {
  while(subscriber.Poll(PrintMessage, 1) == 0) {  
      Thread.Sleep(10);
  }
}
```

### Samples
Here are some of the samples that come with Aeron.
Before running the samples, they need to be built (you will need [Visual Studio 2017](https://www.visualstudio.com/en-us/downloads/download-visual-studio-vs.aspx) installed). Run:

    scripts/build.bat

#### Hello World
This samples sends and receives a hello world message. Make sure the [media driver](#media-driver) is running and run the following batch script:

    scripts/hello-world.bat
    
You should see something like:

```
Received message (Hello World!) to stream 42 from session 42d2f651 term id de97c9e0 term offset 0 (11@32)
Press any key to continue...
```

The source code is [here](src/Samples/Adaptive.Aeron.Samples.HelloWorld/HelloWorld.cs).

#### Throughput
This sample shows the overall throughput of the client and driver. It sends messages via `aeron:ipc` which is designed for interprocess communication. Make sure the [media driver](#media-driver) is running and run the sample:

    scripts/throughput.bat

It runs 2 threads which publish and subscribe 32-byte messages and every second prints out the number of messages & total number of bytes that were sent/received.
        
```
Duration 1,001ms - 14,047,600 messages - 449,523,200 bytes
Duration 1,000ms - 14,031,801 messages - 449,017,632 bytes
Duration 1,001ms - 15,054,055 messages - 481,729,760 bytes
Duration 1,000ms - 14,678,982 messages - 469,727,424 bytes
```

The source code is [here](src/Samples/Adaptive.Aeron.Samples.IpcThroughput/IpcThroughput.cs).
    
#### Ping/Pong
This sample show the latencies for a batch of messages.  Make sure the [media driver](#media-driver) is running and start `pong` which listens for messages and will reply back to each message:

    scripts/pong.bat
    
Then start `ping` which send messages with the current time to `pong` and then records the latency when it receives a response to that message.

    scripts/ping.bat
    
After 1,000,000 messages have been sent, you'll be presented with a histogram.

```
Histogram of RTT latencies in microseconds.
       Value     Percentile TotalCount 1/(1-Percentile)

       9.391 0.000000000000          1           1.00
      11.383 0.100000000000      11404           1.11
      11.663 0.200000000000      25421           1.25
      11.951 0.300000000000      43399           1.43
      11.951 0.400000000000      43399           1.67
      15.647 0.500000000000      52070           2.00
      15.935 0.550000000000      57140           2.22
      16.215 0.600000000000      64190           2.50
      16.511 0.650000000000      68864           2.86
      17.071 0.700000000000      71236           3.33
      17.647 0.750000000000      82369           4.00
      17.647 0.775000000000      82369           4.44
      17.647 0.800000000000      82369           5.00
      17.935 0.825000000000      90885           5.71
...
    5013.503 0.999989318848      99999       93622.86
    5021.695 0.999990844727     100000      109226.67
    5021.695 1.000000000000     100000
#[Mean    =       15.195, StdDeviation   =       46.974]
#[Max     =     5021.695, Total count    =       100000]
#[Buckets =           24, SubBuckets     =         2048]
```

Which you can upload to http://hdrhistogram.github.io/HdrHistogram/plotFiles.html to create a nice chart:

![Latency Histogram](images/Histogram.png?raw=true "Latency Histogram")

The source code is [here](src/Samples/Adaptive.Aeron.Samples.Ping/Ping.cs) and [here](src/Samples/Adaptive.Aeron.Samples.Pong/Pong.cs).

### Building from Source
You will need [Visual Studio 2017 Update 3](https://www.visualstudio.com/en-us/downloads/download-visual-studio-vs.aspx) installed. 
Also, as the tooling hasn't caught up yet you'll need the last [.NET Core SDK](https://www.microsoft.com/net/download/core)

1. Open `src/Adaptive.Aeron.sln`. 
2. Click `Build -> Build Solution`.

Note: For best performance, build in x64 release mode and run without the debugger attached.

### Running tests
As Aeron.NET now supports multitargeting, the only way to properly run tests for all framework versions is from dotnet cli issuing `dotnet test` on the test project folders, Visual Studio 2017 will only detect one project when launching the test runner

### Packing NuGets
Run `dotnet pack src\Adaptive.Aeron\Adaptive.Aeron.csproj /p:Configuration=Release`

If there are also changes in Agrona run `src\Adaptive.Agrona\Adaptive.Agrona.csproj /p:Configuration=Release` Aeron depends on it so both should be published at the same time

### More Information
The best place for more information is the [Aeron Wiki](https://github.com/real-logic/Aeron/wiki)

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/real-logic/Aeron?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) To chat with other Aeron users and the contributors.

### Sponsors
Many thanks to our **premium sponsors**!

<p align="left">
<a href="https://www.tickmill.com/" target=_blank>
    <img src="https://raw.githubusercontent.com/AdaptiveConsulting/Aeron.NET/master/.github/assets/sponsors/tickmill.png"/>
</a>
<a href="https://www.rwe.com/" target=_blank>
    <img src="https://raw.githubusercontent.com/AdaptiveConsulting/Aeron.NET/master/.github/assets/sponsors/rwe.png"/>
</a>
</p>
