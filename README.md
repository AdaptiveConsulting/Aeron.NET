# Aeron.NET
[![NuGet](https://img.shields.io/nuget/v/Adaptive.Aeron.svg?maxAge=2592000)](https://www.nuget.org/packages/Adaptive.Aeron/)

A .NET port of the [Aeron Client](https://github.com/real-logic/Agrona).

Aeron is an efficient reliable UDP unicast, UDP multicast, and IPC message transport.

Performance is the key focus. Aeron is designed to have the highest throughput with the lowest and most predictable latency possible of any messaging system. 
Aeron is designed not to perform any allocations after the initial set-up, this means less time will be spent garbage collecting and as a result, latency will be kept down.

### Getting Started
Aeron comes in two parts: the media driver and the client.

![Architecture Overview](Overview.png?raw=true "Overview")

#### Media Driver
The driver runs in its own process and communicates with the client directly via shared memory. It sends and receives messages across the network to other drivers or routes messages to other clients on the same host.

To run the media driver, you will need [Java 8 JRE](http://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html) installed. Then run:

    scripts/start-media-driver.bat


#### Client  
The client can be [built from source](#building-from-source) or installed from nuget:
https://www.nuget.org/packages/Adaptive.Aeron/

    PM> Install-Package Adaptive.Aeron

### Usage

#### Publisher
Used to send messages to a specified channel & stream.
```csharp
using(Aeron aeron = Aeron.Connect())
using(Publication publication = aeron.AddPublisher("aeron:ipc", 1)) 
using(UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]) {
  byte[] message = Encoding.UTF8.GetBytes("Hello World!");
  buffer.PutBytes(0, message);
  publisher.Offer(buffer, 0, message.Length);
}
```
#### Fragment Handler
A fragment handler is used for processing data that is has been received. The buffer will either contain a whole message or a fragment of a message to be reassembled.
```csharp
FragmentHandler messageHandler = (buffer, offset, length, header) => {
  var data = new byte[length];
  buffer.GetBytes(offset, data);
  Console.WriteLine(Encoding.UTF8.GetString(data));
};
```

#### Subscriber
A subscriber is used to register interest in messages from a publisher on a specific channel & stream. It uses a fragment handler to process the received messages.
```csharp
using(Aeron aeron = Aeron.Connect())
using(Subscription subscription = aeron.AddSubscription("aeron:ipc", 1)) 
using(UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]) {
  while(running) {  
      var fragmentsRead = subscription.Poll(fragmentHandler, fragmentLimitCount);
  }
}
```

### Samples
Here are some of the samples that come with Aeron.
Before running the samples, they need to be built (you will need [Visual Studio 2015](https://www.visualstudio.com/en-us/downloads/download-visual-studio-vs.aspx) installed). Run:

    scripts/build.bat

#### Hello World
This samples sends a hello world message, make sure the driver is running. Start by running the subscriber, this causes the media driver to listen on endpoint `udp://localhost:40123` for publications on channel 10 and prints any messages it receives. Make sure the [media driver](#media-driver) is running and run the following batch script to launch the subscriber:

    scripts/subsciber.bat
    
To send the message run the publisher.

    scripts/publisher.bat
    
You should see something like:

```
Subscribing to udp://localhost:40123 on stream Id 10
Received message (Hello World! ) to stream 10 from session 68fa30b2 term id 1804bb00 term offset 0 (13@32)
Press any key...
```

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

![Latency Histogram](Histogram.png?raw=true "Latency Histogram")

### Building from Source
You will need [Visual Studio 2015](https://www.visualstudio.com/en-us/downloads/download-visual-studio-vs.aspx) installed. 

1. Open `src/Adaptive.Aeron.sln`. 
2. Click `Build -> Build Solution`.

### More Information
The best place for more information is the [Aeron Wiki](https://github.com/real-logic/Aeron/wiki)

[![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/real-logic/Aeron?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) To chat with other Aeron users and the contributors.
