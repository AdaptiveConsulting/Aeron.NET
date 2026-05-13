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
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class ChannelUriStringBuilderTest
    {
        // Constants moved here to avoid pulling private nested constants from Aeron.Configuration.
        private const string MaxResendParamName = "max-resend";
        private const string PublicationWindowLengthParamName = "pub-wnd";
        private const string StreamIdParamName = "stream-id";
        private const string UntetheredLingerTimeoutParamName = "untethered-linger-timeout";
        private const string UntetheredRestingTimeoutParamName = "untethered-resting-timeout";
        private const string UntetheredWindowLimitTimeoutParamName = "untethered-window-limit-timeout";
        private const string ResponseCorrelationIdParamName = "response-correlation-id";

        [Test]
        public void ShouldValidateMedia()
        {
            Assert.Throws<ArgumentException>(() => new ChannelUriStringBuilder().Validate());
        }

        [Test]
        public void ShouldValidateEndpointOrControl()
        {
            Assert.Throws<ArgumentException>(() => new ChannelUriStringBuilder().Media("udp").Validate());
        }

        [Test]
        public void ShouldValidateInitialPosition()
        {
            Assert.Throws<ArgumentException>(() =>
                new ChannelUriStringBuilder().Media("udp").Endpoint("address:port").TermId(999).Validate()
            );
        }

        [Test]
        public void ShouldGenerateBasicIpcChannel()
        {
            var builder = new ChannelUriStringBuilder().Media("ipc");
            Assert.AreEqual("aeron:ipc", builder.Build());
        }

        [Test]
        public void ShouldGenerateBasicUdpChannel()
        {
            var builder = new ChannelUriStringBuilder().Media("udp").Endpoint("localhost:9999");
            Assert.AreEqual("aeron:udp?endpoint=localhost:9999", builder.Build());
        }

        [Test]
        public void ShouldGenerateBasicUdpChannelSpy()
        {
            var builder = new ChannelUriStringBuilder().Prefix("aeron-spy").Media("udp").Endpoint("localhost:9999");
            Assert.AreEqual("aeron-spy:aeron:udp?endpoint=localhost:9999", builder.Build());
        }

        [Test]
        public void ShouldGenerateComplexUdpChannel()
        {
            var builder = new ChannelUriStringBuilder()
                .Media("udp")
                .Endpoint("localhost:9999")
                .Ttl(9)
                .TermLength(1024 * 128);
            Assert.AreEqual("aeron:udp?endpoint=localhost:9999|term-length=128k|ttl=9", builder.Build());
        }

        [Test]
        public void ShouldGenerateReplayUdpChannel()
        {
            var builder = new ChannelUriStringBuilder()
                .Media("udp")
                .Endpoint("address:9999")
                .TermLength(1024 * 128)
                .InitialTermId(777)
                .TermId(999)
                .TermOffset(64);
            Assert.AreEqual(
                "aeron:udp?endpoint=address:9999|term-length=128k|init-term-id=777|term-id=999|term-offset=64",
                builder.Build()
            );
        }

        [Test]
        public void ShouldGenerateChannelWithSocketParameters()
        {
            var builder = new ChannelUriStringBuilder()
                .Media("udp")
                .Endpoint("address:9999")
                .SocketSndbufLength(8192)
                .SocketRcvbufLength(4096);
            Assert.AreEqual("aeron:udp?endpoint=address:9999|so-sndbuf=8k|so-rcvbuf=4k", builder.Build());
        }

        [Test]
        public void ShouldGenerateChannelWithReceiverWindow()
        {
            var builder = new ChannelUriStringBuilder()
                .Media("udp")
                .Endpoint("address:9999")
                .ReceiverWindowLength(8192);
            Assert.AreEqual("aeron:udp?endpoint=address:9999|rcv-wnd=8k", builder.Build());
        }

        [Test]
        public void ShouldGenerateChannelWithLingerTimeout()
        {
            const long lingerNs = 987654321123456789L;
            var builder = new ChannelUriStringBuilder().Media("ipc").Linger(lingerNs);

            Assert.AreEqual(lingerNs, builder.Linger());
            Assert.AreEqual("aeron:ipc?linger=987654321123456789ns", builder.Build());
        }

        [Test]
        public void ShouldGenerateChannelWithoutLingerTimeoutIfNullIsPassed()
        {
            var builder = new ChannelUriStringBuilder().Media("udp").Endpoint("address:9999").Linger((long?)null);

            Assert.IsNull(builder.Linger());
            Assert.AreEqual("aeron:udp?endpoint=address:9999", builder.Build());
        }

        [Test]
        public void ShouldRejectNegativeLingerTimeout()
        {
            var exception = Assert.Throws<ArgumentException>(() =>
                new ChannelUriStringBuilder().Media("udp").Endpoint("address:9999").Linger(-1L)
            );
            Assert.AreEqual("`linger` value cannot be negative: -1", exception.Message);
        }

        [Test]
        public void ShouldCopyLingerTimeoutFromChannelUriHumanForm()
        {
            var builder = new ChannelUriStringBuilder();
            builder.Linger(ChannelUri.Parse("aeron:ipc?linger=7200s"));
            Assert.AreEqual(2L * 60 * 60 * 1_000_000_000, builder.Linger());
        }

        [Test]
        public void ShouldCopyLingerTimeoutFromChannelUriNanoseconds()
        {
            var builder = new ChannelUriStringBuilder();
            builder.Linger(ChannelUri.Parse("aeron:udp?linger=19191919191919191"));
            Assert.AreEqual(19191919191919191L, builder.Linger());
        }

        [Test]
        public void ShouldCopyLingerTimeoutFromChannelUriNoValue()
        {
            var builder = new ChannelUriStringBuilder();
            builder.Linger(ChannelUri.Parse("aeron:udp?endpoint=localhost:8080"));
            Assert.IsNull(builder.Linger());
        }

        [Test]
        public void ShouldCopyLingerTimeoutFromChannelUriNegativeValue()
        {
            var channelUri = ChannelUri.Parse("aeron:udp?linger=-1000");
            Assert.Throws<ArgumentException>(() => new ChannelUriStringBuilder().Linger(channelUri));
        }

        [Test]
        public void ShouldRejectInvalidOffsets()
        {
            Assert.Throws<ArgumentException>(() =>
                new ChannelUriStringBuilder().MediaReceiveTimestampOffset("breserved")
            );
            Assert.Throws<ArgumentException>(() =>
                new ChannelUriStringBuilder().ChannelReceiveTimestampOffset("breserved")
            );
            Assert.Throws<ArgumentException>(() =>
                new ChannelUriStringBuilder().ChannelSendTimestampOffset("breserved")
            );
        }

        [Test]
        public void ShouldRejectInvalidNakDelay()
        {
            // .NET throws FormatException (its parsing-error convention) where Java throws NumberFormatException.
            // Test passes either, since both indicate "could not parse".
            Assert.Catch(() => new ChannelUriStringBuilder().NakDelay("foo"));
        }

        [Test]
        public void ShouldHandleNakDelayWithUnits()
        {
            Assert.AreEqual(1000L, new ChannelUriStringBuilder().NakDelay("1us").NakDelay());
            Assert.AreEqual(1L, new ChannelUriStringBuilder().NakDelay("1ns").NakDelay());
            Assert.AreEqual(1000000L, new ChannelUriStringBuilder().NakDelay("1ms").NakDelay());
        }

        [Test]
        public void ShouldHandleUntetheredWindowLimitTimeoutWithUnits()
        {
            Assert.AreEqual(
                1000L,
                new ChannelUriStringBuilder().UntetheredWindowLimitTimeout("1us").UntetheredWindowLimitTimeoutNs()
            );
            Assert.AreEqual(
                1L,
                new ChannelUriStringBuilder().UntetheredWindowLimitTimeout("1ns").UntetheredWindowLimitTimeoutNs()
            );
            Assert.AreEqual(
                1000000L,
                new ChannelUriStringBuilder().UntetheredWindowLimitTimeout("1ms").UntetheredWindowLimitTimeoutNs()
            );
        }

        [Test]
        public void ShouldHandleUntetheredRestingTimeoutWithUnits()
        {
            Assert.AreEqual(
                1000L,
                new ChannelUriStringBuilder().UntetheredRestingTimeout("1us").UntetheredRestingTimeoutNs()
            );
            Assert.AreEqual(
                1L,
                new ChannelUriStringBuilder().UntetheredRestingTimeout("1ns").UntetheredRestingTimeoutNs()
            );
            Assert.AreEqual(
                1000000L,
                new ChannelUriStringBuilder().UntetheredRestingTimeout("1ms").UntetheredRestingTimeoutNs()
            );
        }

        [Test]
        public void ShouldHandleMaxRetransmits()
        {
            Assert.AreEqual(20, new ChannelUriStringBuilder().MaxResend(20).MaxResend());
            Assert.IsTrue(new ChannelUriStringBuilder().MaxResend(20).Build().Contains(MaxResendParamName + "=20"));
            Assert.AreEqual(
                30,
                new ChannelUriStringBuilder()
                    .MaxResend(ChannelUri.Parse(new ChannelUriStringBuilder().MaxResend(30).Build()))
                    .MaxResend()
            );
        }

        [Test]
        public void ShouldHandleStreamId()
        {
            Assert.IsNull(new ChannelUriStringBuilder().StreamId());

            const int streamId = 1234;
            Assert.AreEqual(streamId, new ChannelUriStringBuilder().StreamId(streamId).StreamId());

            string uri = new ChannelUriStringBuilder().StreamId(streamId).Build();
            Assert.AreEqual(streamId.ToString(), ChannelUri.Parse(uri).Get(StreamIdParamName));
        }

        [Test]
        public void ShouldRejectInvalidStreamId()
        {
            var uri = ChannelUri.Parse("aeron:ipc?stream-id=abc");
            Assert.Throws<ArgumentException>(() => new ChannelUriStringBuilder().StreamId(uri));
        }

        [Test]
        public void ShouldHandlePublicationWindowLength()
        {
            Assert.IsNull(new ChannelUriStringBuilder().PublicationWindowLength());

            const int pubWindowLength = 7777;
            Assert.AreEqual(
                pubWindowLength,
                new ChannelUriStringBuilder().PublicationWindowLength(pubWindowLength).PublicationWindowLength()
            );

            string uri = new ChannelUriStringBuilder().PublicationWindowLength(pubWindowLength).Build();
            Assert.AreEqual(
                pubWindowLength.ToString(),
                ChannelUri.Parse(uri).Get(PublicationWindowLengthParamName)
            );
        }

        [TestCase("abc")]
        [TestCase("1000000000000")]
        public void ShouldRejectInvalidPublicationWindowLength(string pubWnd)
        {
            var uri = ChannelUri.Parse("aeron:ipc");
            uri.Put(PublicationWindowLengthParamName, pubWnd);
            // .NET throws FormatException for "abc", OverflowException for the too-big value.
            Assert.Catch(() => new ChannelUriStringBuilder().PublicationWindowLength(uri));
        }

        [TestCase("this.will.not.work")]
        [TestCase("-2")]
        public void ShouldThrowAnExceptionOnInvalidResponseCorrelationId(string responseCorrelationId)
        {
            var channelUri = ChannelUri.Parse(
                "aeron:udp?" + ResponseCorrelationIdParamName + "=" + responseCorrelationId
            );
            Assert.Throws<ArgumentException>(() => new ChannelUriStringBuilder().ResponseCorrelationId(channelUri));
        }

        [TestCase("prototype")]
        [TestCase("2")]
        public void ShouldNotThrowAnExceptionOnValidResponseCorrelationId(string responseCorrelationId)
        {
            var channelUri = ChannelUri.Parse(
                "aeron:udp?" + ResponseCorrelationIdParamName + "=" + responseCorrelationId
            );
            Assert.DoesNotThrow(() => new ChannelUriStringBuilder().ResponseCorrelationId(channelUri));
        }

        [Test]
        public void ShouldBuildChannelBuilderUsingExistingStringWithAllTheFields()
        {
            const string uri =
                "aeron-spy:aeron:udp?endpoint=127.0.0.1:0|interface=127.0.0.1|control=127.0.0.2:0|"
                + "control-mode=manual|tags=2,4|alias=foo|cc=cubic|fc=min|reliable=false|ttl=16|mtu=8992|"
                + "term-length=1m|init-term-id=5|term-offset=64|term-id=4353|session-id=2314234|gtag=3|"
                + "linger=100000055000001ns|sparse=true|eos=true|tether=false|group=false|ssc=true|so-sndbuf=8m|"
                + "so-rcvbuf=2m|rcv-wnd=1m|media-rcv-ts-offset=reserved|channel-rcv-ts-offset=0|"
                + "channel-snd-ts-offset=8|response-endpoint=127.0.0.3:0|response-correlation-id=12345|nak-delay=100us|"
                + "untethered-window-limit-timeout=1us|untethered-resting-timeout=5us|stream-id=87|pub-wnd=10224";

            var fromString = ChannelUri.Parse(uri);
            var fromBuilder = ChannelUri.Parse(new ChannelUriStringBuilder(uri).Build());

            CollectionAssert.IsEmpty(fromString.Diff(fromBuilder));
        }

        [Test]
        public void ShouldBuildChannelBuilderUsingExistingStringWithTaggedSessionIdAndIpc()
        {
            const string uri = "aeron:ipc?session-id=tag:123456";

            var fromString = ChannelUri.Parse(uri);
            var fromBuilder = ChannelUri.Parse(new ChannelUriStringBuilder(uri).Build());

            CollectionAssert.IsEmpty(fromString.Diff(fromBuilder));
        }

        [TestCase(1000L, 666L, 2002L)]
        [TestCase(50L, 40L, 30L)]
        public void ShouldHandleUntetheredParameters(
            long untetheredWindowLimitTimeoutNs,
            long untetheredLingerTimeoutNs,
            long untetheredRestingTimeoutNs
        )
        {
            var builder = new ChannelUriStringBuilder("aeron:ipc")
                .UntetheredWindowLimitTimeoutNs(untetheredWindowLimitTimeoutNs)
                .UntetheredLingerTimeoutNs(untetheredLingerTimeoutNs)
                .UntetheredRestingTimeoutNs(untetheredRestingTimeoutNs);

            Assert.AreEqual(untetheredWindowLimitTimeoutNs, builder.UntetheredWindowLimitTimeoutNs());
            Assert.AreEqual(untetheredLingerTimeoutNs, builder.UntetheredLingerTimeoutNs());
            Assert.AreEqual(untetheredRestingTimeoutNs, builder.UntetheredRestingTimeoutNs());

            var uri = ChannelUri.Parse(builder.Build());
            Assert.AreEqual(
                Adaptive.Agrona.SystemUtil.FormatDuration(untetheredWindowLimitTimeoutNs),
                uri.Get(UntetheredWindowLimitTimeoutParamName)
            );
            Assert.AreEqual(
                Adaptive.Agrona.SystemUtil.FormatDuration(untetheredLingerTimeoutNs),
                uri.Get(UntetheredLingerTimeoutParamName)
            );
            Assert.AreEqual(
                Adaptive.Agrona.SystemUtil.FormatDuration(untetheredRestingTimeoutNs),
                uri.Get(UntetheredRestingTimeoutParamName)
            );
        }

        [Test]
        public void ShouldFormatSizeAndDurationsWhenCreatingChannelString()
        {
            const long microsecond = 1000L;
            const long millisecond = 1_000_000L;
            const long second = 1_000_000_000L;

            string channel = new ChannelUriStringBuilder()
                .Media("udp")
                .Endpoint("localhost:5050")
                .ReceiverWindowLength(1024)
                .Mtu(8192)
                .TermLength(4 * 1024 * 1024)
                .SocketSndbufLength(64 * 1024)
                .SocketRcvbufLength(32 * 1024)
                .PublicationWindowLength(1024 * 1024)
                .UntetheredWindowLimitTimeoutNs(100 * microsecond)
                .UntetheredLingerTimeoutNs(3 * millisecond)
                .UntetheredRestingTimeoutNs(1 * second)
                .Linger(50 * millisecond)
                .NakDelay(123456789L)
                .MaxResend(1000)
                .Tether(true)
                .Rejoin(false)
                .StreamId(-87)
                .InitialPosition(17 * 1024 * 1024, -9, 4 * 1024 * 1024)
                .Build();

            CollectionAssert.IsEmpty(
                ChannelUri
                    .Parse(channel)
                    .Diff(
                        ChannelUri.Parse(
                            "aeron:udp?endpoint=localhost:5050|mtu=8k|term-length=4m|rcv-wnd=1k|so-sndbuf=64k|"
                                + "so-rcvbuf=32k|pub-wnd=1m|untethered-linger-timeout=3ms|"
                                + "untethered-window-limit-timeout=100us|"
                                + "untethered-resting-timeout=1s|linger=50ms|nak-delay=123456789ns|"
                                + "max-resend=1000|rejoin=false|"
                                + "tether=true|stream-id=-87|term-id=-5|init-term-id=-9|term-offset=1048576"
                        )
                    )
            );
        }
    }
}
