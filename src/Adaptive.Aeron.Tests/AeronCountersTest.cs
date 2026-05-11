using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using NUnit.Framework;
using static Adaptive.Agrona.Concurrent.Status.CountersReader;

namespace Adaptive.Aeron.Tests
{
    public class AeronCountersTest
    {
        [Test]
        public void ShouldNotHaveOverlappingCounterTypeIds()
        {
            var fieldByTypeId = new Dictionary<int, FieldInfo>();
            var duplicates = new Dictionary<int, List<FieldInfo>>();

            var typeIdFields = typeof(AeronCounters).GetFields(BindingFlags.Public | BindingFlags.Static)
                .Where(f => f.IsLiteral || f.IsInitOnly)
                .Where(f => f.Name.EndsWith("_TYPE_ID"))
                .Where(f => f.FieldType == typeof(int));

            foreach (var f in typeIdFields)
            {
                int typeId = (int)f.GetValue(null);
                if (fieldByTypeId.TryGetValue(typeId, out var existing))
                {
                    if (!duplicates.TryGetValue(typeId, out var list))
                    {
                        list = new List<FieldInfo>();
                        duplicates[typeId] = list;
                    }
                    if (!list.Contains(f)) list.Add(f);
                    list.Add(existing);
                }
                else
                {
                    fieldByTypeId[typeId] = f;
                }
            }

            if (duplicates.Count > 0)
            {
                var lines = duplicates.Select(kv => kv.Key + " -> " +
                    string.Join(", ", kv.Value.Select(f => f.DeclaringType?.Name + "." + f.Name)));
                Assert.Fail("Duplicate typeIds: " + string.Join("; ", lines));
            }
        }

        [TestCase(
            "1.42.1",
            "8165495befc07e997a7f2f7743beab9d3846b0a5",
            "version=1.42.1 commit=8165495befc07e997a7f2f7743beab9d3846b0a5")]
        [TestCase("1.43.0-SNAPSHOT", "abc", "version=1.43.0-SNAPSHOT commit=abc")]
        [TestCase("NIL", "12345678", "version=NIL commit=12345678")]
        public void ShouldFormatVersionInfo(string fullVersion, string commitHash, string expected)
        {
            Assert.AreEqual(expected, AeronCounters.FormatVersionInfo(fullVersion, commitHash));
        }

        [TestCase("xyz", "1234567890", "version=xyz commit=1234567890")]
        [TestCase("1.43.0-SNAPSHOT", "abc", "version=1.43.0-SNAPSHOT commit=abc")]
        public void ShouldAppendVersionInfo(string fullVersion, string commitHash, string formatted)
        {
            string expected = " " + formatted;
            var buffer = new ExpandableArrayBuffer(32);
            const int offset = 5;
            buffer.SetMemory(0, buffer.Capacity, 0xFF);

            int length = AeronCounters.AppendVersionInfo(buffer, offset, fullVersion, commitHash);

            Assert.AreEqual(expected.Length, length);
            Assert.AreEqual(expected, buffer.GetStringWithoutLengthAscii(offset, length));
        }

        [TestCase(int.MinValue)]
        [TestCase(-1)]
        public void AppendToLabelThrowsArgumentExceptionIfCounterIsNegative(int counterId)
        {
            var exception = Assert.Throws<ArgumentException>(
                () => AeronCounters.AppendToLabel(new UnsafeBuffer([]), counterId, "test"));
            Assert.AreEqual("counter id " + counterId + " is negative", exception.Message);
        }

        [Test]
        public void AppendToLabelThrowsArgumentNullExceptionIfBufferIsNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => AeronCounters.AppendToLabel(null, 5, "test"));
        }

        [TestCase(1_000_000)]
        [TestCase(int.MaxValue)]
        public void AppendToLabelThrowsArgumentExceptionIfCounterIsOutOfRange(int counterId)
        {
            var metaDataBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH * 3]);

            var exception = Assert.Throws<ArgumentException>(
                () => AeronCounters.AppendToLabel(metaDataBuffer, counterId, "test"));
            Assert.AreEqual("counter id " + counterId + " out of range: 0 - maxCounterId=2", exception.Message);
        }

        [TestCase(RECORD_UNUSED)]
        [TestCase(RECORD_RECLAIMED)]
        public void AppendToLabelThrowsArgumentExceptionIfCounterIsInWrongState(int state)
        {
            var metaDataBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH * 2]);
            const int counterId = 1;
            int metaDataOffset = MetaDataOffset(counterId);
            metaDataBuffer.PutInt(metaDataOffset, state);

            var exception = Assert.Throws<ArgumentException>(
                () => AeronCounters.AppendToLabel(metaDataBuffer, counterId, "test"));
            Assert.AreEqual("counter id 1 is not allocated, state: " + state, exception.Message);
        }

        [Test]
        public void AppendToLabelShouldAddSuffix()
        {
            var countersManager = new CountersManager(
                new UnsafeBuffer(new byte[METADATA_LENGTH]),
                new UnsafeBuffer(new byte[COUNTER_LENGTH]),
                Encoding.ASCII);
            int counterId = countersManager.Allocate("initial value: ");

            int length = AeronCounters.AppendToLabel(countersManager.MetaDataBuffer, counterId, "test");

            Assert.AreEqual(4, length);
            Assert.AreEqual("initial value: test", countersManager.GetCounterLabel(counterId));
        }

        [Test]
        public void AppendToLabelShouldAddAPortionOfSuffixUpToTheMaxLength()
        {
            var countersManager = new CountersManager(
                new UnsafeBuffer(new byte[METADATA_LENGTH]),
                new UnsafeBuffer(new byte[COUNTER_LENGTH]),
                Encoding.ASCII);
            const string initialLabel = "this is a test counter";
            int counterId = countersManager.Allocate(initialLabel);
            string hugeSuffix = " - 42" + new string('x', MAX_LABEL_LENGTH);

            int length = AeronCounters.AppendToLabel(countersManager.MetaDataBuffer, counterId, hugeSuffix);

            Assert.AreNotEqual(hugeSuffix.Length, length);
            Assert.AreEqual(MAX_LABEL_LENGTH - initialLabel.Length, length);
            Assert.AreEqual(initialLabel + hugeSuffix.Substring(0, length), countersManager.GetCounterLabel(counterId));
        }

        [Test]
        public void AppendToLabelIsANoOpIfThereIsNoSpaceInTheLabel()
        {
            var countersManager = new CountersManager(
                new UnsafeBuffer(new byte[METADATA_LENGTH]),
                new UnsafeBuffer(new byte[COUNTER_LENGTH]),
                Encoding.ASCII);
            string label = new string('a', MAX_LABEL_LENGTH);
            int counterId = countersManager.Allocate(label);

            int length = AeronCounters.AppendToLabel(countersManager.MetaDataBuffer, counterId, "test");

            Assert.AreEqual(0, length);
            Assert.AreEqual(label, countersManager.GetCounterLabel(counterId));
        }

        [Test]
        public void SetReferenceIdShouldThrowArgumentNullExceptionIfMetadataBufferIsNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => AeronCounters.SetReferenceId(null, new UnsafeBuffer(new byte[0]), 1, 123));
        }

        [Test]
        public void SetReferenceIdShouldThrowArgumentNullExceptionIfValuesBufferIsNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => AeronCounters.SetReferenceId(new UnsafeBuffer(new byte[0]), null, 1, 123));
        }

        [Test]
        public void SetReferenceIdShouldRejectNegativeCounterId()
        {
            var exception = Assert.Throws<ArgumentException>(
                () => AeronCounters.SetReferenceId(
                    new UnsafeBuffer(new byte[0]), new UnsafeBuffer(new byte[0]), -4, 123));
            Assert.AreEqual("counter id -4 is negative", exception.Message);
        }

        [Test]
        public void SetReferenceIdShouldRejectCounterIdWhichIsOutOfRange()
        {
            var exception = Assert.Throws<ArgumentException>(
                () => AeronCounters.SetReferenceId(
                    new UnsafeBuffer(new byte[2 * METADATA_LENGTH]),
                    new UnsafeBuffer(new byte[0]),
                    42,
                    777));
            Assert.AreEqual("counter id 42 out of range: 0 - maxCounterId=1", exception.Message);
        }

        [TestCase(long.MinValue)]
        [TestCase(0L)]
        [TestCase(54375943437284L)]
        [TestCase(long.MaxValue)]
        public void SetReferenceIdShouldSetSpecifiedValue(long referenceId)
        {
            const int counterId = 7;

            var metadataBuffer = new UnsafeBuffer(new byte[(counterId + 1) * METADATA_LENGTH]);
            var valuesBuffer = new UnsafeBuffer(new byte[(counterId + 1) * COUNTER_LENGTH]);

            AeronCounters.SetReferenceId(metadataBuffer, valuesBuffer, counterId, referenceId);

            Assert.AreEqual(referenceId, valuesBuffer.GetLong(CounterOffset(counterId) + REFERENCE_ID_OFFSET));
        }
    }
}
