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
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;
using static Adaptive.Aeron.LogBuffer.LogBufferDescriptor;

namespace Adaptive.Aeron.Tests
{
    public class LogBuffersTest
    {
        private string _tempDir;

        [SetUp]
        public void SetUp()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "Aeron.NET-LogBuffersTest-" + Guid.NewGuid());
            Directory.CreateDirectory(_tempDir);
        }

        [TearDown]
        public void TearDown()
        {
            try
            {
                Directory.Delete(_tempDir, true);
            }
            catch
            {
                // ignore
            }
        }

        [TestCase(-100)]
        [TestCase(0)]
        [TestCase(TERM_MIN_LENGTH >> 1)]
        [TestCase(TERM_MAX_LENGTH + 1)]
        [TestCase(TERM_MAX_LENGTH - 1)]
        public void ThrowsIllegalStateExceptionIfTermLengthIsInvalid(int termLength)
        {
            var logFile = Path.Combine(_tempDir, "test.log");
            var contents = new byte[LOG_META_DATA_LENGTH];
            var buffer = new UnsafeBuffer(contents);
            TermLength(buffer, termLength);
            File.WriteAllBytes(logFile, contents);
            Assert.AreEqual(contents.Length, new FileInfo(logFile).Length);

            var exception = Assert.Throws<InvalidOperationException>(() => new LogBuffers(logFile));
            Assert.IsTrue(
                exception.Message.StartsWith("Term length") && exception.Message.EndsWith("length=" + termLength),
                "Unexpected message: " + exception.Message
            );
        }

        [TestCase(-100)]
        [TestCase(0)]
        [TestCase(PAGE_MIN_SIZE >> 1)]
        [TestCase(PAGE_MAX_SIZE + 1)]
        [TestCase(PAGE_MAX_SIZE - 1)]
        public void ThrowsIllegalStateExceptionIfPageSizeIsInvalid(int pageSize)
        {
            var logFile = Path.Combine(_tempDir, "test.log");
            var contents = new byte[LOG_META_DATA_LENGTH];
            var buffer = new UnsafeBuffer(contents);
            TermLength(buffer, TERM_MIN_LENGTH);
            PageSize(buffer, pageSize);
            File.WriteAllBytes(logFile, contents);
            Assert.AreEqual(contents.Length, new FileInfo(logFile).Length);

            var exception = Assert.Throws<InvalidOperationException>(() => new LogBuffers(logFile));
            Assert.IsTrue(
                exception.Message.StartsWith("Page size") && exception.Message.EndsWith("page size=" + pageSize),
                "Unexpected message: " + exception.Message
            );
        }

        [Test]
        public void ThrowsIllegalStateExceptionIfLogFileSizeIsLessThanLogMetaDataLength()
        {
            var logFile = Path.Combine(_tempDir, "test.log");
            const int extraShort = 5;
            int fileLength = LOG_META_DATA_LENGTH - extraShort;
            var contents = new byte[fileLength];
            var buffer = new UnsafeBuffer(contents);
            TermLength(buffer, TERM_MIN_LENGTH);
            PageSize(buffer, PAGE_MIN_SIZE);
            File.WriteAllBytes(logFile, contents);
            Assert.AreEqual(contents.Length, new FileInfo(logFile).Length);

            var exception = Assert.Throws<InvalidOperationException>(() => new LogBuffers(logFile));
            Assert.AreEqual(
                "Log file length less than min length of " + LOG_META_DATA_LENGTH + ": length=" + fileLength,
                exception.Message
            );
        }
    }
}
