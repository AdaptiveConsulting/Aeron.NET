using System;
using System.Collections.Generic;
using System.IO;
using Adaptive.Aeron.Exceptions;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.Exceptions
{
    public class StorageSpaceExceptionTest
    {
        [Test]
        public void IsStorageSpaceErrorReturnsFalseIfNull()
        {
            Assert.IsFalse(StorageSpaceException.IsStorageSpaceError(null));
        }

        [Test]
        public void IsStorageSpaceErrorReturnsFalseIfNotIOException()
        {
            Assert.IsFalse(StorageSpaceException.IsStorageSpaceError(
                new ArgumentException("No space left on device")));
        }

        [Test]
        public void IsStorageSpaceErrorReturnsFalseIfWrongMessage()
        {
            Assert.IsFalse(StorageSpaceException.IsStorageSpaceError(
                new ArgumentException("Es steht nicht genug Speicherplatz auf dem Datenträger zur Verfügung")));
        }

        [TestCaseSource(nameof(StorageSpaceErrors))]
        public void IsStorageSpaceErrorReturnsTrueWhenIOExceptionWithAParticularMessage(Exception exception)
        {
            Assert.IsTrue(StorageSpaceException.IsStorageSpaceError(exception));
        }

        public static IEnumerable<TestCaseData> StorageSpaceErrors()
        {
            yield return new TestCaseData(new StorageSpaceException("test"));
            yield return new TestCaseData(new AeronException(new StorageSpaceException("test2")));
            yield return new TestCaseData(new IOException("No space left on device"));
            yield return new TestCaseData(new IOException("There is not enough space on the disk"));
            yield return new TestCaseData(new IOException(
                "something else", new IOException("No space left on device")));
            yield return new TestCaseData(new AeronException(
                new ArgumentException("wrap", new IOException("There is not enough space on the disk"))));
        }
    }
}
