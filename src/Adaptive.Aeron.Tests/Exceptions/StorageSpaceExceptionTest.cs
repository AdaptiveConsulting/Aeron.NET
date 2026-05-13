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
            Assert.IsFalse(StorageSpaceException.IsStorageSpaceError(new ArgumentException("No space left on device")));
        }

        [Test]
        public void IsStorageSpaceErrorReturnsFalseIfWrongMessage()
        {
            Assert.IsFalse(
                StorageSpaceException.IsStorageSpaceError(
                    new ArgumentException("Es steht nicht genug Speicherplatz auf dem Datenträger zur Verfügung")
                )
            );
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
            yield return new TestCaseData(
                new IOException("something else", new IOException("No space left on device"))
            );
            yield return new TestCaseData(
                new AeronException(
                    new ArgumentException("wrap", new IOException("There is not enough space on the disk"))
                )
            );
        }
    }
}
