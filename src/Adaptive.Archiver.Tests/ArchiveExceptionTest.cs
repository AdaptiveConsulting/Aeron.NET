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

using NUnit.Framework;

namespace Adaptive.Archiver.Tests
{
    public class ArchiveExceptionTest
    {
        [TestCase(0, "GENERIC")]
        [TestCase(1, "ACTIVE_LISTING")]
        [TestCase(2, "ACTIVE_RECORDING")]
        [TestCase(3, "ACTIVE_SUBSCRIPTION")]
        [TestCase(4, "UNKNOWN_SUBSCRIPTION")]
        [TestCase(5, "UNKNOWN_RECORDING")]
        [TestCase(6, "UNKNOWN_REPLAY")]
        [TestCase(7, "MAX_REPLAYS")]
        [TestCase(8, "MAX_RECORDINGS")]
        [TestCase(9, "INVALID_EXTENSION")]
        [TestCase(10, "AUTHENTICATION_REJECTED")]
        [TestCase(11, "STORAGE_SPACE")]
        [TestCase(12, "UNKNOWN_REPLICATION")]
        [TestCase(13, "UNAUTHORISED_ACTION")]
        [TestCase(14, "REPLICATION_CONNECTION_FAILURE")]
        public void ErrorCodeAsString(int errorCode, string expected)
        {
            Assert.AreEqual(expected, ArchiveException.ErrorCodeAsString(errorCode));
        }

        [TestCase(-1)]
        [TestCase(1111111)]
        [TestCase(54)]
        public void ShouldHandleUnknownErrorCodes(int errorCode)
        {
            Assert.AreEqual("unknown error code: " + errorCode, ArchiveException.ErrorCodeAsString(errorCode));
        }
    }
}
