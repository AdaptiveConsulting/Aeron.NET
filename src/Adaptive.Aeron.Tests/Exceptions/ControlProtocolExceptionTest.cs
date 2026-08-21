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
using Adaptive.Aeron.Exceptions;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.Exceptions
{
    public class ControlProtocolExceptionTest
    {
        private static readonly Exception RootCause = new InvalidOperationException("root cause");

        public static IEnumerable<TestCaseData> CategoryCases()
        {
            yield return new TestCaseData(
                    new ControlProtocolException(ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE, "msg"),
                    Category.WARN)
                .SetName("MessageCtorMapsResourceTemporarilyUnavailableToWarn");
            yield return new TestCaseData(
                    new ControlProtocolException(ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE, RootCause),
                    Category.WARN)
                .SetName("RootCauseCtorMapsResourceTemporarilyUnavailableToWarn");
            yield return new TestCaseData(
                    new ControlProtocolException(ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE, "msg", RootCause),
                    Category.WARN)
                .SetName("MessageAndRootCauseCtorMapsResourceTemporarilyUnavailableToWarn");

            yield return new TestCaseData(
                    new ControlProtocolException(ErrorCode.MALFORMED_COMMAND, "msg"),
                    Category.ERROR)
                .SetName("MessageCtorMapsMalformedCommandToError");
            yield return new TestCaseData(
                    new ControlProtocolException(ErrorCode.MALFORMED_COMMAND, RootCause),
                    Category.ERROR)
                .SetName("RootCauseCtorMapsMalformedCommandToError");
            yield return new TestCaseData(
                    new ControlProtocolException(ErrorCode.MALFORMED_COMMAND, "msg", RootCause),
                    Category.ERROR)
                .SetName("MessageAndRootCauseCtorMapsMalformedCommandToError");
        }

        [TestCaseSource(nameof(CategoryCases))]
        public void ShouldDeriveCategoryFromErrorCode(ControlProtocolException exception, Category expected)
        {
            Assert.AreEqual(expected, exception.Category);
        }
    }
}
