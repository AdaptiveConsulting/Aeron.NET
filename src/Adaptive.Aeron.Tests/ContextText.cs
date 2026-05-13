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

using Adaptive.Aeron.Exceptions;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    [TestFixture]
    public class ContextText
    {
        private EmbeddedMediaDriver _driver;

        [SetUp]
        public void StartDriver() => _driver = new EmbeddedMediaDriver();

        [TearDown]
        public void StopDriver() => _driver?.Dispose();

        [Test]
        public void ShouldNotAllowConcludeMoreThanOnce()
        {
            var ctx = new Aeron.Context();

            ctx.Conclude();
            Assert.Throws(typeof(ConcurrentConcludeException), () => ctx.Conclude());
        }

        [Test]
        public void ShouldAllowConcludeOfClonedContext()
        {
            var ctx = new Aeron.Context();

            var ctx2 = ctx.Clone();

            ctx.Conclude();
            ctx2.Conclude();
        }
    }
}
