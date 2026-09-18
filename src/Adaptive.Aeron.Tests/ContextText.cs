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
        private Aeron.Context _ctx;

        [SetUp]
        public void SetUp()
        {
            _driver = new EmbeddedMediaDriver();
            _ctx = new Aeron.Context().AeronDirectoryName(_driver.AeronDirectoryName);
        }

        [TearDown]
        public void StopDriver() => _driver?.Dispose();

        [Test]
        public void ShouldNotAllowConcludeMoreThanOnce()
        {
            _ctx.Conclude();
            Assert.Throws(typeof(ConcurrentConcludeException), () => _ctx.Conclude());
        }

        [Test]
        public void ShouldAllowConcludeOfClonedContext()
        {
            var ctx2 = _ctx.Clone();

            _ctx.Conclude();
            ctx2.Conclude();
        }
    }
}
