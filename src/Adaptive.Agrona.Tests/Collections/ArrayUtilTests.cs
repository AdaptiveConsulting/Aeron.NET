/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
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

using Adaptive.Agrona.Collections;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Collections
{
    [TestFixture]
    public class ArrayUtilTest
    {
        // Reference Equality
        private static readonly object One = new object();
        private static readonly object Two = new object();
        private static readonly object Three = new object();

        private readonly object[] _values = { One, Two };

        [Test]
        public void ShouldNotRemoveMissingElement()
        {
            var result = ArrayUtil.Remove(_values, Three);

            Assert.That(_values, Is.EqualTo(result));
        }

        [Test]
        public void ShouldRemovePresentElementAtEnd()
        {
            var result = ArrayUtil.Remove(_values, Two);

            Assert.That(new[] { One }, Is.EqualTo(result));
        }

        [Test]
        public void ShouldRemovePresentElementAtStart()
        {
            var result = ArrayUtil.Remove(_values, One);

            Assert.That(new [] { Two }, Is.EqualTo(result));
        }

        [Test]
        public void ShouldRemoveByIndex()
        {
            var result = ArrayUtil.Remove(_values, 0);

            Assert.That(new [] { Two }, Is.EqualTo(result));
        }
    }

}