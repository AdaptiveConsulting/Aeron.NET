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

using System.Collections.Generic;
using Adaptive.Agrona.Collections;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Collections
{
    [TestFixture]
    public class CollectionUtilTest
    {
        [Test]
        public void GetOrDefaultUsesSupplier()
        {
            var ints = new Dictionary<int?, int?>();
            var result = CollectionUtil.GetOrDefault(ints, 0, x => x + 1);

            Assert.That(result, Is.EqualTo(1));
        }

        [Test]
        public void GetOrDefaultDoesNotCreateNewValueWhenOneExists()
        {
            var ints = new Dictionary<int?, int?> {[0] = 0};
            var result = CollectionUtil.GetOrDefault(ints, 0, (x) =>
            {
                Assert.Fail("Shouldn't be called");
                return x + 1;
            });

            Assert.That(result, Is.EqualTo(0));
        }
    }
}