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

namespace Adaptive.Archiver.IntegrationTests.Infrastructure
{
    [TestFixture]
    [Category("Integration")]
    public class EmbeddedArchiveSmokeTest
    {
        [Test, Timeout(60_000)]
        public void CanStartDriverAndArchiveAndConnect()
        {
            using var driver = new EmbeddedMediaDriver();
            using var archive = new EmbeddedArchive(driver.AeronDirectoryName);
            using var client = AeronArchive.Connect(archive.CreateClientContext(driver.AeronDirectoryName));

            Assert.That(client, Is.Not.Null);
        }
    }
}
