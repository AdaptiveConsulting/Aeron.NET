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
using System.Threading;
using Adaptive.Agrona.Concurrent;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Concurrent
{
    public class AgentRunnerTest
    {
        private IAgent _agent;
        private IErrorHandler _errorHandler;
        private AgentRunner _runner;

        [SetUp]
        public void Setup()
        {
            _agent = A.Fake<IAgent>();
            _errorHandler = A.Fake<IErrorHandler>();
            A.CallTo(() => _agent.RoleName()).Returns("test-agent");

            _runner = new AgentRunner(new NoOpIdleStrategy(), _errorHandler, null, _agent);
        }

        [Test]
        public void ShouldNotInterruptRunnerThreadIfCloseCompletesOnTime()
        {
            var started = SetUpDoWork(() => 0);

            var runnerThread = AgentRunner.StartOnThread(_runner);
            started.Wait();

            _runner.Dispose();

            Assert.That(runnerThread.IsAlive, Is.False);
            Assert.That(_runner.IsClosed, Is.True);
            A.CallTo(() => _agent.OnStart()).MustHaveHappenedOnceExactly();
            A.CallTo(() => _agent.OnClose()).MustHaveHappenedOnceExactly();
            A.CallTo(() => _errorHandler.OnError(A<Exception>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldInterruptRunnerThreadIfCloseCallIsItselfInterrupted()
        {
            var started = SetUpDoWork(() => BlockUntilInterrupted());

            var runnerThread = AgentRunner.StartOnThread(_runner);
            started.Wait();

            var callerReinterrupted = false;
            var enteringClose = new ManualResetEventSlim(false);
            var closerThread = new Thread(() =>
            {
                enteringClose.Set();
                _runner.Dispose();

                try
                {
                    Thread.Sleep(100);
                }
                catch (ThreadInterruptedException)
                {
                    callerReinterrupted = true;
                }
            });
            closerThread.Start();

            enteringClose.Wait();
            closerThread.Interrupt();
            closerThread.Join();

            Assert.That(closerThread.IsAlive, Is.False);
            Assert.That(runnerThread.IsAlive, Is.False);
            Assert.That(callerReinterrupted, Is.True);
            Assert.That(_runner.IsClosed, Is.True);
        }

        [Test]
        public void ShouldInterruptRunnerThreadIfCloseDoesNotCompleteWithinCloseTimeout()
        {
            var started = SetUpDoWork(() => BlockUntilInterrupted());

            var runnerThread = AgentRunner.StartOnThread(_runner);
            started.Wait();

            _runner.Dispose();

            Assert.That(runnerThread.IsAlive, Is.False);
            Assert.That(_runner.IsClosed, Is.True);
            A.CallTo(() => _agent.OnClose()).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void ShouldInterruptRunnerThreadIfCloseCompletesOnTimeWhenMainThreadIsInterruptedBeforeTheCloseCall()
        {
            var agentInterrupted = false;
            var started = SetUpDoWork(() => BlockUntilInterrupted(() => agentInterrupted = true));

            var runnerThread = AgentRunner.StartOnThread(_runner);
            started.Wait();

            var callerReinterrupted = false;
            var closerThread = new Thread(() =>
            {
                Thread.CurrentThread.Interrupt();

                _runner.Dispose();

                try
                {
                    Thread.Sleep(100);
                }
                catch (ThreadInterruptedException)
                {
                    callerReinterrupted = true;
                }
            });
            closerThread.Start();
            closerThread.Join();

            Assert.That(closerThread.IsAlive, Is.False);
            Assert.That(runnerThread.IsAlive, Is.False);
            Assert.That(agentInterrupted, Is.True);
            Assert.That(callerReinterrupted, Is.True);
            Assert.That(_runner.IsClosed, Is.True);
        }

        private ManualResetEventSlim SetUpDoWork(Func<int> onDoWork)
        {
            var started = new ManualResetEventSlim(false);
            A.CallTo(() => _agent.DoWork()).ReturnsLazily(() =>
            {
                started.Set();
                return onDoWork();
            });
            return started;
        }

        private static int BlockUntilInterrupted(Action onInterrupted = null)
        {
            try
            {
                Thread.Sleep(Timeout.Infinite);
            }
            catch (ThreadInterruptedException)
            {
                onInterrupted?.Invoke();
            }

            return 0;
        }
    }
}
