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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Agrona
{
    public static class CloseHelper
    {
        /// <summary>
        /// Quietly close a <seealso cref="IDisposable"/> dealing with nulls and exceptions.
        /// </summary>
        /// <param name="disposable"> to be disposed. </param>
        public static void QuietDispose(IDisposable disposable)
        {
            try
            {
                disposable?.Dispose();
            }
            catch
            {
            }
        }

        /// <summary>
        /// Quietly close a <seealso cref="IDisposable"/> dealing with nulls and exceptions.
        /// </summary>
        /// <param name="disposable"> to be disposed. </param>
        public static void QuietDispose(Action disposable)
        {
            try
            {
                disposable?.Invoke();
            }
            catch
            {
            }
        }

        /// <summary>
        /// Dispose an <see cref="IDisposable"/> delegating exceptions to <see cref="ErrorHandler"/>.
        /// </summary>
        /// <param name="errorHandler"> to delegate exceptions to.</param>
        /// <param name="disposable"> to be closed.</param>
        public static void Dispose(ErrorHandler errorHandler, IDisposable disposable)
        {
            try
            {
                disposable?.Dispose();
            }
            catch (Exception ex)
            {
                errorHandler(ex);
            }
        }

        /// <summary>
        /// Dispose an <see cref="IDisposable"/> delegating exceptions to <see cref="ErrorHandler"/>.
        /// </summary>
        /// <param name="errorHandler"> to delegate exceptions to.</param>
        /// <param name="disposable"> to be closed.</param>
        public static void Dispose(IErrorHandler errorHandler, IDisposable disposable)
        {
            try
            {
                disposable?.Dispose();
            }
            catch (Exception ex)
            {
                errorHandler.OnError(ex);
            }
        }

        /// <summary>
        /// Dispose an <see cref="IDisposable"/> delegating exceptions to <see cref="ErrorHandler"/>.
        /// </summary>
        /// <param name="errorHandler"> to delegate exceptions to.</param>
        /// <param name="disposable"> to be closed.</param>
        public static void Dispose(ErrorHandler errorHandler, Action disposable)
        {
            try
            {
                disposable?.Invoke();
            }
            catch (Exception ex)
            {
                errorHandler(ex);
            }
        }

        /// <summary>
        /// Close an IDisposable, dealing with nulls and rethrowing exceptions as runtime exceptions.
        /// </summary>
        /// <param name="disposable">The IDisposable to close.</param>
        public static void Dispose(IDisposable disposable)
        {
            disposable?.Dispose();
        }

        public static void CloseAll(IEnumerable<IDisposable> disposables)
        {
            if (disposables == null || !disposables.Any())
            {
                return;
            }

            Exception error = null;
            foreach (var disposable in disposables)
            {
                if (disposable != null)
                {
                    try
                    {
                        disposable.Dispose();
                    }
                    catch (Exception ex)
                    {
                        if (error == null)
                        {
                            error = ex;
                        }
                        else
                        {
                            error = new Exception(null, ex); // Exceptions are chained rather than suppressed as in Java
                        }
                    }
                }
            }

            if (error != null)
            {
                throw error;
            }
        }

        /// <summary>
        /// Dispose all disposables and delegate exceptions to the <see cref="IErrorHandler"/>.
        /// If <paramref name="errorHandler"/> is null, exceptions are aggregated and thrown at the end.
        /// </summary>
        public static void CloseAll<T>(IErrorHandler errorHandler, ICollection<T> disposables) where T : IDisposable
        {
            if (disposables == null) return;

            NullReferenceException error = null;
            List<Exception> suppressed = null;

            foreach (var disposable in disposables)
            {
                if (disposable == null) continue;

                try
                {
                    disposable.Dispose();
                }
                catch (Exception ex)
                {
                    if (errorHandler == null)
                    {
                        if (error == null)
                        {
                            error = new NullReferenceException("errorHandler is null");
                        }

                        if (suppressed == null)
                        {
                            suppressed = new List<Exception>();
                        }

                        suppressed.Add(ex);
                    }
                    else
                    {
                        errorHandler.OnError(ex);
                    }
                }
            }

            if (error != null)
            {
                if (suppressed.Count > 0)
                {
                    var all = new List<Exception>(1 + suppressed.Count);
                    all.Add(error);
                    all.AddRange(suppressed);
                    throw new AggregateException("One or more errors occurred while disposing.", all);
                }

                throw error;
            }
        }
    }
}