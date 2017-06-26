/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Adaptive.Agrona.Collections;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Map for navigating to active <seealso cref="Publication"/>s.
    /// </summary>
    class ActivePublications : IDisposable
    {
        private readonly IDictionary<string, Dictionary<int, Publication>> _publicationsByChannelMap = 
            new Dictionary<string, Dictionary<int, Publication>>();

        public Publication Get(string channel, int streamId)
        {
            Dictionary<int, Publication> publicationByStreamIdMap;
            if (!_publicationsByChannelMap.TryGetValue(channel, out publicationByStreamIdMap))
            {
                return null;
            }

            Publication publication;
            if (!publicationByStreamIdMap.TryGetValue(streamId, out publication))
            {
                return null;
            }

            return publication;
        }

        public Publication Put(string channel, int streamId, Publication publication)
        {
            var publicationByStreamIdMap = CollectionUtil.GetOrDefault(
                _publicationsByChannelMap, 
                channel, 
                _ => new Dictionary<int, Publication>());

            publicationByStreamIdMap.Add(streamId, publication);
            return publication;
        }

        public Publication Remove(string channel, int streamId)
        {
            Dictionary<int, Publication> publicationByStreamIdMap;
            if (!_publicationsByChannelMap.TryGetValue(channel, out publicationByStreamIdMap))
            {
                return null;
            }

            Publication publication;
            if (publicationByStreamIdMap.TryGetValue(streamId, out publication))
            {
                publicationByStreamIdMap.Remove(streamId);
            }
            if (publicationByStreamIdMap.Count == 0)
            {
                _publicationsByChannelMap.Remove(channel);
            }
            
            return publication;
        }

        public void Dispose()
        {
            foreach (var publications in _publicationsByChannelMap.Values)
            {
                foreach (var publication in publications.Values)
                {
                    publication.ForceClose();
                }
            }

            _publicationsByChannelMap.Clear();
        }
    }
}