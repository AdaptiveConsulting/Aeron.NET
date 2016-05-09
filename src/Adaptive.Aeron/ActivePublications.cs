using System;
using System.Collections.Generic;
using System.Linq;
using Adaptive.Agrona.Collections;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Map for navigating to active <seealso cref="Publication"/>s.
    /// </summary>
    public class ActivePublications : IDisposable
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
            var publications = from publicationByStreamIdMap in _publicationsByChannelMap.Values
                from publication in publicationByStreamIdMap.Values
                select publication;

            foreach (var publication in publications)
            {
                publication.Release();
            }
        }
    }

}