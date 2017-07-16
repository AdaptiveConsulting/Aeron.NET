using System.Collections;
using System.Collections.Generic;

namespace Adaptive.Agrona.Collections
{
    public class DefaultDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        private readonly IDictionary<TKey, TValue> _dictionaryImplementation = new Dictionary<TKey, TValue>();
        private readonly TValue _defaultValue;

        public DefaultDictionary(TValue defaultValue)
        {
            this._defaultValue = defaultValue;
        }
        
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _dictionaryImplementation.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable) _dictionaryImplementation).GetEnumerator();
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            _dictionaryImplementation.Add(item);
        }

        public void Clear()
        {
            _dictionaryImplementation.Clear();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return _dictionaryImplementation.Contains(item);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            _dictionaryImplementation.CopyTo(array, arrayIndex);
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return _dictionaryImplementation.Remove(item);
        }

        public int Count
        {
            get { return _dictionaryImplementation.Count; }
        }

        public bool IsReadOnly
        {
            get { return _dictionaryImplementation.IsReadOnly; }
        }

        public bool ContainsKey(TKey key)
        {
            return _dictionaryImplementation.ContainsKey(key);
        }

        public void Add(TKey key, TValue value)
        {
            _dictionaryImplementation.Add(key, value);
        }

        public bool Remove(TKey key)
        {
            return _dictionaryImplementation.Remove(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return _dictionaryImplementation.TryGetValue(key, out value);
        }

        public TValue this[TKey key]
        {
            get
            {
                TValue value;
                return _dictionaryImplementation.TryGetValue(key, out value) ? value : _defaultValue;
            }
            set { _dictionaryImplementation[key] = value; }
        }

        public ICollection<TKey> Keys
        {
            get { return _dictionaryImplementation.Keys; }
        }

        public ICollection<TValue> Values
        {
            get { return _dictionaryImplementation.Values; }
        }
    }
}
