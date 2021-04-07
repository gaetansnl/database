using System;
using System.Collections;
using Core.Search;
using RocksDbSharp;

namespace Storage.Rocks
{
    public class RocksDbDocsIdSetEnumerator : IDocIdSetEnumerator
    {
        protected RocksDbStorage Storage;
        protected Iterator RocksDbIterator;
        protected bool Positioned;
        protected int TermId;
        protected int? PropertyId;
        protected byte[] Prefix;
        public long Cost { get; }

        internal RocksDbDocsIdSetEnumerator(RocksDbStorage storage, int termId, int? propertyId)
        {
            Storage = storage;
            TermId = termId;
            PropertyId = propertyId;
            RocksDbIterator = storage.Database.NewIterator();
            RocksIteratorKey = Array.Empty<byte>();
            Prefix = RocksDbEncoder.EncodeInvertedIndexPostingPrefix(termId, propertyId);
            // Todo: Create encoder instead of slicing or create a special method
            Cost = propertyId != null
                ? (long) Storage.GetCounterValue(RocksDbStorage.Counter.TermFrequencyByTermAndProperty, Prefix.AsSpan().Slice(1))
                : (long) Storage.GetCounterValue(RocksDbStorage.Counter.TermFrequencyByTerm, Prefix.AsSpan().Slice(1));
        }

        protected byte[] RocksIteratorKey;
        protected bool RocksIteratorValid;
        protected int CurrentDocId;

        protected void UpdateIteratorState()
        {
            var iteratorValid = RocksDbIterator.Valid();
            if (!iteratorValid)
            {
                RocksIteratorValid = false;
                return;
            }

            // Todo: Allocating, stackalloc here if rocksdb support span
            RocksIteratorKey = RocksDbIterator.Key();
            RocksIteratorValid = RocksIteratorKey.AsSpan().StartsWith(Prefix.AsSpan());
            if (RocksIteratorValid)
            {
                CurrentDocId = RocksDbEncoder.ReadPostingDocId(RocksIteratorKey.AsSpan());
            }
        }

        public bool Advance(int target)
        {
            if (!PropertyId.HasValue)
            {
                using var _ = RocksDbEncoder.EncodeInvertedIndexPosting(TermId, target, out var prefix);
                RocksDbIterator.Seek(prefix.ToArray());
            }
            else
            {
                 using var _ = RocksDbEncoder.EncodeInvertedIndexPostingWithProperty(PropertyId.Value, TermId, target, out var prefix);
                 RocksDbIterator.Seek(prefix.ToArray());
            }
            
            Positioned = true;
            UpdateIteratorState();
            return RocksIteratorValid;
        }

        public bool MoveNext()
        {
            if (!Positioned)
            {
                RocksDbIterator.Seek(Prefix);
                Positioned = true;
            }
            else
            {
                RocksDbIterator.Next();
            }

            UpdateIteratorState();
            return RocksIteratorValid;
        }

        public void Reset()
        {
            Positioned = false;
        }

        public int Current => RocksIteratorValid ? CurrentDocId : Positioned ? int.MaxValue : -1;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            RocksDbIterator.Dispose();
        }
    }
}