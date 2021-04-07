using System;
using System.Buffers.Binary;
using System.Collections;
using System.Runtime.InteropServices;
using Core.Search;
using RocksDbSharp;

namespace Storage.Rocks
{
    public class RocksDbTermsEnumerator : ITermsEnumerator
    {
        protected RocksDbStorage Storage;
        protected Iterator RocksDbIterator;
        protected bool Positioned;
        protected static byte[] TermStart = {(byte) RocksDbStorage.StoreType.InvertedIndexTerm};

        internal RocksDbTermsEnumerator(RocksDbStorage storage)
        {
            Storage = storage;
            RocksDbIterator = storage.Database.NewIterator();
            RocksIteratorKey = Array.Empty<byte>();
        }

        protected byte[] RocksIteratorKey;
        protected bool RocksIteratorValid;
        protected int CurrentTermId;
        protected void UpdateIteratorState()
        {
            var iteratorValid = RocksDbIterator.Valid();
            if (!iteratorValid)
            {
                RocksIteratorValid = false;
                return;
            }
            
            // Todo: Allocating, should fix RocksDB ?
            RocksIteratorKey = RocksDbIterator.Key();
            RocksIteratorValid = RocksIteratorKey[0] == TermStart[0];
            if (RocksIteratorValid)
            {
                CurrentTermId = BinaryPrimitives.ReadInt32BigEndian(RocksDbIterator.Value());
                RocksIteratorValid = true;
            }
        }

        public bool MoveNext()
        {
            if (!Positioned)
            {
                RocksDbIterator.Seek(TermStart);
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
        
        public ReadOnlyMemory<byte> Current => new ReadOnlyMemory<byte>(RocksIteratorKey).Slice(1);

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            RocksDbIterator.Dispose();
        }

        public unsafe bool SeekExact(ReadOnlyMemory<byte> data)
        {
            using var _ = RocksDbEncoder.EncodeInvertedIndexTerm(data.Span, out var bytes);
            fixed (byte* b = &MemoryMarshal.GetReference(bytes))
            {
                RocksDbIterator.Seek(b, (ulong) bytes.Length);
            }
            UpdateIteratorState();
            return RocksIteratorValid && RocksIteratorKey.AsSpan().SequenceEqual(bytes);
        }

        public unsafe bool SeekCeil(ReadOnlyMemory<byte> data)
        {
            using var _ = RocksDbEncoder.EncodeInvertedIndexTerm(data.Span, out var bytes);
            fixed (byte* b = &MemoryMarshal.GetReference(bytes))
            {
                RocksDbIterator.Seek(b, (ulong) bytes.Length);
            }
            UpdateIteratorState();
            return RocksIteratorValid;
        }

        public int CurrentTermFrequency { get; }

        public IDocIdSetEnumerator CurrentTermDocs()
        {
            return new RocksDbDocsIdSetEnumerator(Storage, CurrentTermId, null);
        }
    }
}