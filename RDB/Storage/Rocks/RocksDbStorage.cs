using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using RDB.Core.Search;
using RDB.Core.Term;
using RocksDbSharp;

namespace RDB.Storage.Rocks
{
    public class RocksDbStorage : IDisposable, IStorageEngine
    {
        internal RocksDb Database;
        internal RocksDbAliasStore AliasStore;
        protected string Path;

        internal enum StoreType
        {
            InvertedIndexTerm = 1,
            InvertedIndexPostingWithProperty = 2,
            InvertedIndexPosting = 3,
            Property = 4,
            Counter = 5,
            Alias = 6
        }

        public enum Counter
        {
            DocsCount = 2,
            LastAlias = 3,
            TermFrequencyByTermAndProperty = 4,
            TermFrequencyByTerm = 5
        }

        public RocksDbStorage(DbOptions options, string path)
        {
            Path = path;
            options.SetUint64addMergeOperator();
            Database = RocksDb.Open(options, path);
            AliasStore = new RocksDbAliasStore(this);
        }

        public void Dispose()
        {
            Database.Dispose();
        }

        public ulong GetCounterValue(Counter counter, Span<byte> index)
        {
            using var _ = RocksDbEncoder.EncodeCounter(counter, index, out var key);
            var result = Database.Get(key);
            return result != null ? BinaryPrimitives.ReadUInt64LittleEndian(result) : 0;
        }

        internal void IncrementCounterValue(WriteBatch batch, Counter counter, Span<byte> index)
        {
            using var _ = RocksDbEncoder.EncodeCounter(counter, index, out var key);

            byte[] newValueBytes = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(newValueBytes, 1);
            // Todo: Allocation
            batch.Merge(key.ToArray(), (ulong) key.Length, newValueBytes, (ulong) newValueBytes.Length);
        }

        protected void AddInvertedIndexPosting(WriteBatch batch, int propertyId, int termId, int docId)
        {
            using var posting1 = RocksDbEncoder.EncodeInvertedIndexPostingWithProperty(termId, propertyId, docId, out var span1);
            using var posting2 = RocksDbEncoder.EncodeInvertedIndexPosting(termId, docId, out var span2);
            
            batch.Put(span1, ReadOnlySpan<byte>.Empty);
            IncrementCounterValue(batch, Counter.TermFrequencyByTermAndProperty, span1.Slice(1, 8));

            batch.Put(span2, ReadOnlySpan<byte>.Empty);
            //Todo: Slow
            if (Database.Get(span2) == null) IncrementCounterValue(batch, Counter.TermFrequencyByTerm, span2.Slice(1, 4));
        }

        protected void AddInvertedIndexTerm(WriteBatch batch, ReadOnlySpan<byte> property, ReadOnlySpan<byte> term,
            int docId)
        {
            var propertyId = AliasStore.GetOrCreate(property);
            var termId = AliasStore.GetOrCreate(term);

            using (RocksDbEncoder.EncodeInt32(termId, out var termIdBytes))
            {
                using (RocksDbEncoder.EncodeInvertedIndexTerm(term, out var array))
                {
                    batch.Put(array, termIdBytes);
                    AddInvertedIndexPosting(batch, propertyId, termId, docId);
                }
            }
        }

        public int Index(JsonDocument doc)
        {
            byte[] array = ArrayPool<byte>.Shared.Rent(32768);
            try
            {
                WriteBatch b = new();
                int docId;
                lock (this)
                {
                    docId = (int) GetCounterValue(Counter.DocsCount, stackalloc byte[1] {0});
                    WriteBatch docBatch = new();
                    IncrementCounterValue(docBatch, Counter.DocsCount, stackalloc byte[1] {0});
                    Database.Write(docBatch);
                }

                IndexJsonElement(b, docId, doc.RootElement, array.AsSpan());
                Database.Write(b);
                return docId;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(array);
            }
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public void Optimize()
        {
            Database.CompactRange(null,null, null);
        }

        protected void IndexJsonElement(WriteBatch batch, int docId, JsonElement element, Span<byte> currentPath,
            int currentPathSize = 0)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var v in element.EnumerateObject())
                    {
                        var keySpan = Encoding.UTF8.GetBytes("." + v.Name).AsSpan();
                        keySpan.CopyTo(currentPath.Slice(currentPathSize));
                        IndexJsonElement(batch, docId, v.Value, currentPath, currentPathSize + keySpan.Length);
                    }

                    break;
                case JsonValueKind.Array:
                    foreach (var v in element.EnumerateArray())
                    {
                        // if (v.ValueKind == JsonValueKind.Object) continue;
                        var keySpan = Encoding.UTF8.GetBytes("[]").AsSpan();
                        keySpan.CopyTo(currentPath.Slice(currentPathSize));
                        IndexJsonElement(batch, docId, v, currentPath, currentPathSize + keySpan.Length);
                    }

                    break;
                case JsonValueKind.Undefined:
                case JsonValueKind.Null:
                    using (var nullTerm = Term.FromNull())
                    {
                        AddInvertedIndexTerm(batch, currentPath.Slice(0, currentPathSize), nullTerm.Data.Span, docId);
                    }

                    break;
                case JsonValueKind.True:
                    using (var boolTerm = Term.FromBool(true))
                    {
                        AddInvertedIndexTerm(batch, currentPath.Slice(0, currentPathSize), boolTerm.Data.Span, docId);
                    }

                    break;
                case JsonValueKind.False:
                    using (var boolTerm = Term.FromBool(false))
                    {
                        AddInvertedIndexTerm(batch, currentPath.Slice(0, currentPathSize), boolTerm.Data.Span, docId);
                    }

                    break;
                case JsonValueKind.String:
                    var str = element.GetString();
                    if (str == null) return;
                    using (var stringTerm = Term.FromString(str))
                    {
                        AddInvertedIndexTerm(batch, currentPath.Slice(0, currentPathSize), stringTerm.Data.Span, docId);
                    }

                    break;
                case JsonValueKind.Number:
                    var isInt64 = element.TryGetInt64(out var asInt64);
                    if (isInt64)
                    {
                        using (var longTerm = Term.FromLong(asInt64))
                        {
                            AddInvertedIndexTerm(batch, currentPath.Slice(0, currentPathSize), longTerm.Data.Span,
                                docId);
                            return;
                        }
                    }

                    var isDouble = element.TryGetDouble(out var asDouble);
                    if (isDouble)
                    {
                        using (var doubleTerm = Term.FromDouble(asDouble))
                        {
                            AddInvertedIndexTerm(batch, currentPath.Slice(0, currentPathSize), doubleTerm.Data.Span,
                                docId);
                            return;
                        }
                    }

                    throw new Exception("Can't read number");
            }
        }

        public ITermsEnumerator GetTermsEnumerator()
        {
            return new RocksDbTermsEnumerator(this);
        }

        public int DocCount => (int) GetCounterValue(Counter.DocsCount, stackalloc byte[1] {0});
    }
}