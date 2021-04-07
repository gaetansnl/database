using System;
using System.Buffers;
using System.Buffers.Binary;
using RocksDbSharp;

namespace Storage.Rocks
{
    public class RocksDbAliasStore
    {
        protected RocksDbStorage Storage;

        public RocksDbAliasStore(RocksDbStorage storage)
        {
            Storage = storage;
        }

        public int GetOrCreate(ReadOnlySpan<byte> value)
        {
            byte[] valueBytes = ArrayPool<byte>.Shared.Rent(1 + value.Length);
            try
            {
                valueBytes[0] = (byte) RocksDbStorage.StoreType.Alias;
                value.CopyTo(valueBytes.AsSpan().Slice(1));

                var exising = Storage.Database.Get(valueBytes);
                if (exising != null) return BinaryPrimitives.ReadInt32BigEndian(exising);

                lock (this)
                {
                    exising = Storage.Database.Get(valueBytes);
                    if (exising != null) return BinaryPrimitives.ReadInt32BigEndian(exising);

                    WriteBatch b = new();
                    Storage.IncrementCounterValue(b, RocksDbStorage.Counter.LastAlias, Span<byte>.Empty);
                    Storage.Database.Write(b);

                    Span<byte> idBytes = stackalloc byte[4];
                    var id = (int) Storage.GetCounterValue(RocksDbStorage.Counter.LastAlias, Span<byte>.Empty);
                    BinaryPrimitives.WriteInt32BigEndian(idBytes, id);


                    Storage.Database.Put(valueBytes, idBytes);
                    return id;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(valueBytes);
            }
        }

        public int? Get(ReadOnlySpan<byte> value)
        {
            byte[] valueBytes = ArrayPool<byte>.Shared.Rent(1 + value.Length);
            try
            {
                valueBytes[0] = (byte) RocksDbStorage.StoreType.Alias;
                value.CopyTo(valueBytes.AsSpan().Slice(1));

                var exising = Storage.Database.Get(valueBytes);
                return BinaryPrimitives.ReadInt32BigEndian(exising);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(valueBytes);
            }
        }
    }
}