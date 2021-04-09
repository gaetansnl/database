using System;
using System.Buffers;
using System.Buffers.Binary;
using RocksDbSharp;

namespace RDB.Storage.Rocks
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
            var size = 1 + value.Length;
            //Todo: faire pool avec taille exacte
            using var array = MemoryPool<byte>.Shared.Rent(size);
            var valueBytes = array.Memory.Slice(0, size).Span;

            valueBytes[0] = (byte) RocksDbStorage.StoreType.Alias;
            value.CopyTo(valueBytes.Slice(1));

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

        public int? Get(ReadOnlySpan<byte> value)
        {
            var size = 1 + value.Length;
            using var array = MemoryPool<byte>.Shared.Rent(size);
            var valueBytes = array.Memory.Slice(0, size).Span;

            valueBytes[0] = (byte) RocksDbStorage.StoreType.Alias;
            value.CopyTo(valueBytes.Slice(1));

            var existing = Storage.Database.Get(valueBytes);
            if (existing == null) return null;
            return BinaryPrimitives.ReadInt32BigEndian(existing);
        }
    }
}