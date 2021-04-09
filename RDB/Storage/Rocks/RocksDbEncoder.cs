using System;
using System.Buffers;
using System.Buffers.Binary;

namespace RDB.Storage.Rocks
{
    public static class RocksDbEncoder
    {
        public static IMemoryOwner<byte> EncodeInvertedIndexPostingWithProperty(int termId, int propertyId, int docId,
            out Span<byte> span)
        {
            var size = 1 + 3 * 4;
            IMemoryOwner<byte> array = MemoryPool<byte>.Shared.Rent(size);
            span = array.Memory.Span.Slice(0, size);

            span[0] = (byte) RocksDbStorage.StoreType.InvertedIndexPostingWithProperty;
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(1), termId);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(5), propertyId);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(9), docId);
            return array;
        }

        public static int ReadPostingDocId(Span<byte> span)
        {
            if (span.Length == 9)
            {
                return BinaryPrimitives.ReadInt32BigEndian(span.Slice(5));
            }
            else
            {
                return BinaryPrimitives.ReadInt32BigEndian(span.Slice(9));
            }
        }

        public static IMemoryOwner<byte> EncodeInvertedIndexPosting(int termId, int docId, out Span<byte> span)
        {
            var size = 1 + 2 * 4;
            IMemoryOwner<byte> array = MemoryPool<byte>.Shared.Rent(size);
            span = array.Memory.Span.Slice(0, size);

            span[0] = (byte) RocksDbStorage.StoreType.InvertedIndexPosting;
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(1), termId);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(5), docId);
            return array;
        }

        public static byte[] EncodeInvertedIndexPostingPrefix(int termId, int? propertyId)
        {
            //Todo: allocating
            if (propertyId == null)
            {
                using var p = EncodeInvertedIndexPosting(termId, 0, out var pSpan);
                return pSpan.Slice(0, 5).ToArray();
            }

            using var _ = EncodeInvertedIndexPostingWithProperty(termId, propertyId.Value, 0, out var span);
            return span.Slice(0, 9).ToArray();
        }

        public static IMemoryOwner<byte> EncodeInvertedIndexTerm(ReadOnlySpan<byte> term, out Span<byte> span)
        {
            var size = 1 + term.Length;
            IMemoryOwner<byte> array = MemoryPool<byte>.Shared.Rent(size);
            span = array.Memory.Span.Slice(0, size);

            span[0] = (byte) RocksDbStorage.StoreType.InvertedIndexTerm;
            term.CopyTo(span.Slice(1));
            return array;
        }

        public static IMemoryOwner<byte> EncodeInt32(int value, out Span<byte> span)
        {
            var size = 4;
            IMemoryOwner<byte> array = MemoryPool<byte>.Shared.Rent(size);
            span = array.Memory.Span.Slice(0, size);

            BinaryPrimitives.WriteInt32BigEndian(span, value);
            return array;
        }

        public static IMemoryOwner<byte> EncodeCounter(RocksDbStorage.Counter counter, ReadOnlySpan<byte> index,
            out Span<byte> span)
        {
            var size = 2 + index.Length;
            IMemoryOwner<byte> array = MemoryPool<byte>.Shared.Rent(size);
            span = array.Memory.Span.Slice(0, size);

            span[0] = (byte) RocksDbStorage.StoreType.Counter;
            span[1] = (byte) counter;
            index.CopyTo(span.Slice(2));
            return array;
        }
    }
}