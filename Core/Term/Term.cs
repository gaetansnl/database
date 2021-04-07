using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Core.Term
{
    public struct Term : IDisposable
    {
        private readonly IMemoryOwner<byte>? _owner;
        public ReadOnlyMemory<byte> Data { get; }
        public TermType Type { get; }

        public Term(ReadOnlyMemory<byte> data)
        {
            Data = data;
            _owner = null;
            Type = Data.Span[0] switch
            {
                0 => TermType.Null,
                1 => TermType.Boolean,
                2 => TermType.String,
                3 => TermType.Int,
                4 => TermType.Long,
                5 => TermType.Float,
                6 => TermType.Double,
                _ => throw new ArgumentOutOfRangeException()
            };
        }

        private Term(TermType type, ReadOnlyMemory<byte> data, IMemoryOwner<byte>? owner = null)
        {
            Type = type;
            Data = data;
            _owner = owner;
        }

        public static Term FromBool(bool value)
        {
            var owner = MemoryPool<byte>.Shared.Rent(2);
            owner.Memory.Span[0] = (byte) TermType.Boolean;
            owner.Memory.Span[1] = (byte)(value ? 1 : 0);
            return new Term(TermType.Boolean, owner.Memory.Slice(0, 2), owner);
        }

        public static Term FromNull()
        {
            var owner = MemoryPool<byte>.Shared.Rent(1);
            owner.Memory.Span[0] = (byte) TermType.Null;
            return new Term(TermType.Null, owner.Memory.Slice(0, 1), owner);
        }

        public static Term FromLong(long value)
        {
            var owner = MemoryPool<byte>.Shared.Rent(8 + 1);
            owner.Memory.Span[0] = (byte) TermType.Long;
            BinaryPrimitives.WriteInt64BigEndian(owner.Memory.Span.Slice(1), value);
            return new Term(TermType.Long, owner.Memory.Slice(0, 8 + 1), owner);
        }

        public static Term FromDouble(double value)
        {
            var owner = MemoryPool<byte>.Shared.Rent(8 + 1);
            owner.Memory.Span[0] = (byte) TermType.Double;
            BinaryPrimitives.WriteDoubleBigEndian(owner.Memory.Span.Slice(1), value);
            return new Term(TermType.Double, owner.Memory.Slice(0, 8 + 1), owner);
        }

        public static Term FromString(string value)
        {
            var maxByteCount = Encoding.UTF8.GetMaxByteCount(value.Length);
            var owner = MemoryPool<byte>.Shared.Rent(maxByteCount + 1);

            owner.Memory.Span[0] = (byte) TermType.String;
            var realCount = Encoding.UTF8.GetBytes(value, owner.Memory.Span.Slice(1));

            return new Term(TermType.String, owner.Memory.Slice(0, realCount + 1), owner);
        }

        public ReadOnlySpan<byte> InnerData => Data.Span.Slice(1);

        public void Dispose()
        {
            _owner?.Dispose();
        }
    }
}