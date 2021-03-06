using System;
using System.Collections.Generic;

namespace RDB.Core.Search
{
    public interface ITermsEnumerator: IEnumerator<ReadOnlyMemory<byte>>
    {
        public bool SeekExact(ReadOnlyMemory<byte> data);
        public bool SeekCeil(ReadOnlyMemory<byte> data);
        public int CurrentTermFrequency { get; }
        public IDocIdSetEnumerator CurrentTermDocs(ReadOnlyMemory<byte>? field);
    }
}