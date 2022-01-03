using System;
using System.Collections;

namespace RDB.Core.Search
{
    public class EmptyDocIdSetEnumerator : IDocIdSetEnumerator
    {
        public bool MoveNext()
        {
            Current = Int32.MaxValue;
            return false;
        }

        public void Reset()
        {
        }


        public int Current { get; protected set; } = -1;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public long Cost => 0;
    }
}