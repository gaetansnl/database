using System;
using System.Collections;

namespace RDB.Core.Search
{
    public class EmptyDocIdSetEnumerator : IDocIdSetEnumerator
    {
        public bool MoveNext()
        {
            return false;
        }

        public void Reset()
        {
        }

        public int Current => Int32.MaxValue;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public long Cost => 0;
    }
}