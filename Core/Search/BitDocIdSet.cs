using System.Collections;

namespace Core.Search
{
    public class BitDocIdSet : IDocIdSet
    {
        public BitDocIdSet(IBitSet bits)
        {
            Bits = bits;
        }

        public IBitSet Bits { get; }

        IDocIdSetEnumerator IDocIdSet.GetEnumerator()
        {
            return new BitDocIdSetEnumerator(Bits);
        }

        class BitDocIdSetEnumerator : IDocIdSetEnumerator
        {
            protected IBitSet BitSet;
            protected int CurrentIndex = -1;

            public BitDocIdSetEnumerator(IBitSet bitSet)
            {
                BitSet = bitSet;
            }

            public bool MoveNext()
            {
                bool endReached;
                do
                {
                    CurrentIndex++;
                } while ((endReached = CurrentIndex < BitSet.Length) && !BitSet.Get(CurrentIndex));

                return endReached;
            }

            public bool Advance(int target)
            {
                CurrentIndex = target - 1;
                return MoveNext();
            }

            public void Reset()
            {
                CurrentIndex = -1;
            }

            public int Current => CurrentIndex;

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }

            public long Cost => BitSet.Length;
        }
    }
}