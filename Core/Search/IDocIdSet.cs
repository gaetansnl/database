using System.Collections;

namespace RDB.Core.Search
{
    public interface IDocIdSet: IEnumerable
    {
        public IBitSet? Bits { get; }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        public new IDocIdSetEnumerator GetEnumerator();
    }
}