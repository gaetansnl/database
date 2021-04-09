using System.Collections.Generic;

namespace RDB.Core.Search
{
    public interface IDocIdSetEnumerator: IEnumerator<int>
    {
        public bool Advance(int target) {
            bool endReached;
            do { } while ((endReached = MoveNext()) && Current < target);
            return endReached;
        }

        long Cost
        {
            get;
        }
    }
}