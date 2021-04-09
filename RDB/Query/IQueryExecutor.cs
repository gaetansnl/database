using System.Collections;
using RDB.Core.Search;

namespace RDB.Query
{
    public interface IQueryExecutor: IEnumerable
    {
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        public new IDocIdSetEnumerator GetEnumerator();
    }
}