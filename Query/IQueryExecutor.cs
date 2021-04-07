using System.Collections;
using Core.Search;

namespace Query
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