using System;
using System.Collections;
using Core.Search;
using Storage;

namespace Query
{
    public class NotQuery : IQuery
    {
        protected IQuery Subquery;

        public NotQuery(IQuery subquery)
        {
            Subquery = subquery;
        }

        public IQueryExecutor GetExecutor(IStorageEngine engine)
        {
            return new NotQueryExecutor(engine, this);
        }

        class NotQueryExecutor : IQueryExecutor
        {
            protected IStorageEngine Engine;
            protected NotQuery OuterQuery;

            public NotQueryExecutor(IStorageEngine engine, NotQuery outerQuery)
            {
                Engine = engine;
                OuterQuery = outerQuery;
            }

            public IDocIdSetEnumerator GetEnumerator()
            {
                return new NotQueryEnumerator(OuterQuery.Subquery.GetExecutor(Engine), Engine.DocCount);
            }

            class NotQueryEnumerator : IDocIdSetEnumerator
            {
                public IQueryExecutor SubExecutor;
                public IDocIdSetEnumerator SubEnumerator;
                public int MaxDocs;
                public int CurrentDocId = -2;
                public bool SubEnumeratorEnded;

                public NotQueryEnumerator(IQueryExecutor subExecutor, int maxDocs)
                {
                    SubExecutor = subExecutor;
                    SubEnumerator = subExecutor.GetEnumerator();
                    MaxDocs = maxDocs;
                }

                public bool MoveNext()
                {
                    CurrentDocId++;

                    while (CurrentDocId == SubEnumerator.Current && !SubEnumeratorEnded)
                    {
                        CurrentDocId++;
                        SubEnumeratorEnded = !SubEnumerator.MoveNext();
                    }

                    if (CurrentDocId >= MaxDocs)
                    {
                        CurrentDocId = Int32.MaxValue;
                        return false;
                    }

                    return true;
                }

                public void Reset()
                {
                    throw new NotImplementedException();
                }

                public int Current => CurrentDocId;

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    SubEnumerator.Dispose();
                }

                //Todo: bad cost
                public long Cost => SubEnumerator.Cost;
            }
        }
    }
}