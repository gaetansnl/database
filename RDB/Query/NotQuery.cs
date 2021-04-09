using System;
using System.Collections;
using RDB.Core.Search;
using RDB.Storage;

namespace RDB.Query
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
                public int CurrentDocId = -1;
                public bool SubEnumeratorEnded;

                public NotQueryEnumerator(IQueryExecutor subExecutor, int maxDocs)
                {
                    SubExecutor = subExecutor;
                    SubEnumerator = subExecutor.GetEnumerator();
                    MaxDocs = maxDocs;
                }

                public bool AdvanceUntilNotAligned()
                {
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

                public bool MoveNext()
                {
                    if (CurrentDocId > -1) CurrentDocId++;
                    return AdvanceUntilNotAligned();
                }

                // public bool Advance(int target)
                // {
                //     SubEnumeratorEnded = !SubEnumerator.Advance(target);
                //     CurrentDocId = target;
                //
                //     return AdvanceUntilNotAligned();
                // }

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