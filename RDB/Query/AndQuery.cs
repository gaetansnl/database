using System;
using System.Collections;
using System.Collections.Generic;
using RDB.Core.Search;
using RDB.Storage;

namespace RDB.Query
{
    public class AndQuery : IQuery
    {
        protected List<IQuery> Queries;

        public AndQuery(List<IQuery> queries)
        {
            Queries = queries;
        }

        public IQueryExecutor GetExecutor(IStorageEngine engine)
        {
            return new AndQueryExecutor(engine, this);
        }

        class AndQueryExecutor : IQueryExecutor
        {
            protected IStorageEngine Engine;
            protected AndQuery OuterQuery;
            protected List<IQueryExecutor> Executors = new();

            public AndQueryExecutor(IStorageEngine engine, AndQuery outerQuery)
            {
                Engine = engine;
                OuterQuery = outerQuery;
                foreach (var query in outerQuery.Queries) Executors.Add(query.GetExecutor(engine));
            }

            public IDocIdSetEnumerator GetEnumerator()
            {
                return new AndQueryEnumerator(Executors);
            }

            class AndQueryEnumerator: IDocIdSetEnumerator
            {
                protected IDocIdSetEnumerator Lead;
                protected List<IDocIdSetEnumerator> Enumerators = new();
                public AndQueryEnumerator(List<IQueryExecutor> executors)
                {
                    foreach (var executor in executors) Enumerators.Add(executor.GetEnumerator());
                    Enumerators.Sort(delegate(IDocIdSetEnumerator p1, IDocIdSetEnumerator p2)
                    {
                        return p1.Cost.CompareTo(p2.Cost);
                    });
                    Lead = Enumerators[0];
                }

                protected bool AdvanceLeadUntilAllAligned()
                {
                    var doc = Lead.Current;
                    for (int index = 1; index < Enumerators.Count; ++index)
                    {
                        if (Enumerators[index].Current < doc)
                        {
                            Enumerators[index].Advance(doc);
                            if (Enumerators[index].Current > doc)
                            {
                                if (!Lead.Advance(Enumerators[index].Current)) return false;
                                doc = Lead.Current;
                                index = 0; // 0 Because it will be incremented next loop to 1
                            }
                        }
                    }
                    return true;
                }

                public bool MoveNext()
                {
                    return Lead.MoveNext() && AdvanceLeadUntilAllAligned();
                }

                public bool Advance(int target)
                {
                    return Lead.Advance(target) && AdvanceLeadUntilAllAligned();
                }

                public void Reset()
                {
                    throw new NotImplementedException();
                }

                public int Current => Lead.Current;

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    foreach (var enumerator in Enumerators) enumerator.Dispose();
                }

                public long Cost => Lead.Cost;
            }
        }
    }
}