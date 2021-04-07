using System;
using System.Collections;
using System.Collections.Generic;
using Core.Search;
using Core.Utils;
using Storage;

namespace Query
{
    public class OrQuery : IQuery
    {
        protected List<IQuery> Queries;

        public OrQuery(List<IQuery> queries)
        {
            Queries = queries;
        }

        public IQueryExecutor GetExecutor(IStorageEngine engine)
        {
            return new OrQueryExecuter(engine, this);
        }

        class OrQueryExecuter : IQueryExecutor
        {
            protected IStorageEngine Engine;
            protected OrQuery OuterQuery;
            protected List<IQueryExecutor> Executors = new();

            public OrQueryExecuter(IStorageEngine engine, OrQuery outerQuery)
            {
                Engine = engine;
                OuterQuery = outerQuery;
                foreach (var query in outerQuery.Queries) Executors.Add(query.GetExecutor(engine));
            }

            public IDocIdSetEnumerator GetEnumerator()
            {
                return new OrQueryEnumerator(Executors);
            }

            class OrQueryEnumerator : IDocIdSetEnumerator
            {
                protected readonly PriorityQueue<IDocIdSetEnumerator, int> Sorted = new();
                protected List<IDocIdSetEnumerator> Enumerators = new();
                protected int CurrentDocId = -1;

                public OrQueryEnumerator(List<IQueryExecutor> executors)
                {
                    foreach (var executor in executors)
                    {
                        var enumerator = executor.GetEnumerator();
                        Enumerators.Add(enumerator);
                        Sorted.Enqueue(enumerator, -1);
                        Cost = Math.Max(Cost, enumerator.Cost);
                    }
                }

                protected void ReplaceRoot()
                {
                    var root = Sorted.Dequeue();
                    Sorted.Enqueue(root, root.Current);
                }

                public bool MoveNext()
                {
                    do
                    {
                        var root = Sorted.Peek();
                        if (root.MoveNext())
                        {
                            ReplaceRoot();
                        }
                        else
                        {
                            Sorted.Dequeue();
                        }
                    } while (Sorted.Count > 0 && Sorted.Peek().Current == CurrentDocId);

                    if (Sorted.Count > 0)
                    {
                        CurrentDocId = Sorted.Peek().Current;
                        return true;
                    }
                    return false;
                }

                public void Reset()
                {
                    throw new NotImplementedException();
                }

                public int Current => CurrentDocId;

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    foreach (var enumerator in Enumerators) enumerator.Dispose();
                }

                public long Cost { get; }
            }
        }
    }
}