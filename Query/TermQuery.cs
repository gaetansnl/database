using Core.Search;
using Core.Term;
using Storage;

namespace Query
{
    public class TermQuery: IQuery
    {
        protected Term Term;

        public TermQuery(Term term)
        {
            Term = term;
        }

        public IQueryExecutor GetExecutor(IStorageEngine engine)
        {
            return new TermQueryExecutor(engine, this);
        }

        class TermQueryExecutor: IQueryExecutor
        {
            protected IStorageEngine Engine;
            protected TermQuery OuterQuery;

            public TermQueryExecutor(IStorageEngine engine, TermQuery outerQuery)
            {
                Engine = engine;
                OuterQuery = outerQuery;
            }

            public IDocIdSetEnumerator GetEnumerator()
            {
                using (var termEnum = Engine.GetTermsEnumerator())
                {
                    var found = termEnum.SeekExact(OuterQuery.Term.Data);
                    if (found) return termEnum.CurrentTermDocs();
                    return new EmptyDocIdSetEnumerator();                    
                }
            }
        }
    }
}