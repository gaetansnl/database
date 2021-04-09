using System;
using System.Text;
using RDB.Core.Search;
using RDB.Core.Term;
using RDB.Storage;

namespace RDB.Query
{
    public class TermQuery: IQuery
    {
        protected Term Term;
        protected ReadOnlyMemory<byte>? Field;
        public TermQuery(Term term)
        {
            Term = term;
        }
        public TermQuery(string field, Term term)
        {
            Term = term;
            Field = Encoding.UTF8.GetBytes(field).AsMemory();
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
                    if (found) return termEnum.CurrentTermDocs(OuterQuery.Field);
                    return new EmptyDocIdSetEnumerator();                    
                }
            }
        }
    }
}