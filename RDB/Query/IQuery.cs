using RDB.Storage;

namespace RDB.Query
{
    public interface IQuery
    {
        public IQueryExecutor GetExecutor(IStorageEngine engine);
    }
}