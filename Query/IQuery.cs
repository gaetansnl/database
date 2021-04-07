using Storage;

namespace Query
{
    public interface IQuery
    {
        public IQueryExecutor GetExecutor(IStorageEngine engine);
    }
}