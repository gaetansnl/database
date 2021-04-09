using System.Text.Json;
using RDB.Core.Search;

namespace RDB.Storage
{
    public interface IStorageEngine
    {
        public ITermsEnumerator GetTermsEnumerator();
        public int DocCount { get; }
        public int Index(JsonDocument doc);

        public void Clear();
    }
}