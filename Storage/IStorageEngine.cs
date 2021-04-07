using System;
using Core.Search;

namespace Storage
{
    public interface IStorageEngine
    {
        public ITermsEnumerator GetTermsEnumerator();
        public int DocCount { get; }
    }
}