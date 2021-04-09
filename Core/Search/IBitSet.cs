namespace RDB.Core.Search
{
    public interface IBitSet
    {
        public bool Get(int index);
        public int Length { get; }
    }
}