namespace KopiQueue;

/// <summary>Compares unique keys (design §5.6) by their bytes. Both drivers use it to dedupe a batch's keys.</summary>
internal sealed class UniqueKeyComparer : IEqualityComparer<byte[]>
{
    public static readonly UniqueKeyComparer Instance = new();

    public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);

    public int GetHashCode(byte[] obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        var hash = new HashCode();
        hash.AddBytes(obj);
        return hash.ToHashCode();
    }
}
