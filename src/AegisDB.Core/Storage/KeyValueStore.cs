namespace AegisDB.Core.Storage;

public class KeyValueStore
{
    private readonly Dictionary<string, byte[]> _data = new();

    public void Put(string key, byte[] value)
    {
        _data[key] = value;
    }

    public byte[]? Get(string key)
    {
        return _data.GetValueOrDefault(key);
    }

    public bool Delete(string key)
    {
        return _data.Remove(key);
    }
}