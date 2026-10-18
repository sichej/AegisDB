namespace AegisDB.Core.Storage;

public class KeyValueStore
{
    private readonly Dictionary<string, byte[]> _data = new();
    private readonly object _lock = new();

    public void Put(string key, byte[] value)
    {
        lock (_lock)
        {
            // ToArray for Defensive Copying
            // Create a defensive copy of the value to prevent external modifications
            _data[key] = value.ToArray();
        }
    }

    public byte[]? Get(string key)
    {
        lock (_lock)
        {
            // ToArray for Defensive Copying
            // Create a defensive copy of the value to prevent external modifications
            return _data.TryGetValue(key, out var value)
                ? value.ToArray()
                : null;
        }
    }

    public bool Delete(string key)
    {
        lock (_lock)
        {
            return _data.Remove(key);
        }
    }
}