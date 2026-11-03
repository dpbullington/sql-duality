namespace SqlDuality.Tests;

/// <summary>A disposable temporary directory for test isolation.</summary>
public sealed class TempDir : IDisposable
{
    public string Path { get; }

    public TempDir()
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(), "sqld-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
    }

    /// <summary>Combines a relative name onto the temp directory root.</summary>
    public string Combine(string name) => System.IO.Path.Combine(Path, name);

    public void Dispose()
    {
        try { Directory.Delete(Path, recursive: true); }
        catch { /* best effort cleanup */ }
    }
}
