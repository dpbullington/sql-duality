using System.Runtime.CompilerServices;

namespace SqlDuality.Core.Logging;

/// <summary>
/// CRC-32C (Castagnoli, polynomial 0x1EDC6F41) implemented in pure managed code.
///
/// <para>
/// We deliberately avoid a dependency on <c>System.IO.Hashing</c> so the core engine
/// ships with zero external packages and stays trivially AOT/trim compatible. The
/// Castagnoli polynomial is used because it has strong error-detection properties for
/// the short, framed records the durable log produces and is the de-facto choice for
/// storage checksums.
/// </para>
///
/// <para>This is a table-driven software implementation; it does not use hardware intrinsics.</para>
/// </summary>
public static class Crc32C
{
    private const uint Polynomial = 0x82F63B78; // reflected form of 0x1EDC6F41
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint crc = i;
            for (int bit = 0; bit < 8; bit++)
            {
                crc = (crc & 1) != 0 ? (crc >> 1) ^ Polynomial : crc >> 1;
            }
            table[i] = crc;
        }
        return table;
    }

    /// <summary>Computes the CRC-32C of <paramref name="data"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Compute(ReadOnlySpan<byte> data) => Append(0u, data);

    /// <summary>
    /// Continues a running CRC-32C. Pass <paramref name="crc"/> as <c>0</c> to start, or the
    /// result of a previous call to hash additional chunks.
    /// </summary>
    public static uint Append(uint crc, ReadOnlySpan<byte> data)
    {
        crc = ~crc;
        foreach (byte b in data)
        {
            crc = Table[(byte)(crc ^ b)] ^ (crc >> 8);
        }
        return ~crc;
    }
}
