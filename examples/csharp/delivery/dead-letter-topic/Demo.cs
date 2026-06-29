namespace KubeMQ.GcpPubSub.Examples.Delivery;

/// <summary>
/// Console-output and assertion helpers shared by the delivery example programs.
///
/// Examples are runnable proofs, not demos: each prints clear human-readable
/// progress and MUST exit non-zero on any failed assertion or unexpected error.
/// <see cref="Require"/> throws <see cref="DemoFailure"/>, which
/// <see cref="RunAsync"/> turns into a non-zero process exit (the
/// SHARED-CONVENTIONS exit-code rule).
///
/// This helper is duplicated per variant directory on purpose: every variant is
/// a self-contained console project (no shared project reference), so a variant
/// builds and runs in isolation.
/// </summary>
internal static class Demo
{
    /// <summary>Prints a progress step, e.g. <c>[*] Created topic 'orders-ab12cd34'</c>.</summary>
    public static void Step(string message) => Console.WriteLine($"[*] {message}");

    /// <summary>Prints a send/produce action, e.g. <c>[x] Published id=...</c>.</summary>
    public static void Sent(string message) => Console.WriteLine($"[x] {message}");

    /// <summary>Prints a receive/observe action, e.g. <c>[v] Pulled '...'</c>.</summary>
    public static void Got(string message) => Console.WriteLine($"[v] {message}");

    /// <summary>Prints a final success banner.</summary>
    public static void Ok(string message) => Console.WriteLine($"[ok] {message}");

    /// <summary>Asserts a condition; throws <see cref="DemoFailure"/> (→ exit 1) when false.</summary>
    public static void Require(bool condition, string message)
    {
        if (!condition) throw new DemoFailure(message);
    }

    /// <summary>Asserts equality and reports both values on mismatch.</summary>
    public static void RequireEqual<T>(T expected, T actual, string what)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
            throw new DemoFailure($"{what}: expected '{expected}', got '{actual}'");
    }

    /// <summary>
    /// Runs the example body, mapping any <see cref="DemoFailure"/> or unexpected
    /// exception to a non-zero process exit.
    /// </summary>
    public static async Task<int> RunAsync(Func<Task> body)
    {
        try
        {
            await body();
            return 0;
        }
        catch (DemoFailure ex)
        {
            Console.Error.WriteLine($"[FAIL] {ex.Message}");
            return 1;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[ERROR] {ex.GetType().Name}: {ex.Message}");
            return 1;
        }
    }

    /// <summary>A short per-run suffix so concurrent runs use distinct resource ids / channels.</summary>
    public static string RunSuffix() => Guid.NewGuid().ToString("N")[..8];
}

/// <summary>Raised by <see cref="Demo.Require"/> on a failed assertion.</summary>
internal sealed class DemoFailure(string message) : Exception(message);
