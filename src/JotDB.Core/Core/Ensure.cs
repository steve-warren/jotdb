using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace JotDB.Core;

/// <summary>
/// Contains methods for ensuring that arguments passed to a method meet certain criteria.
/// </summary>
internal static class Ensure
{
    /// <summary>
    /// Throws an <see cref="ArgumentException"/> if the specified string is null, empty, or consists only of white-space characters.
    /// </summary>
    /// <param name="argument">The string to check.</param>
    /// <param name="message">The error message to include in the exception if the check fails.</param>
    /// <param name="paramName">The name of the parameter being checked.</param>
    /// <exception cref="ArgumentException">Thrown if <paramref name="argument"/> is null, empty, or consists only of white-space characters.</exception>
    [DebuggerHidden]
    public static void NotNullOrWhiteSpace(
        [NotNull]
        string? argument,
        string? message = null,
        [CallerArgumentExpression(nameof(argument))]
        string? paramName = null)
    {
        if (string.IsNullOrWhiteSpace(argument))
            throw new ArgumentException(message, paramName);
    }

    /// <summary>
    /// Throws an <see cref="ArgumentNullException"/> if the specified argument is null.
    /// </summary>
    /// <param name="argument">The argument to check for null.</param>
    /// <param name="message">The message to include in the exception if the argument is null.</param>
    /// <param name="paramName">The name of the parameter that is being checked for null.</param>
    public static void NotNull(
        [NotNull]
        object? argument,
        string? message = null,
        [CallerArgumentExpression(nameof(argument))]
        string? paramName = null)
    {
        if (argument is null)
            throw new ArgumentNullException(paramName, message);
    }

    public static void Equals<T>(T left, T right, string? message = null)
    {
        if (object.Equals(left, right) is false)
            throw new ArgumentException(message);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void That(bool expression, string? because = null)
    {
        if (expression is false)
            throw new ArgumentException(because);
    }
}
