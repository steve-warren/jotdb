namespace JotDB.Threading;

/// <summary>
/// Represents an asynchronous awaitable utility for signaling completion, fault, or cancellation
/// in an asynchronous operation. The class ties completion handling with cancellation tokens
/// to provide a mechanism for synchronization and handling task state transitions.
/// </summary>
/// <remarks>
/// This class is designed to provide controlled task signaling between various components
/// in asynchronous workflows. It is particularly useful in scenarios where tasks need
/// to await completion, handle exceptions or be aware of cancellation requests.
/// The class ensures thread safety and proper resource cleanup.
/// </remarks>
/// <threadsafety>
/// This class is thread-safe and multiple threads can safely interact with its public members.
/// </threadsafety>
/// <example>
/// This class can be utilized as a lightweight awaitable object for synchronization or task transition
/// in high-performance applications requiring direct control of asynchronous behavior.
/// </example>
/// <note>
/// Always ensure the object is disposed properly to release allocated resources and unregister from
/// any cancellation token sources.
/// </note>
public sealed class AsyncAwaiter : IDisposable
{
    private readonly CancellationToken _token;
    private readonly CancellationTokenSource _cts;
    private readonly CancellationTokenRegistration _ctr;

    private readonly TaskCompletionSource _tcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public AsyncAwaiter(CancellationToken cancellationToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        _ctr = _cts.Token.Register(() => _tcs.TrySetCanceled());
        _token = _cts.Token;
    }

    public AsyncAwaiter(int timeout = -1,
        CancellationToken cancellationToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        _ctr = _cts.Token.Register(() => _tcs.TrySetCanceled());
        _token = _cts.Token;

        _cts.CancelAfter(timeout);
    }

    ~AsyncAwaiter()
    {
        Dispose();
    }

    /// <summary>
    /// Gets a task that represents the state of completion for the AsyncAwaiter.
    /// </summary>
    /// <remarks>
    /// This property returns a Task that completes when the AsyncAwaiter signals its completion.
    /// It can be awaited to coordinate asynchronous operations that depend on the completion signal.
    /// </remarks>
    public Task CompletedTask => _tcs.Task;

    public bool IsSet => _tcs.Task.IsCompleted;

    public void SignalCompletion()
    {
        _tcs.TrySetResult();
    }

    /// <summary>
    /// Signals the completion of the current task when the specified task is completed.
    /// </summary>
    /// <param name="after">The task after which the completion signal will be triggered.</param>
    public void SignalCompletionWhen(Task after)
    {
        _ = after.ContinueWith((_, o) =>
            {
                var tcs = (TaskCompletionSource)o;
                tcs.TrySetResult();
            }, _tcs, _token, TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Current);
    }

    public void SignalFault(Exception exception)
    {
        _tcs.TrySetException(exception);
    }

    public Task WaitForSignalAsync()
    {
        _token.ThrowIfCancellationRequested();
        return _tcs.Task;
    }

    public void Dispose()
    {
        _cts.Cancel();
        _ctr.Dispose();
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }
}