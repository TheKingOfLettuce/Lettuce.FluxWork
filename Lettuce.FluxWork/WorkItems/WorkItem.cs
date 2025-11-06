namespace Lettuce.FluxWork.WorkItems;

internal class WorkItem {
    public TimeSpan IntervalTime => _intervalTime;

    private readonly Func<object?, CancellationToken, ValueTask> _work;
    private TimeSpan _intervalTime;
    private SemaphoreSlim? _overlapGate;
    private CancellationToken _cancelToken;

    public WorkItem(Func<object?, CancellationToken, ValueTask> work, TimeSpan interval, bool allowOverlap = false) {
        UpdateTime(interval);

        _work = work;
        if (!allowOverlap) {
            _overlapGate = new SemaphoreSlim(1);
        }
        _cancelToken = CancellationToken.None;
    }

    public void UpdateTime(TimeSpan newTime) {
        if (newTime < TimeSpan.Zero) {
            throw new ArgumentException("Provided time interval is negative when it should be 0 or greater", nameof(newTime));
        }
        _intervalTime = newTime;
    }

    public void UpdateCancelToken(CancellationToken token) {
        _cancelToken = token;
    }

    public Task PerformWork() {
        return Task.Run(PerformWorkAsync, _cancelToken);
    }

    public virtual async void PerformWorkAsync() {
        await PerformWorkAsync(null);
    }

    protected async Task PerformWorkAsync(object? data) {
        await PerformWorkAsync(data, _cancelToken);
    }

    protected async Task PerformWorkAsync(object? data, CancellationToken token) {
        if (_overlapGate != null) {
            try {
                await _overlapGate.WaitAsync(token);
            }
            catch (OperationCanceledException) {
                return;
            }
        }

        try {
            await _work.Invoke(data, token);
        }
        finally {
            _overlapGate?.Release();
        }
    }
}