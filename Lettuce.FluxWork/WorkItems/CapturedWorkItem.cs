namespace Lettuce.FluxWork.WorkItems;

internal class CapturedWorkItem<T> : WorkItem {
    private T? _capturedData;

    public CapturedWorkItem(Func<object?, CancellationToken, ValueTask> work, TimeSpan interval) 
    : base(work, interval) {
    }

    public CapturedWorkItem(Func<object?, CancellationToken, ValueTask> work, TimeSpan interval, T? data) 
    : this(work, interval) {
        UpdateCapturedData(data);
    }

    public T? UpdateCapturedData(T? data) {
        T? oldData = _capturedData;
        _capturedData = data;
        return oldData;
    }

    public override async void PerformWorkAsync() {
        await PerformWorkAsync(_capturedData);
    }
}