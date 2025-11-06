using Lettuce.FluxWork.Exceptions;

namespace Lettuce.FluxWork;

internal class AsyncAutoResetEvent
{
    private readonly SemaphoreSlim _sem = new(0, 1);

    public void Reset() {
        while (_sem.CurrentCount != 0) {
            _sem.Wait(0);
        }
    }

    public Task WaitAsync(CancellationToken token = default) => _sem.WaitAsync(token);

    public void Set() {
        if (_sem.CurrentCount == 0) {
            _sem.Release();
        }
    }
}

internal class ConcurrentPriorityQueue<T, TPriority> {
    public int Count {
        get {
            lock (_queueLock) {
                return _wrappedQueue.Count;
            }
        }
    }

    public AsyncAutoResetEvent DataModified => _dataModifySignal;

    #if NET9_0_OR_GREATER
        private readonly PriorityQueue<T, TPriority> _wrappedQueue;
    #else
        private PriorityQueue<T, TPriority> _wrappedQueue;
    #endif
    
    private readonly object _queueLock;
    private TaskCompletionSource _dataSignal;
    private AsyncAutoResetEvent _dataModifySignal;

    public ConcurrentPriorityQueue() {
        _wrappedQueue = new PriorityQueue<T, TPriority>();
        _queueLock = new object();
        _dataSignal = CreateTaskSource();
        _dataModifySignal = new AsyncAutoResetEvent();
    }

    public void Enqueue(T item, TPriority priority) {
        lock (_queueLock) {
            _wrappedQueue.Enqueue(item, priority);
            _dataModifySignal.Set();
            FlushDataSignal();
        }
    }

    public T Dequeue() {
        lock (_queueLock) {
            T toReturn = _wrappedQueue.Dequeue();
            HandleElementRemoved();
            return toReturn;
        }
    }

    public async Task<T> PeekAsync(bool resetDataSignal) {
        Task waitTask;
        lock (_queueLock) {
            if (_wrappedQueue.Count != 0) {
                if (resetDataSignal) {
                    _dataModifySignal.Reset();
                }
                return _wrappedQueue.Peek();
            }

            waitTask = _dataSignal.Task;
        }

        await waitTask;
        try {
            return Peek(resetDataSignal);
        }
        catch (InvalidOperationException e) {
            throw new FluxConcurrencyException("Queue modified during peek wait", e);
        }
    }

    public T Peek(bool resetDataSignal) {
        lock (_queueLock) {
            T toReturn = _wrappedQueue.Peek();
            if (resetDataSignal) {
                _dataModifySignal.Reset();
            }
            return toReturn;
        }
    }

    public bool TryPeek(out T? item, out TPriority? priority) {
        lock (_queueLock) {
            return _wrappedQueue.TryPeek(out item, out priority);
        }
    }

    public bool Remove(T item) {
        lock (_queueLock) {
            #if NET9_0_OR_GREATER
                bool result = _wrappedQueue.Remove(item, out _, out _);
            #else
                bool result = SlowRemove(item);
            #endif
            if (result) {
                HandleElementRemoved();
            }
            return result;
        }
    }

    #if !NET9_0_OR_GREATER
    private bool SlowRemove(T item) {
        var newQueue = new PriorityQueue<T, TPriority>();
        bool didRemove = false;
        while (_wrappedQueue.TryDequeue(out T? element, out TPriority? priority)) {
            if (!ReferenceEquals(element, item)) {
                newQueue.Enqueue(element, priority);
            }
            else {
                didRemove = true;
            }
        }

        _wrappedQueue = newQueue;
        return didRemove;
    }
    #endif

    private void HandleElementRemoved() {
        if (_wrappedQueue.Count == 0) {
            FlushAndResetDataSignal();
        }
        _dataModifySignal.Set();
    }

    private void FlushDataSignal() {
        if (!_dataSignal.Task.IsCompleted) {
            _dataSignal.TrySetResult();
        }
    }

    private void FlushAndResetDataSignal() {
        FlushDataSignal();
        _dataSignal = CreateTaskSource();
    }

    private static TaskCompletionSource CreateTaskSource() {
        return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}