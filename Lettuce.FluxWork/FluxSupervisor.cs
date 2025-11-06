using Lettuce.FluxWork.Exceptions;
using Lettuce.FluxWork.Schedule;
using Lettuce.FluxWork.WorkItems;

namespace Lettuce.FluxWork;

internal class FluxSupervisor {
    public AsyncAutoResetEvent DataModified => _workQueue.DataModified;

    private readonly ConcurrentPriorityQueue<WorkHandle, DateTime> _workQueue;
    private readonly Dictionary<WorkHandle, WorkItem> _workHandles;
    private readonly Dictionary<WorkHandle, WorkSchedule> _workSchedules;
    private readonly object _mainLock;

    public FluxSupervisor() {
        _workQueue = new ConcurrentPriorityQueue<WorkHandle, DateTime>();
        _workHandles = new Dictionary<WorkHandle, WorkItem>();
        _workSchedules = new Dictionary<WorkHandle, WorkSchedule>();
        _mainLock = new object();
    }

    public void RegisterWork(WorkHandle handle, WorkItem work) {
        lock (_mainLock) {
            RegisterWorkInternal(handle, work);
        }
    }

    private void RegisterWorkInternal(WorkHandle handle, WorkItem work) {
        _workHandles.Add(handle, work);
    }

    public void StartWork(WorkHandle handle, WorkSchedule schedule) {
        lock (_mainLock) {
            StartWorkInternal(handle, schedule);
        }
    }

    private void StartWorkInternal(WorkHandle handle, WorkSchedule schedule) {
        _workSchedules[handle] = schedule;
        _workQueue.Enqueue(handle, schedule.DueDate);
    }

    public void RegisterAndStart(WorkHandle handle, WorkItem work, WorkSchedule schedule) {
        lock (_mainLock) {
            RegisterWorkInternal(handle, work);
            StartWorkInternal(handle, schedule);
        }
    }

    public void StopWork(WorkHandle handle) {
        lock (_mainLock) {
            _ = StopWorkInternal(handle, out _, true);
        }
    }

    private bool StopWorkInternal(WorkHandle handle, out WorkSchedule schedule, bool thorwIfNoutFound = false) {
        if (!_workSchedules.Remove(handle, out schedule)) {
            if (!thorwIfNoutFound) {
                return false;
            }
            throw new FluxWorkException("Cannot remove work as it has not been added");
        }

        if (schedule.IsRunning) {
            _workQueue.Remove(handle);
        }

        return true;
    }

    public void RestartWork(WorkHandle handle, bool thorwIfNoutFound = false) {
        lock (_mainLock) {
            if (!StopWorkInternal(handle, out WorkSchedule schedule, thorwIfNoutFound)) {
                return;
            }

            schedule.StartWork();
            StartWorkInternal(handle, schedule);
        }
    }

    public void RemoveWork(WorkHandle handle) {
        lock (_mainLock) {
            RemoveWorkInternal(handle);
        }
    }

    private void RemoveWorkInternal(WorkHandle handle) {
        if (!_workHandles.Remove(handle, out _)) {
            throw new FluxWorkException("Cannot remove work as it has not been added");
        }

        _ = StopWorkInternal(handle, out _);
    }

    public WorkItem GetWorkItem(WorkHandle handle) {
        lock (_mainLock) {
            if (!_workHandles.TryGetValue(handle, out WorkItem? item)) {
                throw new FluxWorkException("Cannot get work item, work has not been registered");
            }

            return item;
        }
    }

    public async Task<(WorkHandle handle, WorkItem work, WorkSchedule schedule)> PeekEverythingAsync() {
        WorkHandle handle = await _workQueue.PeekAsync(true);
        WorkItem? work;
        WorkSchedule schedule;
        lock (_mainLock) {
            if (!_workHandles.TryGetValue(handle, out work)) {
                throw new FluxConcurrencyException("Queue modified after initial peek");
            }

            schedule = _workSchedules[handle];

            return (handle, work, schedule);
        }
    }
    
    public void RemoveEverything() {
        lock (_mainLock) {
            while (_workQueue.Count != 0) {
                WorkHandle handle = _workQueue.Dequeue();
                RemoveWorkInternal(handle);
            }
        }
    }
}