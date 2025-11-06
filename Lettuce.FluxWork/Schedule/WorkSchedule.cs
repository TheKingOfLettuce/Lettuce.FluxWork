using Lettuce.FluxWork.WorkItems;

namespace Lettuce.FluxWork.Schedule;

internal struct WorkSchedule {
    public DateTime DueDate => _dueDate;
    public bool IsRunning => _isRunning;
    public readonly WorkItem ScheduledWork;

    private bool _isRunning;
    private DateTime _dueDate;

    public WorkSchedule(WorkItem work) {
        ScheduledWork = work;
        _isRunning = false;
        _dueDate = DateTime.MinValue;
    }

    public void StartWork() => StartWork(DateTime.UtcNow);

    public void StartWork(DateTime startPoint) {
        _isRunning = true;
        _dueDate = startPoint + ScheduledWork.IntervalTime;
    }
}