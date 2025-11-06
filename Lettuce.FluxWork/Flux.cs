using System.Diagnostics.CodeAnalysis;
using Lettuce.FluxWork.Exceptions;
using Lettuce.FluxWork.Schedule;
using Lettuce.FluxWork.WorkItems;

namespace Lettuce.FluxWork;

/// <summary>
/// The main class to schedule, pause, or remove work
/// </summary>
public static class Flux {
    /// <summary>
    /// Flag to represent if our main schedule loop is running to perform work
    /// </summary>
    public static bool IsScheduleRunning => _scheduleLoop.Status == TaskStatus.Running;

    private static readonly FluxSupervisor _supervisor = new FluxSupervisor();
    private static CancellationTokenSource _mainJobToken;
    private static Task _scheduleLoop;

    static Flux() {
        StartScheduleLoop();
    }

    /// <summary>
    /// Starts up our main schedule loop if its not already started
    /// </summary>
    [MemberNotNull(nameof(_scheduleLoop))]
    [MemberNotNull(nameof(_mainJobToken))]
    public static void StartScheduleLoop() {
        if (_scheduleLoop != default && IsScheduleRunning) {
            #pragma warning disable CS8774 // Member must have a non-null value when exiting.
            return;
            #pragma warning restore CS8774 // if we are running, it means we have a task
        }

        _mainJobToken = new CancellationTokenSource();
        _scheduleLoop = Task.Run(() => MainScheduleLoop(_mainJobToken.Token));
    }
    
    /// <summary>
    /// Stops our main schedule loop if its not already stopped
    /// </summary>
    public static void StopScheduleLoop() {
        _mainJobToken.Cancel();
    }

    /// <summary>
    /// Adds a simple <see cref="Action"/>
    /// </summary>
    /// <param name="work">the work to be done</param>
    /// <param name="time">how often should the work run</param>
    /// <param name="start">if the work should be started immediately via <see cref="StartWork"/></param>
    /// <returns>the <see cref="WorkHandle"/> to use for interacting with the work</returns>
    public static WorkHandle Schedule(Action work, TimeSpan time, bool start = true) {
        WorkItem workItem = new WorkItem((_, _) => {
            work.Invoke();
            return ValueTask.CompletedTask;
        }, time);

        return Schedule(workItem, start);
    }

    /// <summary>
    /// Adds a simple <see cref="Action"/> that takes captured data to use during the work
    /// </summary>
    /// <param name="work">the work to be done</param>
    /// <param name="data">the captured data to use when performing work</param>
    /// <param name="time">how often should the work run</param>
    /// <param name="start">if the work should be started immediately via <see cref="StartWork"/></param>
    /// <returns>the <see cref="WorkHandle"/> to use for interacting with the work</returns>
    public static WorkHandle Schedule<T>(Action<T> work, T? data, TimeSpan time, bool start = true) {
        CapturedWorkItem<T> workItem = new CapturedWorkItem<T>((capturedData, _) => {
            work.Invoke((T?)capturedData);
            return ValueTask.CompletedTask;
        }, time, data);
        workItem.UpdateCapturedData(data);

        return Schedule(workItem, start);
    }

    /// <summary>
    /// Adds work that can run asynchronously 
    /// </summary>
    /// <param name="work">the work to be done</param>
    /// <param name="time">how often should the work run</param>
    /// <param name="start">if the work should be started immediately via <see cref="StartWork"/></param>
    /// <returns>the <see cref="WorkHandle"/> to use for interacting with the work</returns>
    public static WorkHandle Schedule(Func<Task> work, TimeSpan time, bool start = true) {
        WorkItem workItem = new WorkItem(async (_, _) => {
            await work.Invoke();
        }, time);

        return Schedule(workItem, start);
    }

    /// <summary>
    /// Adds work that can run asynchronously that takes captured data to use during the work
    /// </summary>
    /// <param name="work">the work to be done</param>
    /// <param name="data">the captured data to use when performing work</param>
    /// <param name="time">how often should the work run</param>
    /// <param name="start">if the work should be started immediately via <see cref="StartWork"/></param>
    /// <returns>the <see cref="WorkHandle"/> to use for interacting with the work</returns>
    public static WorkHandle Schedule<T>(Func<T, Task> work, T? data, TimeSpan time, bool start = true) {
        CapturedWorkItem<T> workItem = new CapturedWorkItem<T>(async (capturedData, _) => {
            await work.Invoke((T?)capturedData);
        }, time);
        workItem.UpdateCapturedData(data);

        return Schedule(workItem, start);
    }

    /// <summary>
    /// Adds work that can run asynchronously with a <see cref="CancellationToken"/> to cancel the work gracefully
    /// </summary>
    /// <param name="work">the work to be done</param>
    /// <param name="cancellationToken">the token to use for cancellation</param>
    /// <param name="time">how often should the work run</param>
    /// <param name="start">if the work should be started immediately via <see cref="StartWork"/></param>
    /// <returns>the <see cref="WorkHandle"/> to use for interacting with the work</returns>
    public static WorkHandle Schedule(Func<CancellationToken, Task> work, CancellationToken cancellationToken, TimeSpan time, bool start = true) {
        WorkItem workItem = new WorkItem(async (_, cancelToken) => {
            await work.Invoke(cancelToken);
        }, time);
        workItem.UpdateCancelToken(cancellationToken);

        return Schedule(workItem, start);
    }

    /// <summary>
    /// Adds work that can run asynchronously with a <see cref="CancellationToken"/> to cancel the work gracefully
    /// as well as captured data to use during the work
    /// </summary>
    /// <param name="work">the work to be done</param>
    /// <param name="cancellationToken">the token to use for cancellation</param>
    /// <param name="data">the captured data to use when performing work</param>
    /// <param name="time">how often should the work run</param>
    /// <param name="start">if the work should be started immediately via <see cref="StartWork"/></param>
    /// <returns>the <see cref="WorkHandle"/> to use for interacting with the work</returns>
    public static WorkHandle Schedule<T>(Func<T, CancellationToken, Task> work, T? data, CancellationToken cancellationToken, TimeSpan time, bool start = true) {
        CapturedWorkItem<T> workItem = new CapturedWorkItem<T>(async (capturedData, cancelToken) => {
            await work.Invoke((T?)capturedData, cancelToken);
        }, time);
        workItem.UpdateCancelToken(cancellationToken);
        workItem.UpdateCapturedData(data);

        return Schedule(workItem, start);
    }

    private static WorkHandle Schedule(WorkItem workItem, bool start = true) {
        WorkHandle workHandle = new WorkHandle();
        _supervisor.RegisterWork(workHandle, workItem);
        if (start) {
            StartWork(workHandle);
        }

        return workHandle;
    }

    /// <summary>
    /// Starts the associated work on its schedule
    /// </summary>
    /// <param name="handle">the handle to use for the work in Flux</param>
    public static void StartWork(WorkHandle handle) {
        WorkSchedule newSchedule = new WorkSchedule(_supervisor.GetWorkItem(handle));
        newSchedule.StartWork();
        _supervisor.StartWork(handle, newSchedule);
    }

    /// <summary>
    /// Pauses the work in the schedule, preventing it from running till started again
    /// </summary>
    /// <param name="handle">the handle to use for the work in Flux</param>
    public static void PauseWork(WorkHandle handle) {
        _supervisor.StopWork(handle);
    }

    /// <summary>
    /// Completely removes the associated work from the schedule, making the given <see cref="WorkHandle"/> dead
    /// </summary>
    /// <param name="handle">the handle to use for the work in Flux</param>
    public static void RemoveWork(WorkHandle handle) {
        _supervisor.RemoveWork(handle);
    }

    /// <summary>
    /// Removes all work from the schedule, making all <see cref="WorkHandle"/> dead
    /// </summary>
    public static void RemoveAllWork() {
        _supervisor.RemoveEverything();
    }

    private static async Task MainScheduleLoop(CancellationToken token) {
        while (!token.IsCancellationRequested) {
            (WorkHandle Handle, WorkItem Work, WorkSchedule Schedule) nextWork;
            try {
                nextWork = await _supervisor.PeekEverythingAsync();
            }
            catch (FluxConcurrencyException) {
                continue;
            }

            TimeSpan dueTime = nextWork.Schedule.DueDate - DateTime.UtcNow;
            using CancellationTokenSource newWorkTaskToken = new CancellationTokenSource();
            Task newWorkTask = Task.Run(async () => await _supervisor.DataModified.WaitAsync(newWorkTaskToken.Token), newWorkTaskToken.Token);
            if (newWorkTask.IsCompleted) {
                continue;
            }

            if (dueTime <= TimeSpan.Zero) {
                PerformWork(nextWork.Work, nextWork.Handle);
                continue;
            }

            Task waitTask = Task.Delay(dueTime, token);
            await Task.WhenAny(waitTask, newWorkTask);
            if (waitTask.IsCompletedSuccessfully) {
                PerformWork(nextWork.Work, nextWork.Handle);
                continue;
            }
        }
    }

    private static void PerformWork(WorkItem work, WorkHandle handle, bool shouldRestart = true) {
        _ = Task.Run(work.PerformWorkAsync);
        if (shouldRestart)
            _supervisor.RestartWork(handle);
    }
}