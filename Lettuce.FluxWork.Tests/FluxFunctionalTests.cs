namespace Lettuce.FluxWork.Tests;

[TestClass]
public sealed class FluxFunctionalTests
{
    [TestCleanup]
    public void TestCleanUp() {
        Flux.RemoveAllWork();
    }

    [AssemblyCleanup]
    public static void AllTestCleanup() {
        Flux.RemoveAllWork();
        Flux.StopScheduleLoop();
    }

    [TestMethod]
    public async Task TestBasicSchedule_ShouldRun3Times() {
        using CounterHelper helper = new CounterHelper(3);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(.5));
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_WithLambda_ShouldRun3Times() {
        using CounterHelper helper = new CounterHelper(3);
        WorkHandle handle = Flux.Schedule(() => helper.IncrementCount(), TimeSpan.FromSeconds(.5));
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task Test2BasicSchedules_ShouldEachRun3Times() {
        using CounterHelper helper = new CounterHelper(3);
        using CounterHelper helper2 = new CounterHelper(3);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(.5));
        WorkHandle handle2 = Flux.Schedule(helper2.IncrementCount, TimeSpan.FromSeconds(1));
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        await helper2.CountSignal.WaitAsync();
        Flux.RemoveWork(handle2);
        Assert.IsFalse(helper.IsOver);
        Assert.IsFalse(helper2.IsOver);
    }

    [TestMethod]
    public void TestRemoveSchedule_ShouldNotThrow() {
        static void EmptyMethod() {}
        WorkHandle handle = Flux.Schedule(EmptyMethod, TimeSpan.FromSeconds(2));
        try {
            Flux.RemoveWork(handle);
        }
        catch (Exception e) {
            Assert.Fail($"Should not have thrown but got exception {e}");
        }
    }

    [TestMethod]
    public async Task TestRemoveSchedule_ThenAddSchedule_ShouldRun() {
        using CounterHelper helper = new CounterHelper(1);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(.5));
        Flux.RemoveWork(handle);
        Thread.Sleep(TimeSpan.FromSeconds(2));
        handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(.5));
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_ThenRemove_ShouldNotRunAgain() {
        using CounterHelper helper = new CounterHelper(3);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(.5));
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_WithCapturedPrimitiveData_ShouldRun3Times() {
        using CounterHelper helper = new CounterHelper(30);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, 10, TimeSpan.FromSeconds(.5));
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_WithCapturedReferenceData_ShouldRun3Times() {
        using CounterHelper helper = new CounterHelper(30);
        OffsetHelper offsetHelper = new OffsetHelper();
        offsetHelper.OffSet = 10;
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, offsetHelper, TimeSpan.FromSeconds(.5));
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_WithCapturedReferenceData_ThenModified_ShouldRun3Times() {
        using CounterHelper helper = new CounterHelper(40);
        OffsetHelper offsetHelper = new OffsetHelper();
        offsetHelper.OffSet = 10;
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, offsetHelper, TimeSpan.FromSeconds(.5));
        Assert.IsTrue(await helper.WaitUntilCount(20, TimeSpan.FromSeconds(10)));
        offsetHelper.OffSet = 20;
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_WithToken_ThenCancel_ShouldRun2Times() {
        using CounterHelper helper = new CounterHelper(2);
        using CancellationTokenSource cancellationToken = new CancellationTokenSource();
        WorkHandle handle = Flux.Schedule(helper.IncrementCountAsync, cancellationToken.Token, TimeSpan.FromSeconds(.5));
        await helper.CountSignal.WaitAsync();
        cancellationToken.Cancel();
        await Task.Delay(TimeSpan.FromSeconds(1));
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestRemoveAll_ShouldEachRun1Times() {
        using CounterHelper helper = new CounterHelper(0);
        using CounterHelper helper2 = new CounterHelper(1);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(1));
        WorkHandle handle2 = Flux.Schedule(helper2.IncrementCount, TimeSpan.FromSeconds(.5));
        await helper2.CountSignal.WaitAsync();
        Flux.RemoveAllWork();
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.IsFalse(helper.IsOver);
        Assert.IsFalse(helper2.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_ThenPauseWork_ShouldRun2Times() {
        using CounterHelper helper = new CounterHelper(2);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(.5));
        await helper.CountSignal.WaitAsync();
        Flux.PauseWork(handle);
        await Task.Delay(TimeSpan.FromSeconds(1));
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_ThenPauseWork_ThenResumeWork_ShouldRun3Times() {
        using CounterHelper helper = new CounterHelper(3);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(.5));
        await helper.WaitUntilCount(2, TimeSpan.FromSeconds(5));
        Flux.PauseWork(handle);
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.AreEqual(2, helper.CurrentCount);
        Flux.StartWork(handle);
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_ThenPauseLoop_ShouldRun2Times() {
        using CounterHelper helper = new CounterHelper(2);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(.5));
        await helper.CountSignal.WaitAsync();
        Flux.StopScheduleLoop();
        await Task.Delay(TimeSpan.FromSeconds(1));
        Flux.RemoveWork(handle);
        Flux.StartScheduleLoop();
        Assert.IsFalse(helper.IsOver);
    }

    [TestMethod]
    public async Task TestBasicSchedule_ThenPauseLoop_ThenResumeLoop_ShouldRun3Times() {
        using CounterHelper helper = new CounterHelper(3);
        WorkHandle handle = Flux.Schedule(helper.IncrementCount, TimeSpan.FromSeconds(.5));
        await helper.WaitUntilCount(2, TimeSpan.FromSeconds(5));
        Flux.StopScheduleLoop();
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.AreEqual(2, helper.CurrentCount);
        Flux.StartScheduleLoop();
        await helper.CountSignal.WaitAsync();
        Flux.RemoveWork(handle);
        Assert.IsFalse(helper.IsOver);
    }

    private sealed class CounterHelper : IDisposable {
        public int CurrentCount => _currentCount;

        private readonly int _trigger;
        private int _currentCount;
        public readonly SemaphoreSlim CountSignal;
        public bool IsOver => _currentCount > _trigger;

        public CounterHelper(int count) {
            _trigger = count;
            CountSignal = new SemaphoreSlim(0, 1);
        }

        public void IncrementCount() {
            IncrementCount(1);
        }

        public void IncrementCount(int offset) {
            _currentCount += offset;
            if (_currentCount == _trigger) {
                CountSignal.Release();
            }
        }

        public void IncrementCount(OffsetHelper helper) {
            IncrementCount(helper.OffSet);
        }

        public async Task IncrementCountAsync(CancellationToken token) {
            if (token.IsCancellationRequested) {
                return;
            }
            IncrementCount();
        }

        internal static readonly TimeSpan POLL_TIME = TimeSpan.FromMilliseconds(10);
        public async Task<bool> WaitUntilCount(int targetCount, TimeSpan timeout) {
            while (_currentCount < targetCount && timeout > TimeSpan.Zero) {
                await Task.Delay(POLL_TIME);
                timeout -= POLL_TIME;
            }

            return timeout > TimeSpan.Zero;
        }

        public void Dispose() {
            CountSignal.Dispose();
        }
    }

    private class OffsetHelper() {
        public int OffSet { get; set; }
    }
}
