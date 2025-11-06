# Lettuce.FluxWork

A minimalistic scheduling library for running sync or async work on configured intervals

[![Build & Test](https://github.com/TheKingOfLettuce/Lettuce.FluxWork/actions/workflows/build.yml/badge.svg)](https://github.com/TheKingOfLettuce/Lettuce.FluxWork/actions/workflows/build.yml)

## Quick Start

`Lettuce.FluxWork` is available on [nuget](https://www.nuget.org/packages/Lettuce.FluxWork)

### Basic Schedule
Scheduling work is extremely simple with `Flux`, here is an example with a simple lambda 
```csharp
Flux.Schedule(() => Console.WriteLine("We are up and live!"), TimeSpan.FromSeconds(1));
```

### Basic Schedule with Data
Have data that needs to be passed as a parameter, pass it to `Flux` so it can be efficiently captured
```csharp
T myDataParameter = ...;
Flux.Schedule<T>((T dataParameter) => Console.WriteLine($"Got my parameter {dataParameter}"), myDataParameter, TimeSpan.FromSeconds(1));
```

### Pausing and Removing Work
Need to pause or remove your schedules work, just pass the returned `WorkHandle` from scheduling
```csharp
WorkHandle work = Flux.Schedule(() => Console.WriteLine("We are up and live!"), TimeSpan.FromSeconds(1));
Flux.PauseWork(work);
Flux.RemoveWork(work);
```

### Async and Cancel Work
The `Flux` scheduler is thread safe when scheduling, pausing, or removing work. There is a small window where the work may already be running when attempting to pause or remove. For that case, pass a `CancellationToken` and allow your work to co-operatively cancel:
```csharp
CancellationTokenSource cancellationToken = new CancellationTokenSource();
WorkHandle work = Flux.Schedule(LongRunningAsyncTask, cancellationToken.Token, TimeSpan.FromSeconds(5));
cancellationToken.Cancel();
Flux.RemoveWork(work);
```