namespace Lettuce.FluxWork.Exceptions;

internal class FluxConcurrencyException : Exception {
    public FluxConcurrencyException(string message) : base(message) {}
    public FluxConcurrencyException(string message, Exception exception) : base(message, exception) {}
}