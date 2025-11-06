namespace Lettuce.FluxWork.Exceptions;

internal class FluxWorkException : Exception {
    public FluxWorkException(string message) : base(message) {}
    public FluxWorkException(string message, Exception exception) : base(message, exception) {}
}