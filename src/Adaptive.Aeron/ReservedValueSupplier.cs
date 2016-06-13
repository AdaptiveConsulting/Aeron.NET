using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    public delegate long ReservedValueSupplier(IDirectBuffer termBuffer, int termOffset, int frameLength);
}