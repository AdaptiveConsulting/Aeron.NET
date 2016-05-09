using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Samples.Common
{
    public class DelegateFragmentHandler : IFragmentHandler
    {
        private readonly Action<IDirectBuffer, int, int, Header> _action;

        public DelegateFragmentHandler(Action<IDirectBuffer, int, int, Header> action)
        {
            _action = action;
        }

        public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            _action(buffer, offset, length, header);
        }
    }
}