using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    public static class HandlerHelper
    {
        private class FragmentHandlerWrapper : IFragmentHandler
        {
            private readonly FragmentHandler _delegate;

            public FragmentHandlerWrapper(FragmentHandler @delegate)
            {
                _delegate = @delegate;
            }

            public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
            {
                _delegate(buffer, offset, length, header);
            }
        }
        
        private class ControlledFragmentHandlerWrapper : IControlledFragmentHandler
        {
            private readonly ControlledFragmentHandler _delegate;

            public ControlledFragmentHandlerWrapper(ControlledFragmentHandler @delegate)
            {
                _delegate = @delegate;
            }

            public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
            {
                return _delegate(buffer, offset, length, header);
            }
        }
        
        private class BlockHandlerWrapper : IBlockHandler
        {
            private readonly BlockHandler _delegate;

            public BlockHandlerWrapper(BlockHandler @delegate)
            {
                _delegate = @delegate;
            }

            public void OnBlock(IDirectBuffer buffer, int offset, int length, int sessionId, int termId)
            {
                _delegate(buffer, offset, length, sessionId, termId);
            }
        }

        public static IFragmentHandler ToFragmentHandler(FragmentHandler @delegate)
        {
            return new FragmentHandlerWrapper(@delegate);
        }
        
        public static IControlledFragmentHandler ToControlledFragmentHandler(ControlledFragmentHandler @delegate)
        {
            return new ControlledFragmentHandlerWrapper(@delegate);
        }

        public static IBlockHandler ToBlockHandler(BlockHandler @delegate)
        {
            return new BlockHandlerWrapper(@delegate);
        }
    }
}