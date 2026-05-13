/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Adaptive.Agrona;

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

            public ControlledFragmentHandlerAction OnFragment(
                IDirectBuffer buffer,
                int offset,
                int length,
                Header header
            )
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
