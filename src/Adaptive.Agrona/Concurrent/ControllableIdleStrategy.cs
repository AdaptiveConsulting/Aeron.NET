using System.Threading;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona.Concurrent
{
    public class ControllableIdleStrategy : IIdleStrategy
    {
        public const int NOT_CONTROLLED = 0;
        public const int NOOP = 1;
        public const int BUSY_SPIN = 2;
        public const int YIELD = 3;
        public const int PARK = 4;

        private const long PARK_PERIOD_NANOSECONDS = 1000;

        private readonly StatusIndicatorReader statusIndicatorReader;

        public ControllableIdleStrategy(StatusIndicatorReader statusIndicatorReader)
        {
            this.statusIndicatorReader = statusIndicatorReader;
        }

        /// <summary>
        /// Idle based on current status indication value
        /// </summary>
        /// <param name="workCount"> performed in last duty cycle. </param>
        /// <seealso cref="IIdleStrategy.Idle(int)"></seealso>
        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                return;
            }

            Idle();
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public void Idle()
        {
            int status = (int)statusIndicatorReader.GetVolatile();

            switch (status)
            {
                case NOOP:
                    break;

                case BUSY_SPIN:
                    Thread.SpinWait(0);
                    break;

                case YIELD:
                    Thread.Yield();
                    break;

                default:
                    LockSupport.ParkNanos(PARK_PERIOD_NANOSECONDS);
                    break;
            }
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public void Reset()
        {
        }

        public override string ToString()
        {
            return "ControllableIdleStrategy{" +
                   "statusIndicatorReader=" + statusIndicatorReader +
                   '}';
        }
    }
}