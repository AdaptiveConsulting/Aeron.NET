namespace Adaptive.Agrona.Concurrent
{
    public class Configuration
    {
        /// <summary>
        /// Spin on no activity before backing off to yielding.
        /// </summary>
        public const long IDLE_MAX_SPINS = 10;

        /// <summary>
        /// Yield the thread so others can run before backing off to parking.
        /// </summary>
        public const long IDLE_MAX_YIELDS = 40;

        /// <summary>
        /// Park for the minimum period of time which is typically 50-55 microseconds on 64-bit non-virtualised Linux.
        /// You will typically get 50-55 microseconds plus the number of nanoseconds requested if a core is available.
        /// On Windows expect to wait for at least 16ms or 1ms if the high-res timers are enabled.
        /// </summary>
        public const long IDLE_MIN_PARK_MS = 1;

        /// <summary>
        /// Maximum back-off park time which doubles on each interval stepping up from the min park idle.
        /// </summary>
        public static readonly long IDLE_MAX_PARK_MS = 16;
    }
}