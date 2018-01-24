namespace Adaptive.Agrona.Concurrent
{
    public static class IdleStrategyFactory
    {
        public static IIdleStrategy Create(string strategyName, StatusIndicator controllableStatus)
        {
            switch (strategyName)
            {
                default:
                    return new BackoffIdleStrategy(
                        Configuration.IDLE_MAX_SPINS,
                        Configuration.IDLE_MAX_YIELDS,
                        Configuration.IDLE_MIN_PARK_MS,
                        Configuration.IDLE_MAX_PARK_MS);
            }
        }
    }
}