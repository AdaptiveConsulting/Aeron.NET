namespace Adaptive.Agrona.Concurrent
{
    public static class IdleStrategyFactory
    {
        public static IIdleStrategy Create(string strategyName, StatusIndicator controllableStatus)
        {
            switch (strategyName)
            {
                case "ControllableIdleStrategy":
                    var idleStrategy = new ControllableIdleStrategy(controllableStatus);
                    controllableStatus.SetOrdered(ControllableIdleStrategy.PARK);
                    return idleStrategy;

                case "YieldingIdleStrategy":
                    return new YieldingIdleStrategy();
                    
                case "SleepingIdleStrategy":
                    return new SleepingIdleStrategy(1);
                
                case "BusySpinIdleStrategy":
                    return new BusySpinIdleStrategy();
                
                case "NoOpIdleStrategy":
                    return new NoOpIdleStrategy();
                
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