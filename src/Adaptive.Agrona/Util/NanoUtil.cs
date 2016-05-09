namespace Adaptive.Agrona.Util
{
    public static class NanoUtil
    {
        public static long FromSeconds(long seconds)
        {
            return seconds*1000*1000*1000;
        }

        public static long FromMilliseconds(long milliseconds)
        {
            return milliseconds * 1000 * 1000;
        }
    }
}