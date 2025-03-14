using System;

namespace Adaptive.Agrona
{
    public static class Objects
    {
        public static T RequireNonNull<T>(T obj, string name)
        {
            if (obj == null)
            {
                throw new NullReferenceException(name);
            }

            return obj;
        }
        
        public static T RequireNonNull<T>(T obj)
        {
            if (obj == null)
            {
                throw new NullReferenceException();
            }

            return obj;
        }
    }
}
