using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
    }
}
