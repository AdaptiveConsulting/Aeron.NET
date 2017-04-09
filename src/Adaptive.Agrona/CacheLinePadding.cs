namespace Adaptive.Agrona
{
    public struct CacheLinePadding
    {
        private long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;

        // To prevent compiler removing unused padding fields
        public override string ToString()
        {
            return $"{nameof(p1)}: {p1}, {nameof(p2)}: {p2}, {nameof(p3)}: {p3}, {nameof(p4)}: {p4}, {nameof(p5)}: {p5}, {nameof(p6)}: {p6}, {nameof(p7)}: {p7}, {nameof(p8)}: {p8}, {nameof(p9)}: {p9}, {nameof(p10)}: {p10}, {nameof(p11)}: {p11}, {nameof(p12)}: {p12}, {nameof(p13)}: {p13}, {nameof(p14)}: {p14}, {nameof(p15)}: {p15}";
        }
    }
}
