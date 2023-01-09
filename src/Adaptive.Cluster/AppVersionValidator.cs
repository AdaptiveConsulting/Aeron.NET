using Adaptive.Agrona;

namespace Adaptive.Cluster
{
    /// <summary>
    /// Class to be used for determining AppVersion compatibility.
    /// <para>
    /// Default is to use <seealso cref="SemanticVersion"/> major version for checking compatibility.
    /// </para>
    /// </summary>
    public class AppVersionValidator
    {
        /// <summary>
        /// Singleton instance of <seealso cref="AppVersionValidator"/> version which can be used to avoid allocation.
        /// </summary>
        public static readonly AppVersionValidator SEMANTIC_VERSIONING_VALIDATOR = new AppVersionValidator();

        /// <summary>
        /// Check version compatibility between configured context appVersion and appVersion in
        /// new leadership term or snapshot.
        /// </summary>
        /// <param name="contextAppVersion">   configured appVersion value from context. </param>
        /// <param name="appVersionUnderTest"> to check against configured appVersion. </param>
        /// <returns> true for compatible or false for not compatible. </returns>
        public bool IsVersionCompatible(int contextAppVersion, int appVersionUnderTest)
        {
            return SemanticVersion.Major(contextAppVersion) == SemanticVersion.Major(appVersionUnderTest);
        }
    }
}