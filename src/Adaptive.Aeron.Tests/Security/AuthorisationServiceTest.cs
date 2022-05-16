using System;
using Adaptive.Aeron.Security;
using Adaptive.Agrona;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.Security
{
    public class authorisation_service_test
    {
        [Test]
        public void ShouldAllowAnyCommandIfAllowAllIsUsed()
        {
            byte[] encodedCredentials = {0x1, 0x2, 0x3};
            var errorHandler = A.Fake<ErrorHandler>();
            const int protocolId = 77;
            int actionId = new Random().Next();

            Assert.True(AllowAllAuthorisationService.INSTANCE.IsAuthorised(protocolId, actionId, null, encodedCredentials));
            A.CallTo(errorHandler).MustNotHaveHappened();
        }

        [Test]
        public void ShouldForbidAllCommandsIfDenyAllIsUsed()
        {
            byte[] encodedCredentials = {0x4, 0x5, 0x6};
            var errorHandler = A.Fake<ErrorHandler>();
            const int protocolId = 77;
            int actionId = new Random().Next();

            Assert.False(DenyAllAuthorisationService.INSTANCE.IsAuthorised(protocolId, actionId, null, encodedCredentials));
            A.CallTo(errorHandler).MustNotHaveHappened();
        }
    }

}