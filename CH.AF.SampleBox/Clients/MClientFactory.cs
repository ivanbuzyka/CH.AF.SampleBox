using Stylelabs.M.Sdk.WebClient;
using Stylelabs.M.Sdk.WebClient.Authentication;
using System;

namespace CH.AF.SampleBox.Clients
{
    internal static class MClientFactory
    {
        internal static IWebMClient CreateClient()
        {
            Uri endpoint = new Uri(Settings.ContentHub.Endpoint);

            OAuthPasswordGrant oauth = new OAuthPasswordGrant
            {
                ClientId = Settings.ContentHub.ClientId,
                ClientSecret = Settings.ContentHub.ClientSecret,
                UserName = Settings.ContentHub.Username,
                Password = Settings.ContentHub.Password
            };

            IWebMClient client = Stylelabs.M.Sdk.WebClient.MClientFactory.CreateMClient(endpoint, oauth);

            return client;
        }
    }
}
