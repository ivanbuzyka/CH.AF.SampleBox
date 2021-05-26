using System;
using System.Collections.Generic;
using System.Text;

namespace CH.AF.SampleBox
{
    public static class Settings
    {
        public static class ContentHub
        {
            public static string Endpoint => Environment.GetEnvironmentVariable("CH-Endpoint");
            public static string ClientId => Environment.GetEnvironmentVariable("CH-ClientId");
            public static string ClientSecret => Environment.GetEnvironmentVariable("CH-ClientSecret");
            public static string Username => Environment.GetEnvironmentVariable("CH-Username");
            public static string Password => Environment.GetEnvironmentVariable("CH-Password");
        }
        public static class StorageAccont
        {
            public static string ConnectionString => Environment.GetEnvironmentVariable("SA-ConnectionString");
            public static string ContainerName => Environment.GetEnvironmentVariable("SA-ContainerName");
        }
    }
}
