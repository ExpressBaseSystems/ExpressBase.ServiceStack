using ServiceStack.Configuration;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class EbLiveSettings
    {
        public string RedisServer { get; set; }

        public int RedisPort { get; set; }

        public string RedisPassword { get; set; }

        public string PrivateKeyXml { get; set; }

        public string PublicKeyXml { get; set; }

        public EbLiveSettings(string s,string pwd,int p,string prikey, string pubkey)
        {
            this.RedisServer = s;
            this.RedisPassword = pwd;
            this.RedisPort = p;
            this.PrivateKeyXml = prikey;
            this.PublicKeyXml = pubkey;
        }

        public EbLiveSettings(IAppSettings appSettings)
        {
            this.RedisServer = appSettings.Get<string>("RedisServer", string.Empty);
            this.RedisPort = appSettings.Get<int>("RedisPort", 6379);
            this.RedisPassword = appSettings.Get<string>("RedisPassword", string.Empty);
            this.PrivateKeyXml = appSettings.Get<string>("PrivateKeyXml", string.Empty);
            this.PublicKeyXml = appSettings.Get<string>("PublicKeyXml", string.Empty);
        }
    }
}
