//using ServiceStack.Configuration;

//namespace ExpressBase.ServiceStack
//{
//    public class EbLiveSettings
//    {
//        public string RedisServer { get; set; }

//        public int RedisPort { get; set; }

//        public string RedisPassword { get; set; }

//        public string PrivateKeyXml { get; set; }

//        public string PublicKeyXml { get; set; }

//        public string RabbitHost { get; set; }

//        public int RabbitPort { get; set; }

//        public string RabbitUser { get; set; }

//        public string RabbitPassword { get; set; }

//        public string RabbitVHost { get; set; }

//        public EbLiveSettings(string s, string pwd, int p, string prikey, string pubkey, string rabusr, string rabpass, string rabhost, int rabpor, string rabvhos)
//        {
//            this.RedisServer = s;
//            this.RedisPassword = pwd;
//            this.RedisPort = p;
//            this.PrivateKeyXml = prikey;
//            this.PublicKeyXml = pubkey;
//            this.RabbitUser = rabusr;
//            this.RabbitPassword = rabpass;
//            this.RabbitHost = rabhost;
//            this.RabbitPort = rabpor;
//            this.RabbitVHost = rabvhos;
//        }

//        public EbLiveSettings(IAppSettings appSettings)
//        {
//            this.RedisServer = appSettings.Get<string>("RedisServer", string.Empty);
//            this.RedisPort = appSettings.Get<int>("RedisPort", 6379);
//            this.RedisPassword = appSettings.Get<string>("RedisPassword", string.Empty);
//            this.PrivateKeyXml = appSettings.Get<string>("PrivateKeyXml", string.Empty);
//            this.PublicKeyXml = appSettings.Get<string>("PublicKeyXml", string.Empty);
//            this.RabbitUser = appSettings.Get<string>("RabbitUser", string.Empty);
//            this.RabbitPassword = appSettings.Get<string>("RabbitPassword", string.Empty);
//            this.RabbitHost = appSettings.Get<string>("RabbitHost", string.Empty);
//            this.RabbitPort = appSettings.Get<int>("RabbitPort", 5672);
//            this.RabbitVHost = appSettings.Get<string>("RabbitVHost", string.Empty);
//        }
//    }
//}
