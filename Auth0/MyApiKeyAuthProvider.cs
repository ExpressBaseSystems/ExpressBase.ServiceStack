//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Net;
//using ExpressBase.Objects.ServiceStack_Artifacts;
//using ServiceStack.Auth;
//using ServiceStack.Configuration;
//using ServiceStack.Host;
//using ServiceStack.Logging;
//using ServiceStack.Web;

//namespace ServiceStack.Auth
//{
//    /// <summary>
//    /// The Interface Auth Repositories need to implement to support API Keys
//    /// </summary>
//    public interface IMyManageApiKeys
//    {
//        void InitApiKeySchema();

//        bool ApiKeyExists(string apiKey);

//        MyApiKey GetApiKey(string apiKey);

//        List<MyApiKey> GetUserApiKeys(string userId);

//        void StoreAll(IEnumerable<MyApiKey> apiKeys);
//    }

//    /// <summary>
//    /// The POCO Table used to persist API Keys
//    /// </summary>
//    public class MyApiKey : IMeta
//    {
//        public string Id { get; set; }
//        public string UserAuthId { get; set; }

//        public string Environment { get; set; }
//        public string KeyType { get; set; }

//        public DateTime CreatedDate { get; set; }
//        public DateTime? ExpiryDate { get; set; }
//        public DateTime? CancelledDate { get; set; }
//        public string Notes { get; set; }

//        //Custom Reference Data
//        public int? RefId { get; set; }
//        public string RefIdStr { get; set; }
//        public Dictionary<string, string> Meta { get; set; }
//    }

//    public delegate string MyCreateApiKeyDelegate(string environment, string keyType, int keySizeBytes);

//    /// <summary>
//    /// Enable access to protected Services using API Keys
//    /// </summary>
//    public class MyApiKeyAuthProvider : ApiKeyAuthProvider
//    {
//        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
//        {
//            ILog log = LogManager.GetLogger(GetType());

//            log.Info("In Authenticate method1");
//            AuthenticateResponse authResponse = base.Authenticate(authService, session, request) as AuthenticateResponse;

//            log.Info("In Authenticate method2");
//            var _customUserSession = authService.GetSession() as CustomUserSession;

//            log.Info("In Authenticate method3");
//            return new MyAuthenticateResponse
//            {
//                User = _customUserSession.User
//            };
//        }
//    }

//}