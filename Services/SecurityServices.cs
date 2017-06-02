using ExpressBase.Common;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using System;
using System.Collections.Generic;
using ServiceStack.Web;
using System.IO;
using ExpressBase.Data;
using ServiceStack.Logging;
using System.Runtime.Serialization;

namespace ExpressBase.ServiceStack
{
    public class CustomUserSession : IAuthSession
    {
        public DateTime CreatedAt { get; set; }

        public string DisplayName { get; set; }

        public string Email { get; set; }

        public string FirstName { get; set; }

        public bool FromToken { get; set; }

        public string Id { get; set; }

        public bool IsAuthenticated { get; set; }

        public DateTime LastModified { get; set; }

        public string LastName { get; set; }

        public List<string> Permissions { get; set; }

        public string ProfileUrl { get; set; }

        public List<IAuthTokens> ProviderOAuthAccess { get; set; }

        public string ReferrerUrl { get; set; }

        public List<string> Roles { get; set; }

        public string Sequence { get; set; }

        public string UserAuthId { get; set; }

        public string UserAuthName { get; set; }

        public string UserName { get; set; }

        public string CId { get; set; }

        public int Uid { get; set; }

        public string AuthProvider
        {
             get; set; 
            //get
            //{
            //    throw new NotImplementedException();
            //}

            //set
            //{
            //    throw new NotImplementedException();
            //}
        }

        public CustomUserSession()
        {
            this.ProviderOAuthAccess = new List<IAuthTokens>();
        }

        public bool HasPermission(string permission, IAuthRepository authRepo)
        {
            throw new NotImplementedException();
        }

        public bool HasRole(string role, IAuthRepository authRepo)
        {
            throw new NotImplementedException();
        }

        public bool IsAuthorized(string provider)
        {
            return true;
        }
        
        public void OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            Dictionary<string, string> dict = authInfo;

           

            // throw new NotImplementedException();
        }

        public void OnCreated(IRequest httpReq) { }

        public void OnLogout(IServiceBase authService)
        {
            throw new NotImplementedException();
        }

        public void OnRegistered(IRequest httpReq, IAuthSession session, IServiceBase service)
        {
            throw new NotImplementedException();
        }
    }

    public class MyJwtAuthProvider : JwtAuthProvider
    {
        User _authUser = null;
        IAppSettings AppSettings = null;

        public MyJwtAuthProvider(IAppSettings settings) : base(settings) { AppSettings = settings; }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            ILog log = LogManager.GetLogger(GetType());
            CustomUserSession mysession = session as CustomUserSession;
            EbBaseService bservice = new EbBaseService();

            MyAuthenticateResponse response = null;
           
            
            if (string.IsNullOrEmpty(request.Meta["cid"]))
            {
                string path = Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName;
                var infraconf = EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(Path.Combine(path, "EbInfra.conn")));
                var df = new DatabaseFactory(infraconf);   
                _authUser = InfraUser.GetDetails(df, request.UserName, request.Password);
                log.Info("#Eb reached 1");
            }
            else
            {
                bservice.ClientID = request.Meta["cid"];
                _authUser = User.GetDetails(bservice.DatabaseFactory, request.UserName, request.Password);
                log.Info("#Eb reached 2");

            }
            if (_authUser != null)
            {
                log.Info("#Eb reached 3");
                var redisClient = (authService as AuthenticateService).Redis;
                mysession.UserName = _authUser.Uname;
                mysession.FirstName = _authUser.Fname;
                mysession.Uid = _authUser.Id;      
               if(!string.IsNullOrEmpty(request.Meta["cid"]))
                {
                    log.Info("#Eb reached 4");
                    mysession.CId = request.Meta["cid"];
                   
                }
                else
                {
                    log.Info("#Eb reached 5");
                    mysession.CId= string.Empty;                   
                }
              
                response = new MyAuthenticateResponse
                {                  
                    UserId = _authUser.Id.ToString(),
                    UserName = _authUser.Uname,
                    ReferrerUrl = string.Empty,
                    BearerToken = base.CreateJwtBearerToken(mysession),
                    User = _authUser ,
                };
            }
            else
            {
                response = new MyAuthenticateResponse
                {
                    ResponseStatus = new ResponseStatus("EbUnauthorized", "Eb Unauthorized Access")
                };
            }

            return response;
        }
    }
    [DataContract]
    public class MyAuthenticateResponse : AuthenticateResponse
    {
        [DataMember(Order =1)]
        public User User { get; set; }
    }
}
