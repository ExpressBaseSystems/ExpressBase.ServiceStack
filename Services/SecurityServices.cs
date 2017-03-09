using ExpressBase.Common;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using ServiceStack.Web;
using System.IO;
using ExpressBase.Data;
using System.Data.Common;

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

        public string ClientId { get; set; }

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
            throw new NotImplementedException();
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
      

        public MyJwtAuthProvider(IAppSettings settings) : base(settings) { }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            CustomUserSession mysession = session as CustomUserSession;

            AuthenticateResponse response = null;
            if (request.Meta["Login"] == "Client")
            {
                string path = Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName;
                var infraconf = EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(Path.Combine(path, "EbInfra.conn")));
                var df = new DatabaseFactory(infraconf);   
                _authUser = InfraUser.GetDetails(df, request.UserName, request.Password);
            }
            else
            {
                EbBaseService bservice = new EbBaseService();
                bservice.ClientID = request.Meta["ClientId"];

                _authUser = User.GetDetails(bservice.DatabaseFactory, request.UserName, request.Password);
            }
            if (_authUser != null)
            {
                mysession.IsAuthenticated = true;
                mysession.UserAuthId = _authUser.Id.ToString();
                mysession.UserName = _authUser.Uname;
                mysession.FirstName = _authUser.Fname;
                if(request.Meta.ContainsKey("ClientId"))
                {
                    mysession.ClientId = request.Meta["ClientId"];
                }
                else
                {
                    mysession.ClientId = _authUser.Cid.ToString();
                }
                

                response = new AuthenticateResponse
                {
                    UserId = _authUser.Id.ToString(),
                    UserName = _authUser.Uname,
                    ReferrerUrl = string.Empty,
                    BearerToken = base.CreateJwtBearerToken(mysession),
                };
            }
            else
            {
                response = new AuthenticateResponse
                {
                    ResponseStatus = new ResponseStatus("EbUnauthorized", "Eb Unauthorized Access")
                };
            }

            return response;
        }
    }
}
