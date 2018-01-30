using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using ServiceStack.Logging;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Auth0
{
    public class MyCredentialsAuthProvider : CredentialsAuthProvider
    {
        private RedisClient Redis { get; set; }

        public MyCredentialsAuthProvider(IAppSettings settings) : base(settings) { }

        public override bool TryAuthenticate(IServiceBase authService, string UserName, string password)
        {
            ILog log = LogManager.GetLogger(GetType());

            log.Info("In TryAuthenticate method1");
            var TenantDbFactory = authService.ResolveService<IEbConnectionFactory>() as EbConnectionFactory;

            log.Info("In TryAuthenticate method2");

            User _authUser = null;

            var request = authService.Request.Dto as Authenticate;

            var cid = request.Meta.ContainsKey("cid") ? request.Meta["cid"] : string.Empty;
            var socialId = request.Meta.ContainsKey("socialId") ? request.Meta["socialId"] : string.Empty;

            if (request.Meta.ContainsKey("signup_tok"))
            {
                cid = "expressbase";
                _authUser = User.GetInfraVerifiedUser(TenantDbFactory.DataDB, UserName, request.Meta["signup_tok"]);
            }
            else
            {
                //if (cid == "expressbase")
                //{
                //    log.Info("for tenant login");
                //    _authUser = (string.IsNullOrEmpty(socialId)) ? User.GetInfraUser(TenantDbFactory.DataDB, UserName, password) : User.GetInfraUserViaSocial(TenantDbFactory.DataDB, UserName, socialId);
                //    log.Info("#Eb reached 1");
                //}
                //else
                //{
                    //log.Info("for user login");
                //    _authUser = (string.IsNullOrEmpty(socialId)) ? User.GetDetails(TenantDbFactory.DataDB, UserName, password) : User.GetInfraUserViaSocial(TenantDbFactory.DataDB, socialId);
                _authUser = User.GetDetails(TenantDbFactory.DataDB, UserName, password, socialId);
                log.Info("#Eb reached 2");
                //}
            }

            if (_authUser != null)
            {

                CustomUserSession session = authService.GetSession(false) as CustomUserSession;
                var redisClient = authService.TryResolve<IRedisClientsManager>().GetClient();
                session.CId = cid;
                _authUser.CId = cid;
                session.Uid = _authUser.UserId;
                session.Email = _authUser.Email;
                session.IsAuthenticated = true;
                session.User = _authUser;
                session.WhichConsole = request.Meta["wc"];
                _authUser.wc = request.Meta["wc"];
                session.UserAuthId = string.Format("{0}-{1}-{2}", cid, _authUser.Email, request.Meta["wc"]);

                var authRepo = HostContext.AppHost.GetAuthRepository(authService.Request);
                var existingUser = (authRepo as EbRedisAuthRepository).GetUserAuth(string.Format("{0}-{1}-{2}", cid, _authUser.Email, request.Meta["wc"]));
                //if (existingUser != null)
                (authRepo as EbRedisAuthRepository).UpdateUserAuth(existingUser, _authUser);
                //redisClient.Set<IUserAuth>(string.Format("{0}-{1}", cid, _authUser.Email), _authUser);
            }

            return (_authUser != null);
        }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
            {
            ILog log = LogManager.GetLogger(GetType());

            log.Info("In Authenticate method1");
            AuthenticateResponse authResponse = base.Authenticate(authService, session, request) as AuthenticateResponse;
            log.Info("In Authenticate method2");
            var _customUserSession = authService.GetSession() as CustomUserSession;
            _customUserSession.WhichConsole = request.Meta.ContainsKey("wc") ? request.Meta["wc"] : string.Empty;

            if (!string.IsNullOrEmpty(authResponse.SessionId) && _customUserSession != null)
            {
                log.Info("In Authenticate method3");
                return new MyAuthenticateResponse
                {
                    UserId = _customUserSession.UserAuthId,
                    UserName = _customUserSession.UserName,
                    User = _customUserSession.User
            };
            }
            

            return authResponse;
        }

        public override object Logout(IServiceBase service, Authenticate request)
        {
            return base.Logout(service, request);
        }
    }
}
