using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using ServiceStack.Logging;
using ServiceStack.Redis;

namespace ExpressBase.ServiceStack.Auth0
{
	public class MyCredentialsAuthProvider : CredentialsAuthProvider
    {
        private RedisClient Redis { get; set; }

        public MyCredentialsAuthProvider(IAppSettings settings) : base(settings) { }

		private ILog Logger
		{
			get { return LogManager.GetLogger(GetType()); }
		}

        public override bool TryAuthenticate(IServiceBase authService, string UserName, string password)
        {
			Logger.Info("In TryAuthenticate method1");

			var request = authService.Request.Dto as Authenticate;
			var cid = request.Meta.ContainsKey("cid") ? request.Meta["cid"] : string.Empty;
			var socialId = request.Meta.ContainsKey("socialId") ? request.Meta["socialId"] : string.Empty;
			var emailId = request.Meta.ContainsKey("emailId") ? request.Meta["emailId"] : string.Empty;//for anonymous
			var phone = request.Meta.ContainsKey("phone") ? request.Meta["phone"] : string.Empty;//for anonymous
			var whichContext = request.Meta["wc"].ToLower().Trim();

			var EbConnectionFactory = authService.TryResolve<IEbConnectionFactory>() as EbConnectionFactory;

			Logger.Info("In TryAuthenticate method2");
			//string[] app_types = { "Mobile", "Web", "Bot" };
			//if (request.Meta["context"] == "tc" || request.Meta["context"] == "dc")
			//	app_types 

			//if (request.Meta.ContainsKey("signup_tok"))
			//         {
			//             cid = CoreConstants.EXPRESSBASE;
			//             _authUser = User.GetInfraVerifiedUser(EbConnectionFactory.DataDB, UserName, request.Meta["signup_tok"]);
			//         }

			User _authUser = null;
			if (request.Meta.ContainsKey("anonymous") && whichContext.Equals("bc"))
			{
				_authUser = User.GetDetailsAnonymous(EbConnectionFactory.DataDB, socialId, emailId, phone, whichContext);
				Logger.Info("TryAuthenticate -> anonymous");
			}
			else if (!string.IsNullOrEmpty(socialId))
            {
				_authUser = User.GetDetailsSocial(EbConnectionFactory.DataDB, socialId, whichContext);
				Logger.Info("TryAuthenticate -> socialId");
            }
			else if (request.Meta.ContainsKey("sso") && whichContext.Equals("dc"))
			{
				_authUser = User.GetDetailsSSO(EbConnectionFactory.DataDB, UserName, whichContext);
				Logger.Info("TryAuthenticate -> sso");
			}
			else 
			{
				_authUser = User.GetDetailsNormal(EbConnectionFactory.DataDB, UserName, password, whichContext);
				Logger.Info("TryAuthenticate -> Normal");
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
                session.WhichConsole = whichContext;
                _authUser.wc = whichContext;
                session.UserAuthId = string.Format("{0}-{1}-{2}", cid, _authUser.Email, whichContext);

                var authRepo = HostContext.AppHost.GetAuthRepository(authService.Request);
                var existingUser = (authRepo as EbRedisAuthRepository).GetUserAuth(session.UserAuthId);
                (authRepo as EbRedisAuthRepository).UpdateUserAuth(existingUser, _authUser);
            }

            return (_authUser != null);
        }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
			Logger.Info("Authenticate -> Start");
			AuthenticateResponse authResponse = base.Authenticate(authService, session, request) as AuthenticateResponse;
            var _customUserSession = authService.GetSession() as CustomUserSession;
            _customUserSession.WhichConsole = request.Meta.ContainsKey("wc") ? request.Meta["wc"] : string.Empty;

            if (!string.IsNullOrEmpty(authResponse.SessionId) && _customUserSession != null)
            {
				Logger.Info("In Authenticate method3");
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
