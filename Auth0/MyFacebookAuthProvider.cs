using ServiceStack.Auth;
using ServiceStack.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ServiceStack;
using ServiceStack.Web;
using ExpressBase.Objects.ServiceStack_Artifacts;

namespace ExpressBase.ServiceStack.Auth0
{
    public class MyFacebookAuthProvider : FacebookAuthProvider
    {
        public MyFacebookAuthProvider(IAppSettings settings) : base(settings) { }

        public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            return base.OnAuthenticated(authService, session, tokens, authInfo);
        }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            var authResponse = base.Authenticate(authService, session, request);
            //AuthenticateResponse authResponse = base.Authenticate(authService, session, request) as AuthenticateResponse;

            var _customUserSession = authService.GetSession() as CustomUserSession;

            //if (!string.IsNullOrEmpty(authResponse.SessionId) && _customUserSession != null)
            {
                var x = new MyAuthenticateResponse
                {
                    UserId = _customUserSession.UserAuthId,
                    UserName = _customUserSession.UserName,
                    User = _customUserSession.User,
                };

                return x;
            }

            return authResponse;
        }
    }
}
