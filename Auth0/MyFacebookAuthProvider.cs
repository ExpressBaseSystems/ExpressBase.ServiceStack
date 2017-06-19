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

        //public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        //{
        //    //SuccessRedirectUrlFilter = (authProvider, url) => "http://localhost:53431/Tenant/TenantDashboard";
        //    return base.OnAuthenticated(authService, session, tokens, authInfo);
        //}

        public override bool IsAuthorized(IAuthSession session, IAuthTokens tokens, Authenticate request = null)
        {
            request.UseTokenCookie = true;
            return base.IsAuthorized(session, tokens, request);
        }

        //public override void LoadUserOAuthProvider(IAuthSession authSession, IAuthTokens tokens)
        //{
        //    base.LoadUserOAuthProvider(authSession, tokens);
        //}

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            var objret = base.Authenticate(authService, session, request);

            if (!string.IsNullOrEmpty(session.Email))
            {
                (session as CustomUserSession).Company = "expressbase";
                (session as CustomUserSession).WhichConsole = "tc";

                var jwtprovider = authService.TryResolve<JwtAuthProvider>();
                string token = jwtprovider.CreateJwtBearerToken(session);
                string rToken = jwtprovider.CreateJwtRefreshToken(session.UserAuthId);
                
                //using (var service = authService.ResolveService<ConvertSessionToTokenService>()) //In Process
                //{
                //    (session as CustomUserSession).Company = "expressbase";
                //    var obj = service.Any(new ConvertSessionToToken()) as HttpResult;
                //    token = obj.Cookies[0].Value;
                //}

                return authService.Redirect(SuccessRedirectUrlFilter(this, "http://localhost:53431/Tenant/AfterSignInSocial?Token=" + token + "&rToken=" + rToken));
            }

            return objret;
        }

        public override object Logout(IServiceBase service, Authenticate request)
        {
            return base.Logout(service, request);
        }
    }
}
