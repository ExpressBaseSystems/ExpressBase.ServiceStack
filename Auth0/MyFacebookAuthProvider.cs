using ServiceStack.Auth;
using ServiceStack.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ServiceStack;
using ServiceStack.Web;

namespace ExpressBase.ServiceStack.Auth0
{
    public class MyFacebookAuthProvider : FacebookAuthProvider
    {
        public MyFacebookAuthProvider(IAppSettings settings) : base(settings) { }

        public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            return base.OnAuthenticated(authService, session, tokens, authInfo);
        }
    }
}
