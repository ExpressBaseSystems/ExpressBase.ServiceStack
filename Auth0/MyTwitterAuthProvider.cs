using System.Collections.Generic;
using ExpressBase.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using ServiceStack.Web;
using ExpressBase.Common;
using ExpressBase.Common.Data;

namespace ExpressBase.ServiceStack
{
    public class MyTwitterAuthProvider : TwitterAuthProvider
    {

        public MyTwitterAuthProvider(IAppSettings settings) : base(settings) { }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            var objret = base.Authenticate(authService, session, request);

            if (!string.IsNullOrEmpty(session.ProviderOAuthAccess[0].DisplayName))
            {
                var _InfraDb = authService.ResolveService<ITenantDbFactory>() as TenantDbFactory;
                using (var con = _InfraDb.DataDB.GetNewConnection())
                {
                    con.Open();
                    var cmd = _InfraDb.DataDB.GetNewCommand(con, "INSERT INTO eb_users (email,firstname,socialid,prolink) VALUES(@email, @firstname,@socialid,@prolink) ON CONFLICT(socialid) DO UPDATE SET loginattempts = eb_users.loginattempts + EXCLUDED.loginattempts RETURNING eb_users.loginattempts");
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("email", System.Data.DbType.String, session.ProviderOAuthAccess[0].Email));
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("firstname", System.Data.DbType.String, session.ProviderOAuthAccess[0].DisplayName));
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("socialid", System.Data.DbType.String, session.ProviderOAuthAccess[0].UserName));
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("prolink", System.Data.DbType.String, session.ProviderOAuthAccess[0].Items["profileUrl"]));
                    cmd.ExecuteNonQuery();
                }

                (session as CustomUserSession).Company = "expressbase";
                (session as CustomUserSession).WhichConsole = "tc";
                return authService.Redirect(SuccessRedirectUrlFilter(this, "http://localhost:5000/Ext/AfterSignInSocial?email=" + session.Email + "&socialId=" + session.UserName + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret));
            }

            return objret;
        }

        public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            return base.OnAuthenticated(authService, session, tokens, authInfo);
        }

    }
}