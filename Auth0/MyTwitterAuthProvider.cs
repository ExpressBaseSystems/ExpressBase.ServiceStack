using System.Collections.Generic;
using ExpressBase.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using ServiceStack.Web;
using ExpressBase.Common;

namespace ExpressBase.ServiceStack
{
    public class MyTwitterAuthProvider : TwitterAuthProvider
    {

        public MyTwitterAuthProvider(IAppSettings settings) : base(settings) { }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            var objret = base.Authenticate(authService, session, request);

            if (!string.IsNullOrEmpty(session.FirstName))
            {
                var _InfraDb = authService.TryResolve<TenantDbFactory>().DataDB;
                using (var con = _InfraDb.GetNewConnection())
                {
                    con.Open();
                    var cmd = _InfraDb.GetNewCommand(con, "INSERT INTO eb_tenants (cname,firstname,socialid,prolink) VALUES(@cname, @firstname,@socialid,@prolink) ON CONFLICT(socialid) DO UPDATE SET cname=@cname RETURNING id");
                    cmd.Parameters.Add(_InfraDb.GetNewParameter("cname", System.Data.DbType.String, session.ProviderOAuthAccess[0].Email));
                    cmd.Parameters.Add(_InfraDb.GetNewParameter("firstname", System.Data.DbType.String, session.ProviderOAuthAccess[0].DisplayName));
                    cmd.Parameters.Add(_InfraDb.GetNewParameter("socialid", System.Data.DbType.String, session.ProviderOAuthAccess[0].UserName));
                    cmd.Parameters.Add(_InfraDb.GetNewParameter("prolink", System.Data.DbType.String, session.ProviderOAuthAccess[0].Items["profileUrl"]));
                    cmd.ExecuteNonQuery();
                }

                (session as CustomUserSession).Company = "expressbase";
                (session as CustomUserSession).WhichConsole = "tc";
                return authService.Redirect(SuccessRedirectUrlFilter(this, "http://localhost:53431/Ext/AfterSignInSocial?email=" + session.Email + "&socialId=" + session.UserName + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret));
            }

            return objret;
        }

        public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            return base.OnAuthenticated(authService, session, tokens, authInfo);
        }

    }
}