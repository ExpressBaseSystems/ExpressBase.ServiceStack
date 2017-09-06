using ExpressBase.Common;
using ExpressBase.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using System;

namespace ExpressBase.ServiceStack
{
    internal class MyGithubAuthProvider : GithubAuthProvider
    {
        public MyGithubAuthProvider(IAppSettings settings) : base(settings) { }
        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            var objret = base.Authenticate(authService, session, request);

            if (!string.IsNullOrEmpty(session.ProviderOAuthAccess[0].Email))
            {
                var _InfraDb = authService.TryResolve<InfraDbFactory>().InfraDB;
                using (var con = _InfraDb.GetNewConnection())
                {
                    con.Open();
                    var cmd = _InfraDb.GetNewCommand(con, "INSERT INTO eb_tenants (cname,firstname,socialid,prolink) VALUES(@cname, @firstname,@socialid,@prolink) ON CONFLICT(socialid) DO UPDATE SET loginattempts = eb_tenants.loginattempts + EXCLUDED.loginattempts RETURNING id,eb_tenants.loginattempts");
                    cmd.Parameters.Add(_InfraDb.GetNewParameter("cname", System.Data.DbType.String, session.ProviderOAuthAccess[0].Email));
                    cmd.Parameters.Add(_InfraDb.GetNewParameter("firstname", System.Data.DbType.String, session.ProviderOAuthAccess[0].DisplayName));
                    cmd.Parameters.Add(_InfraDb.GetNewParameter("socialid", System.Data.DbType.String, session.ProviderOAuthAccess[0].UserId));
                    cmd.Parameters.Add(_InfraDb.GetNewParameter("prolink", System.Data.DbType.String, session.ProviderOAuthAccess[0].Items["profileUrl"]));

                    int logatmp = Convert.ToInt32(cmd.ExecuteScalar());


                    //(session as CustomUserSession).Company = "expressbase";
                    //(session as CustomUserSession).WhichConsole = "tc";
                    return authService.Redirect(SuccessRedirectUrlFilter(this, "http://expressbase.org/Ext/AfterSignInSocial?email=" + session.ProviderOAuthAccess[0].Email + "&socialId=" + session.ProviderOAuthAccess[0].UserId + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret + "&lg=" + logatmp));
                }
            }

            return objret;
        }
    }
}