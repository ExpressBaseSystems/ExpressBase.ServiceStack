using ExpressBase.Common;
using ExpressBase.Common.Data;
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
                var _InfraDb = authService.ResolveService<IEbConnectionFactory>() as EbConnectionFactory;
                using (var con = _InfraDb.DataDB.GetNewConnection())
                {
                    con.Open();
                    var cmd = _InfraDb.DataDB.GetNewCommand(con, "INSERT INTO eb_users (email,firstname,socialid,prolink) VALUES(@email, @firstname,@socialid,@prolink) ON CONFLICT(socialid) DO UPDATE SET loginattempts = eb_users.loginattempts + EXCLUDED.loginattempts RETURNING eb_users.loginattempts");
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("email", System.Data.DbType.String, session.ProviderOAuthAccess[0].Email));
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("firstname", System.Data.DbType.String, session.ProviderOAuthAccess[0].DisplayName));
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("socialid", System.Data.DbType.String, session.ProviderOAuthAccess[0].UserId));
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("prolink", System.Data.DbType.String, session.ProviderOAuthAccess[0].Items["profileUrl"]));

                    int logatmp = Convert.ToInt32(cmd.ExecuteScalar());


                    //(session as CustomUserSession).Company = "expressbase";
                    //(session as CustomUserSession).WhichConsole = "tc";
                    return authService.Redirect(SuccessRedirectUrlFilter(this, "http://expressbase.com/Ext/AfterSignInSocial?email=" + session.ProviderOAuthAccess[0].Email + "&socialId=" + session.ProviderOAuthAccess[0].UserId + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret + "&lg=" + logatmp));
                }
            }

            return objret;
        }
    }
}