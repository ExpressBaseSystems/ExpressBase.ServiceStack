using ServiceStack.Auth;
using ServiceStack.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ServiceStack;
using ServiceStack.Web;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Data;
using ServiceStack.Text;
using ExpressBase.Common;
using ExpressBase.Common.Data;

namespace ExpressBase.ServiceStack.Auth0
{
    public class MyFacebookAuthProvider : FacebookAuthProvider
    {
        public MyFacebookAuthProvider(IAppSettings settings) : base(settings) { }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)

        {
            var objret = base.Authenticate(authService, session, request);

            if (!string.IsNullOrEmpty(session.FirstName))
            {
                var _InfraDb = authService.ResolveService<ITenantDbFactory>() as TenantDbFactory;
                using (var con = _InfraDb.DataDB.GetNewConnection())
                {
                    con.Open();
                    var cmd = _InfraDb.DataDB.GetNewCommand(con, "INSERT INTO eb_users (email,firstname,socialid,profileimg) VALUES(@email, @firstname,@socialid,@profileimg) ON CONFLICT(socialid) DO UPDATE SET loginattempts = eb_users.loginattempts + EXCLUDED.loginattempts RETURNING eb_users.loginattempts");
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("email", System.Data.DbType.String, session.ProviderOAuthAccess[0].Email));
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("firstname", System.Data.DbType.String, session.ProviderOAuthAccess[0].DisplayName));
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("socialid", System.Data.DbType.String, session.ProviderOAuthAccess[0].UserName));
                    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("profileimg", System.Data.DbType.String, session.ProviderOAuthAccess[0].Items["profileUrl"]));
                    int logatmp = Convert.ToInt32(cmd.ExecuteScalar());

                    //(session as CustomUserSession).Company = "expressbase";
                    //(session as CustomUserSession).WhichConsole = "tc";
                    return authService.Redirect(SuccessRedirectUrlFilter(this, "http://expressbase.com/Ext/AfterSignInSocial?email=" + session.ProviderOAuthAccess[0].Email + "&socialId=" + session.ProviderOAuthAccess[0].UserName + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret + "&lg=" + logatmp));
                }
            }

            return objret;
        }

    }

    public class MyJwtAuthProvider : JwtAuthProvider
    {
        public MyJwtAuthProvider(IAppSettings settings) : base(settings) { }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            return base.Authenticate(authService, session, request);
        }

        public override string CreateOrMergeAuthSession(IAuthSession session, IAuthTokens tokens)
        {
            return base.CreateOrMergeAuthSession(session, tokens);
        }

        public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            return base.OnAuthenticated(authService, session, tokens, authInfo);
        }

        public override void OnSaveUserAuth(IServiceBase authService, IAuthSession session)
        {
            base.OnSaveUserAuth(authService, session);
        }

        public override void Init(IAppSettings appSettings = null)
        {
            base.Init(appSettings);
        }

        public override bool IsAuthorized(IAuthSession session, IAuthTokens tokens, Authenticate request = null)
        {
            return base.IsAuthorized(session, tokens, request);
        }

        public override string GetKeyId()
        {
            return base.GetKeyId();
        }
       
    }
}
