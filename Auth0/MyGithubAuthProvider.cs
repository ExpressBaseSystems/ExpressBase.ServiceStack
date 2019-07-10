using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Structures;
using ExpressBase.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using ServiceStack.Web;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Auth0
{
    internal class MyGithubAuthProvider : GithubAuthProvider
    {
        public MyGithubAuthProvider(IAppSettings settings) : base(settings) { }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            var objret = base.Authenticate(authService, session, request);

            if (!string.IsNullOrEmpty(session.ProviderOAuthAccess[0].Email))
            {
                EbConnectionFactory InfraConnectionFactory = authService.ResolveService<IEbConnectionFactory>() as EbConnectionFactory;


                string b = string.Empty;
                try
                {
                    Console.WriteLine("reached try of github auth");
                    string pasword = null;
                    SocialSignup sco_signup = new SocialSignup();
                    bool unique = false;
                    string sql1 = "SELECT id,fb_id,github_id,twitter_id FROM eb_tenants WHERE email ~* @email and eb_del=false";
                    DbParameter[] parameters2 = { InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, session.ProviderOAuthAccess[0].Email) };
                    EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(sql1, parameters2);
                    if (dt.Rows.Count > 0)
                    {
                        unique = false;
                        sco_signup.FbId = Convert.ToString(dt.Rows[0][1]);
                        sco_signup.GithubId = Convert.ToString(dt.Rows[0][2]);
                        sco_signup.TwitterId = Convert.ToString(dt.Rows[0][3]);
                        Console.WriteLine("mail id is not unique");
                    }
                    else
                        unique = true;
                    
                    if (unique == true)
                    {
                        string pd = Guid.NewGuid().ToString();
                         pasword = (session.ProviderOAuthAccess[0].UserId.ToString() + pd + session.ProviderOAuthAccess[0].Email.ToString()).ToMD5Hash();
                        DbParameter[] parameter1 = {
                            InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String,  session.ProviderOAuthAccess[0].Email),
                            InfraConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String,  session.ProviderOAuthAccess[0].UserName),
                             InfraConnectionFactory.DataDB.GetNewParameter("githubid", EbDbTypes.String,  (session.ProviderOAuthAccess[0].UserId).ToString()),
                             InfraConnectionFactory.DataDB.GetNewParameter("password", EbDbTypes.String,pasword)

                             };

                        EbDataTable dtbl = InfraConnectionFactory.DataDB.DoQuery(@"INSERT INTO eb_tenants (email, fullname, github_id, pwd, eb_created_at) 
                             VALUES 
                             (:email,:name,:githubid,:password,NOW()) RETURNING id;", parameter1);

                        Console.WriteLine("inserted details to tenant table");
                    }
                   
                    
                        sco_signup.AuthProvider = session.ProviderOAuthAccess[0].Provider;
                        sco_signup.Country = session.ProviderOAuthAccess[0].Country;
                        sco_signup.Email = session.ProviderOAuthAccess[0].Email;
                        sco_signup.Social_id = (session.ProviderOAuthAccess[0].UserId).ToString();
                        sco_signup.Fullname = session.ProviderOAuthAccess[0].UserName;
                        // sco_signup.IsVerified = session.IsAuthenticated;
                        sco_signup.Pauto = pasword;
                        sco_signup.UniqueEmail = unique;
                       
                    
                    b = JsonConvert.SerializeObject(sco_signup);
                    string urllink = session.ReferrerUrl;
                    string sociallink1 = "localhost:41500";
                    string sociallink2 = "eb-test.xyz";
                    string sociallink3 = "expressbase.com";
                    Console.WriteLine("ReferrerUrl= " + session.ReferrerUrl);
                    if (urllink.Contains(sociallink1, StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine("reached  redirect to localhost:41500/social_oauth");
                        return authService.Redirect(SuccessRedirectUrlFilter(this, string.Format("http://localhost:41500/social_oauth?scosignup={0}", b)));

                    }

                    if (urllink.Contains(sociallink2, StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine("reached  redirect to myaccount.eb-test.xyz");
                        return authService.Redirect(SuccessRedirectUrlFilter(this, string.Format("https://myaccount.eb-test.xyz/social_oauth?scosignup={0}", b)));
                    }

                    if (urllink.Contains(sociallink3, StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine("reached redirect to myaccount.expressbase.com/");
                        return authService.Redirect(SuccessRedirectUrlFilter(this, string.Format("https://myaccount.expressbase.com/social_oauth?scosignup={0}", b)));
                    }


                }
                catch (Exception e)
                {

                    Console.WriteLine("Exception: " + e.Message + e.StackTrace);
                }






                //using (var con = _InfraDb.DataDB.GetNewConnection())
                //{
                //    con.Open();
                //    var cmd = _InfraDb.DataDB.GetNewCommand(con, "INSERT INTO eb_users (email,firstname,socialid,prolink) VALUES(@email, @firstname,@socialid,@prolink) ON CONFLICT(socialid) DO UPDATE SET loginattempts = eb_users.loginattempts + EXCLUDED.loginattempts RETURNING eb_users.loginattempts");
                //    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("email", EbDbTypes.String, session.ProviderOAuthAccess[0].Email));
                //    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("firstname", EbDbTypes.String, session.ProviderOAuthAccess[0].DisplayName));
                //    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("socialid", EbDbTypes.String, session.ProviderOAuthAccess[0].UserId));
                //    cmd.Parameters.Add(_InfraDb.DataDB.GetNewParameter("prolink", EbDbTypes.String, session.ProviderOAuthAccess[0].Items["profileUrl"]));

                //    int logatmp = Convert.ToInt32(cmd.ExecuteScalar());


                //    //(session as CustomUserSession).Company = CoreConstants.EXPRESSBASE;
                //    //(session as CustomUserSession).WhichConsole = "tc";
                //    return authService.Redirect(SuccessRedirectUrlFilter(this, "http://expressbase.com/Ext/AfterSignInSocial?email=" + session.ProviderOAuthAccess[0].Email + "&socialId=" + session.ProviderOAuthAccess[0].UserId + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret + "&lg=" + logatmp));
                //}
            }

            return objret;
        }

        public override string CreateOrMergeAuthSession(IAuthSession session, IAuthTokens tokens)
        {
            Console.WriteLine("reached CreateOrMergeAuthSession ");
            return base.CreateOrMergeAuthSession(session, tokens);
        }

        public override bool IsAccountLocked(IAuthRepository authRepo, IUserAuth userAuth, IAuthTokens tokens = null)
        {
            Console.WriteLine("reached IsAccountLocked ");
            return base.IsAccountLocked(authRepo, userAuth, tokens);
        }

        public override bool IsAuthorized(IAuthSession session, IAuthTokens tokens, Authenticate request = null)
        {
            Console.WriteLine("reached IsAuthorized ");
            return base.IsAuthorized(session, tokens, request);
        }

        public override void LoadUserOAuthProvider(IAuthSession authSession, IAuthTokens tokens)
        {
            Console.WriteLine("reached LoadUserOAuthProvider ");
            base.LoadUserOAuthProvider(authSession, tokens);
        }

        public override object Logout(IServiceBase service, Authenticate request)
        {
            Console.WriteLine("reached Logout ");
            return base.Logout(service, request);
        }

        public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            Console.WriteLine("reached OnAuthenticated ");
            return base.OnAuthenticated(authService, session, tokens, authInfo);
        }

        public override void OnFailedAuthentication(IAuthSession session, IRequest httpReq, IResponse httpRes)
        {
            Console.WriteLine("reached OnFailedAuthentication ");
            base.OnFailedAuthentication(session, httpReq, httpRes);
        }

        public override Task OnFailedAuthenticationAsync(IAuthSession session, IRequest httpReq, IResponse httpRes)
        {
            Console.WriteLine("reached OnFailedAuthenticationAsync ");
            return base.OnFailedAuthenticationAsync(session, httpReq, httpRes);
        }

        protected override object AuthenticateWithAccessToken(IServiceBase authService, IAuthSession session, IAuthTokens tokens, string accessToken)
        {
            Console.WriteLine("reached AuthenticateWithAccessToken ");
            return base.AuthenticateWithAccessToken(authService, session, tokens, accessToken);
        }

        protected override object ConvertToClientError(object failedResult, bool isHtml)
        {
            Console.WriteLine("reached ConvertToClientError ");
            return base.ConvertToClientError(failedResult, isHtml);
        }

        protected override bool EmailAlreadyExists(IAuthRepository authRepo, IUserAuth userAuth, IAuthTokens tokens = null)
        {
            Console.WriteLine("reached EmailAlreadyExists ");
            return base.EmailAlreadyExists(authRepo, userAuth, tokens);
        }

        protected override string GetAuthRedirectUrl(IServiceBase authService, IAuthSession session)
        {
            Console.WriteLine("reached GetAuthRedirectUrl ");
            return base.GetAuthRedirectUrl(authService, session);
        }

        protected override IAuthRepository GetAuthRepository(IRequest req)
        {
            Console.WriteLine("reached GetAuthRepository ");
            return base.GetAuthRepository(req);
        }

        protected override string GetReferrerUrl(IServiceBase authService, IAuthSession session, Authenticate request = null)
        {
            
            string url= base.GetReferrerUrl(authService, session, request);
            Console.WriteLine("reached GetReferrerUrl:" + url);
            return url;
        }

        protected override void LoadUserAuthInfo(AuthUserSession userSession, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            Console.WriteLine("reached LoadUserAuthInfo ");
            base.LoadUserAuthInfo(userSession, tokens, authInfo);
        }

        protected override bool UserNameAlreadyExists(IAuthRepository authRepo, IUserAuth userAuth, IAuthTokens tokens = null)
        {
            Console.WriteLine("reached UserNameAlreadyExists ");
            return base.UserNameAlreadyExists(authRepo, userAuth, tokens);
        }

        protected override IHttpResult ValidateAccount(IServiceBase authService, IAuthRepository authRepo, IAuthSession session, IAuthTokens tokens)
        {
            Console.WriteLine("reached ValidateAccount ");
            return base.ValidateAccount(authService, authRepo, session, tokens);
        }
    }
}