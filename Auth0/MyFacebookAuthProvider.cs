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
using ExpressBase.Common.Structures;
using System.Data.Common;
using ExpressBase.Common.Extensions;
using Newtonsoft.Json;
using ExpressBase.ServiceStack.Services;

namespace ExpressBase.ServiceStack.Auth0
{
    public class MyFacebookAuthProvider : FacebookAuthProvider
    {

        public MyFacebookAuthProvider(IAppSettings settings) : base(settings) { }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)

        {
            EbConnectionFactory InfraConnectionFactory = authService.ResolveService<IEbConnectionFactory>() as EbConnectionFactory;

            object objret = base.Authenticate(authService, session, request);

            if (!string.IsNullOrEmpty(session.FirstName))
            {


                //   using (var con = InfraConnectionFactory.DataDB.GetNewConnection())
                {

                    if ((session.ProviderOAuthAccess[0].Email) != null)
                    {
                        string b = string.Empty;
                        try
                        {
                            string pasword = null;
                            SocialSignup sco_signup = new SocialSignup();

                            bool unique = false;
                            string sql1 = "SELECT id, pwd,fb_id,github_id,twitter_id FROM eb_tenants WHERE email ~* @email";
                            DbParameter[] parameters2 = { InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, session.ProviderOAuthAccess[0].Email) };
                            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(sql1, parameters2);
                            if (dt.Rows.Count > 0)
                            {
                                unique = false;
                                sco_signup.FbId = Convert.ToString(dt.Rows[0][1]);
                                sco_signup.GithubId = Convert.ToString(dt.Rows[0][2]);
                                sco_signup.TwitterId = Convert.ToString(dt.Rows[0][3]);
                            }
                            else
                                unique = true;
                        
                            if (unique == true)
                            {
                                string pd = Guid.NewGuid().ToString();
                               pasword = (session.ProviderOAuthAccess[0].UserId.ToString() + pd + session.ProviderOAuthAccess[0].Email.ToString()).ToMD5Hash();
                                DbParameter[] parameter1 = {
                                InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String,  session.ProviderOAuthAccess[0].Email),
                                InfraConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String,  session.ProviderOAuthAccess[0].DisplayName),
                                 InfraConnectionFactory.DataDB.GetNewParameter("fbid", EbDbTypes.String,  (session.ProviderOAuthAccess[0].UserId).ToString()),
                                 InfraConnectionFactory.DataDB.GetNewParameter("password", EbDbTypes.String,pasword  ),

                                 };

                                EbDataTable dtbl = InfraConnectionFactory.DataDB.DoQuery(@"INSERT INTO eb_tenants (email,fullname,fb_id,pwd) 
                                 VALUES 
                                 (:email,:name,:fbid,:password) RETURNING id;", parameter1);

                                
                            }
                           
                            {
                                sco_signup.AuthProvider = session.ProviderOAuthAccess[0].Provider;
                                sco_signup.Country = session.ProviderOAuthAccess[0].Country;
                                sco_signup.Email = session.ProviderOAuthAccess[0].Email;
                                sco_signup.Social_id = (session.ProviderOAuthAccess[0].UserId).ToString();
                                sco_signup.Fullname = session.ProviderOAuthAccess[0].DisplayName;
                                //sco_signup.IsVerified = session.IsAuthenticated,
                                sco_signup.Pauto = pasword;
                                sco_signup.UniqueEmail = unique;
                               
                            };
                            b = JsonConvert.SerializeObject(sco_signup);
                            return authService.Redirect(SuccessRedirectUrlFilter(this, string.Format("http://localhost:41500/social_oauth?scosignup={0}", b)));

                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception: " + e.Message + e.StackTrace);
                        }


                        //try
                        //{

                        //    con.Open();
                        //    var cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, "INSERT INTO eb_users (email,firstname,socialid,profileimg) VALUES(@email, @firstname,@socialid,@profileimg) ON CONFLICT(socialid) DO UPDATE SET loginattempts = eb_users.loginattempts + EXCLUDED.loginattempts RETURNING eb_users.loginattempts");
                        //    cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, session.ProviderOAuthAccess[0].Email));
                        //    cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("firstname", EbDbTypes.String, session.ProviderOAuthAccess[0].DisplayName));
                        //    cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("socialid", EbDbTypes.String, session.ProviderOAuthAccess[0].UserName));
                        //    cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("profileimg", EbDbTypes.String, session.ProviderOAuthAccess[0].Items["profileUrl"]));
                        //    int logatmp = Convert.ToInt32(cmd.ExecuteScalar());

                        //    //(session as CustomUserSession).Company = CoreConstants.EXPRESSBASE;
                        //    //(session as CustomUserSession).WhichConsole = "tc";
                        //    //return authService.Redirect(SuccessRedirectUrlFilter(this, "http://expressbase.com/Ext/AfterSignInSocial?email=" + session.ProviderOAuthAccess[0].Email + "&socialId=" + session.ProviderOAuthAccess[0].UserName + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret + "&lg=" + logatmp));
                        //    //return authService.Redirect(SuccessRedirectUrlFilter(this, "http://myaccount.localhost:41500/MySolutions" + session.ProviderOAuthAccess[0].Email + "&socialId=" + session.ProviderOAuthAccess[0].UserName + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret + "&lg=" + logatmp));
                        //    return authService.Redirect(SuccessRedirectUrlFilter(this, "http://localhost:41500/Ext/FbLogin"));
                        //}
                        //catch(Exception e)
                        //{
                        //    Console.WriteLine("Exception: " + e.Message + e.StackTrace);
                        //}
                    }

                }
            }

            return objret;


        }

        public override string CreateOrMergeAuthSession(IAuthSession session, IAuthTokens tokens)
        {
            return base.CreateOrMergeAuthSession(session, tokens);
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override bool IsAccountLocked(IAuthRepository authRepo, IUserAuth userAuth, IAuthTokens tokens = null)
        {
            return base.IsAccountLocked(authRepo, userAuth, tokens);
        }

        public override bool IsAuthorized(IAuthSession session, IAuthTokens tokens, Authenticate request = null)
        {
            return base.IsAuthorized(session, tokens, request);
        }

        public override void LoadUserOAuthProvider(IAuthSession authSession, IAuthTokens tokens)
        {
            base.LoadUserOAuthProvider(authSession, tokens);
        }

        public override object Logout(IServiceBase service, Authenticate request)
        {
            return base.Logout(service, request);
        }

        public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            return base.OnAuthenticated(authService, session, tokens, authInfo);
        }

        public override void OnFailedAuthentication(IAuthSession session, IRequest httpReq, IResponse httpRes)
        {
            base.OnFailedAuthentication(session, httpReq, httpRes);
        }

        public override Task OnFailedAuthenticationAsync(IAuthSession session, IRequest httpReq, IResponse httpRes)
        {
            return base.OnFailedAuthenticationAsync(session, httpReq, httpRes);
        }

        public override string ToString()
        {
            return base.ToString();
        }

        protected override object AuthenticateWithAccessToken(IServiceBase authService, IAuthSession session, IAuthTokens tokens, string accessToken)
        {
            return base.AuthenticateWithAccessToken(authService, session, tokens, accessToken);
        }

        protected override object ConvertToClientError(object failedResult, bool isHtml)
        {
            return base.ConvertToClientError(failedResult, isHtml);
        }

        protected override bool EmailAlreadyExists(IAuthRepository authRepo, IUserAuth userAuth, IAuthTokens tokens = null)
        {
            return base.EmailAlreadyExists(authRepo, userAuth, tokens);
        }

        protected override string GetAuthRedirectUrl(IServiceBase authService, IAuthSession session)
        {
            return base.GetAuthRedirectUrl(authService, session);
        }

        protected override IAuthRepository GetAuthRepository(IRequest req)
        {
            return base.GetAuthRepository(req);
        }

        protected override string GetReferrerUrl(IServiceBase authService, IAuthSession session, Authenticate request = null)



        {
            return base.GetReferrerUrl(authService, session, request);
        }

        protected override void LoadUserAuthInfo(AuthUserSession userSession, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            base.LoadUserAuthInfo(userSession, tokens, authInfo);
        }

        protected override bool UserNameAlreadyExists(IAuthRepository authRepo, IUserAuth userAuth, IAuthTokens tokens = null)
        {
            return base.UserNameAlreadyExists(authRepo, userAuth, tokens);
        }

        protected override IHttpResult ValidateAccount(IServiceBase authService, IAuthRepository authRepo, IAuthSession session, IAuthTokens tokens)
        {
            return base.ValidateAccount(authService, authRepo, session, tokens);
        }
    }
}