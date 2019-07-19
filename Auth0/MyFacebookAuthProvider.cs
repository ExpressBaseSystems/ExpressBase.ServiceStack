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
					IAuthTokens t = session.ProviderOAuthAccess.FirstOrDefault(e => e.Provider == "facebook");

					if ((t.Email) != null)
                    {
                        string b = string.Empty;
                        try
                        {
							Console.WriteLine("reached try of facebook auth");
							Console.WriteLine($"refferal url  =  {session.ReferrerUrl}");
							string pasword = null;
                            SocialSignup sco_signup = new SocialSignup();

                            bool unique = false;
							string urllink = session.ReferrerUrl;
							string pathsignup = "Platform/OnBoarding";
							string pathsignin = "TenantSignIn";
							string sql1 = "SELECT id, pwd,fb_id,github_id,twitter_id FROM eb_tenants WHERE email ~* @email and eb_del='F'";
                            DbParameter[] parameters2 = { InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String,t.Email) };
                            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(sql1, parameters2);
                            if (dt.Rows.Count > 0)
                            {
                                unique = false;
                                sco_signup.FbId = Convert.ToString(dt.Rows[0][1]);
                                sco_signup.GithubId = Convert.ToString(dt.Rows[0][2]);
                                sco_signup.TwitterId = Convert.ToString(dt.Rows[0][3]);
								Console.WriteLine("mail id is not unique");
								//if (urllink.Contains(pathsignup, StringComparison.OrdinalIgnoreCase))
								//{
								//	sco_signup.Forsignup = true;
								//}
								//else
								//if(urllink.Contains(pathsignin, StringComparison.OrdinalIgnoreCase))
								{
									sco_signup.Forsignup = false;
								}
							}
                            else
                                unique = true;
                        
                            if (unique == true)
                            {
                                string pd = Guid.NewGuid().ToString();
                               pasword = (t.UserId.ToString() + pd + t.Email.ToString()).ToMD5Hash();
                                DbParameter[] parameter1 = {
                                InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, t.Email),
                                InfraConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String,  t.DisplayName),
                                 InfraConnectionFactory.DataDB.GetNewParameter("fbid", EbDbTypes.String,  (t.UserId).ToString()),
                                 InfraConnectionFactory.DataDB.GetNewParameter("password", EbDbTypes.String,pasword),
                                 InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String,'F'),

                                 };

                                EbDataTable dtbl = InfraConnectionFactory.DataDB.DoQuery(@"INSERT INTO eb_tenants 
								(email,fullname,fb_id,pwd, eb_created_at,eb_del, is_verified, is_email_sent) 
                                 VALUES 
                                 (:email,:name,:fbid,:password,NOW(),:fals,:fals,:fals) RETURNING id;", parameter1);

								Console.WriteLine("inserted details to tenant table");

							}
                           
                            {
                                sco_signup.AuthProvider = t.Provider;
                                sco_signup.Country = t.Country;
                                sco_signup.Email = t.Email;
                                sco_signup.Social_id = (t.UserId).ToString();
                                sco_signup.Fullname = t.DisplayName;
                                //sco_signup.IsVerified = session.IsAuthenticated,
                                sco_signup.Pauto = pasword;
                                sco_signup.UniqueEmail = unique;
                               
                            };
                            b = JsonConvert.SerializeObject(sco_signup);
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