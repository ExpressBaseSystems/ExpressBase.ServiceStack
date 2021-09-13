//using ServiceStack.Auth;
//using ServiceStack.Configuration;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;
//using ServiceStack;
//using ServiceStack.Web;
//using ExpressBase.Objects.ServiceStack_Artifacts;
//using ExpressBase.Data;
//using ServiceStack.Text;
//using ExpressBase.Common;
//using ExpressBase.Common.Data;
//using ExpressBase.Common.Structures;
//using System.Data.Common;
//using ExpressBase.Common.Extensions;
//using Newtonsoft.Json;
//using ExpressBase.ServiceStack.Services;
//using System.Net;

//namespace ExpressBase.ServiceStack.Auth0
//{
//	public class MyFacebookAuthProvider : FacebookAuthProvider
//	{

//		public MyFacebookAuthProvider(IAppSettings settings) : base(settings) { }




//		public object Authenticate111(IServiceBase authService, IAuthSession session, Authenticate request)
//		{
//			var tokens = Init(authService, ref session, request);

//			Console.WriteLine("reached call back url in init" + this.CallbackUrl);
//			if (this.CallbackUrl.Split(":")[0].Equals("http"))
//			{
//				this.CallbackUrl = this.CallbackUrl.Replace("http", "https");
//			}

//			Console.WriteLine("reached call back url after split" + this.CallbackUrl);

//			//Transfering AccessToken/Secret from Mobile/Desktop App to Server
//			if (request?.AccessToken != null)
//			{
//				Console.WriteLine("reached access toke =null? access token =  " + request.AccessToken);

//				if (!AuthHttpGateway.VerifyFacebookAccessToken(AppId, request.AccessToken))
//					return HttpError.Unauthorized("AccessToken is not for App: " + AppId);

//				var isHtml = authService.Request.IsHtml();
//				Console.WriteLine("reached ishtml =  " + isHtml);
//				var failedResult = AuthenticateWithAccessToken(authService, session, tokens, request.AccessToken);
//				if (failedResult != null)
//					return ConvertToClientError(failedResult, isHtml);

//				return isHtml
//					? authService.Redirect(SuccessRedirectUrlFilter(this, session.ReferrerUrl.SetParam("s", "1")))
//					: null; //return default AuthenticateResponse
//			}

//			var httpRequest = authService.Request;
//			var error = httpRequest.QueryString["error_reason"]
//				?? httpRequest.QueryString["error"]
//				?? httpRequest.QueryString["error_code"]
//				?? httpRequest.QueryString["error_description"];

//			var hasError = !error.IsNullOrEmpty();
//			if (hasError)
//			{
//				Console.WriteLine("reached hasError =  ");
//				Log.Error($"Facebook error callback. {httpRequest.QueryString}");
//				return authService.Redirect(FailedRedirectUrlFilter(this, session.ReferrerUrl.SetParam("f", error)));
//			}

//			var code = httpRequest.QueryString[Keywords.Code];
//			var isPreAuthCallback = !code.IsNullOrEmpty();
//			if (!isPreAuthCallback)
//			{
//				Console.WriteLine("reached !isPreAuthCallback =  ");
//				var preAuthUrl = $"{PreAuthUrl}?client_id={AppId}&redirect_uri={this.CallbackUrl.UrlEncode()}&scope={string.Join(",", Permissions)}";

//				this.SaveSession(authService, session, SessionExpiry);
//				return authService.Redirect(PreAuthUrlFilter(this, preAuthUrl));
//			}

//			try
//			{
//				var accessTokenUrl = $"{AccessTokenUrl}?client_id={AppId}&redirect_uri={this.CallbackUrl.UrlEncode()}&client_secret={AppSecret}&code={code}";
//				Console.WriteLine("reached accessTokenUrl =  " + accessTokenUrl);
//				var contents = AccessTokenUrlFilter(this, accessTokenUrl).GetJsonFromUrl();
//				var authInfo = JsonObject.Parse(contents);

//				var accessToken = authInfo["access_token"];
//				Console.WriteLine("reached accessToken =  " + accessToken);
//				return AuthenticateWithAccessToken(authService, session, tokens, accessToken)
//					   ?? authService.Redirect(SuccessRedirectUrlFilter(this, session.ReferrerUrl.SetParam("s", "1"))); //Haz Access!
//			}
//			catch (WebException we)
//			{
//				Console.WriteLine("reached catch Exception: " + we + we.StackTrace);
//				var statusCode = ((HttpWebResponse)we.Response).StatusCode;
//				if (statusCode == HttpStatusCode.BadRequest)
//				{
//					Console.WriteLine("reached catch bad request: " + statusCode);
//					return authService.Redirect(FailedRedirectUrlFilter(this, session.ReferrerUrl.SetParam("f", "AccessTokenFailed")));
//				}
//			}

//			//Shouldn't get here
//			return authService.Redirect(FailedRedirectUrlFilter(this, session.ReferrerUrl.SetParam("f", "Unknown")));
//		}




//		public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)

//		{
//			Console.WriteLine("reached facebook auth started");
//			EbConnectionFactory InfraConnectionFactory = authService.ResolveService<IEbConnectionFactory>() as EbConnectionFactory;
//			Console.WriteLine("reached InfraConnectionFactory : url" + authService.Request.AbsoluteUri);

//			object objret = Authenticate111(authService, session, request);
//			Console.WriteLine("reached base.Authenticate");
//			if (!string.IsNullOrEmpty(session.FirstName))
//			{
//				Console.WriteLine("reached session first name not empty");

//				//   using (var con = InfraConnectionFactory.DataDB.GetNewConnection())
//				{
//					IAuthTokens t = session.ProviderOAuthAccess.FirstOrDefault(e => e.Provider == "facebook");

//					if ((t.Email) != null)
//					{
//						string b = string.Empty;
//						try
//						{
//							Console.WriteLine("reached 1st try of facebook auth");
//							Console.WriteLine($"refferal url  =  {session.ReferrerUrl}");
//							string pasword = null;
//							SocialSignup sco_signup = new SocialSignup();

//							bool unique = false;
//							string urllink = session.ReferrerUrl;
//							string pathsignup = "Platform/OnBoarding";
//							string pathsignin = "TenantSignIn";
//							string sql1 = "SELECT id,fb_id,github_id,twitter_id FROM eb_tenants WHERE email ~* @email and eb_del='F'";
//							DbParameter[] parameters2 = { InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, t.Email) };
//							EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(sql1, parameters2);
//							if (dt.Rows.Count > 0)
//							{
//								unique = false;
//								sco_signup.FbId = Convert.ToString(dt.Rows[0][1]);
//								sco_signup.GithubId = Convert.ToString(dt.Rows[0][2]);
//								sco_signup.TwitterId = Convert.ToString(dt.Rows[0][3]);
//								Console.WriteLine("mail id is not unique");
//								//if (urllink.Contains(pathsignup, StringComparison.OrdinalIgnoreCase))
//								//{
//								//	sco_signup.Forsignup = true;
//								//}
//								//else
//								//if(urllink.Contains(pathsignin, StringComparison.OrdinalIgnoreCase))
//								{
//									sco_signup.Forsignup = false;
//								}
//							}
//							else
//								unique = true;

//							if (unique == true)
//							{
//								string pd = Guid.NewGuid().ToString();
//								pasword = (t.UserId.ToString() + pd + t.Email.ToString()).ToMD5Hash();
//								DbParameter[] parameter1 = {
//								InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, t.Email),
//								InfraConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String,  t.DisplayName),
//								 InfraConnectionFactory.DataDB.GetNewParameter("fbid", EbDbTypes.String,  (t.UserId).ToString()),
//								 InfraConnectionFactory.DataDB.GetNewParameter("password", EbDbTypes.String,pasword),
//								 InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String,'F'),

//								 };

//								EbDataTable dtbl = InfraConnectionFactory.DataDB.DoQuery(@"INSERT INTO eb_tenants 
//								(email,fullname,fb_id,pwd, eb_created_at,eb_del, is_verified, is_email_sent) 
//                                 VALUES 
//                                 (:email,:name,:fbid,:password,NOW(),:fals,:fals,:fals) RETURNING id;", parameter1);

//								Console.WriteLine("inserted details to tenant table");
//								sco_signup.Pauto = pasword;
//							}

//							{
//								sco_signup.AuthProvider = t.Provider;
//								sco_signup.Country = t.Country;
//								sco_signup.Email = t.Email;
//								sco_signup.Social_id = (t.UserId).ToString();
//								sco_signup.Fullname = t.DisplayName;
//								//sco_signup.IsVerified = session.IsAuthenticated,

//								sco_signup.UniqueEmail = unique;

//							};
//							b = JsonConvert.SerializeObject(sco_signup);
//							string sociallink1 = "localhost:";
//							string sociallink2 = "eb-test.xyz";
//							string sociallink3 = "expressbase.com";
//							Console.WriteLine("ReferrerUrl= " + session.ReferrerUrl);
//							if (urllink.Contains(sociallink1, StringComparison.OrdinalIgnoreCase))
//							{
//								Console.WriteLine("reached  redirect to localhost:41500/social_oauth");
//								return authService.Redirect(SuccessRedirectUrlFilter(this, string.Format("http://localhost:41500/social_oauth?scosignup={0}", b)));

//							}

//							if (urllink.Contains(sociallink2, StringComparison.OrdinalIgnoreCase))
//							{
//								Console.WriteLine("reached  redirect to myaccount.eb-test.xyz");
//								return authService.Redirect(SuccessRedirectUrlFilter(this, string.Format("https://myaccount.eb-test.xyz/social_oauth?scosignup={0}", b)));
//							}

//							if (urllink.Contains(sociallink3, StringComparison.OrdinalIgnoreCase))
//							{
//								Console.WriteLine("reached redirect to myaccount.expressbase.com/");
//								return authService.Redirect(SuccessRedirectUrlFilter(this, string.Format("https://myaccount.expressbase.com/social_oauth?scosignup={0}", b)));
//							}



//						}
//						catch (Exception e)
//						{
//							Console.WriteLine("Exception: " + e.Message + e.StackTrace);
//						}


//						//try
//						//{

//						//    con.Open();
//						//    var cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, "INSERT INTO eb_users (email,firstname,socialid,profileimg) VALUES(@email, @firstname,@socialid,@profileimg) ON CONFLICT(socialid) DO UPDATE SET loginattempts = eb_users.loginattempts + EXCLUDED.loginattempts RETURNING eb_users.loginattempts");
//						//    cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, session.ProviderOAuthAccess[0].Email));
//						//    cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("firstname", EbDbTypes.String, session.ProviderOAuthAccess[0].DisplayName));
//						//    cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("socialid", EbDbTypes.String, session.ProviderOAuthAccess[0].UserName));
//						//    cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("profileimg", EbDbTypes.String, session.ProviderOAuthAccess[0].Items["profileUrl"]));
//						//    int logatmp = Convert.ToInt32(cmd.ExecuteScalar());

//						//    //(session as CustomUserSession).Company = CoreConstants.EXPRESSBASE;
//						//    //(session as CustomUserSession).WhichConsole = "tc";
//						//    //return authService.Redirect(SuccessRedirectUrlFilter(this, "http://expressbase.com/Ext/AfterSignInSocial?email=" + session.ProviderOAuthAccess[0].Email + "&socialId=" + session.ProviderOAuthAccess[0].UserName + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret + "&lg=" + logatmp));
//						//    //return authService.Redirect(SuccessRedirectUrlFilter(this, "http://myaccount.localhost:41500/MySolutions" + session.ProviderOAuthAccess[0].Email + "&socialId=" + session.ProviderOAuthAccess[0].UserName + "&provider=" + session.AuthProvider + "&providerToken=" + session.ProviderOAuthAccess[0].AccessTokenSecret + "&lg=" + logatmp));
//						//    return authService.Redirect(SuccessRedirectUrlFilter(this, "http://localhost:41500/Ext/FbLogin"));
//						//}
//						//catch(Exception e)
//						//{
//						//    Console.WriteLine("Exception: " + e.Message + e.StackTrace);
//						//}
//					}

//				}
//			}

//			return objret;


//		}

//		public override string CreateOrMergeAuthSession(IAuthSession session, IAuthTokens tokens)
//		{
//			Console.WriteLine("reached CreateOrMergeAuthSession ");
//			string cm = base.CreateOrMergeAuthSession(session, tokens);
//			Console.WriteLine("reached  " + cm);
//			return cm;
//		}

//		public override bool Equals(object obj)
//		{
//			Console.WriteLine("reached Equals ");
//			return base.Equals(obj);
//		}

//		public override int GetHashCode()
//		{
//			Console.WriteLine("reached GetHashCode ");
//			return base.GetHashCode();
//		}

//		public override bool IsAccountLocked(IAuthRepository authRepo, IUserAuth userAuth, IAuthTokens tokens = null)
//		{
//			Console.WriteLine("reached IsAccountLocked ");
//			return base.IsAccountLocked(authRepo, userAuth, tokens);
//		}

//		public override bool IsAuthorized(IAuthSession session, IAuthTokens tokens, Authenticate request = null)
//		{
//			Console.WriteLine("reached IsAuthorized ");
//			return base.IsAuthorized(session, tokens, request);
//		}

//		public override void LoadUserOAuthProvider(IAuthSession authSession, IAuthTokens tokens)
//		{
//			Console.WriteLine("reached LoadUserOAuthProvider ");
//			base.LoadUserOAuthProvider(authSession, tokens);
//		}

//		public override object Logout(IServiceBase service, Authenticate request)
//		{
//			Console.WriteLine("reached Logout ");
//			return base.Logout(service, request);
//		}

//		public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
//		{
//			Console.WriteLine("reached OnAuthenticated ");
//			return base.OnAuthenticated(authService, session, tokens, authInfo);
//		}

//		public override void OnFailedAuthentication(IAuthSession session, IRequest httpReq, IResponse httpRes)
//		{
//			Console.WriteLine("reached OnFailedAuthentication ");
//			base.OnFailedAuthentication(session, httpReq, httpRes);
//		}

//		public override Task OnFailedAuthenticationAsync(IAuthSession session, IRequest httpReq, IResponse httpRes)
//		{
//			Console.WriteLine("reached OnFailedAuthenticationAsync ");
//			return base.OnFailedAuthenticationAsync(session, httpReq, httpRes);
//		}

//		public override string ToString()
//		{
//			Console.WriteLine("reached ToString ");
//			return base.ToString();
//		}

//		protected override object AuthenticateWithAccessToken(IServiceBase authService, IAuthSession session, IAuthTokens tokens, string accessToken)
//		{
//			Console.WriteLine("reached AuthenticateWithAccessToken ");
//			return base.AuthenticateWithAccessToken(authService, session, tokens, accessToken);
//		}

//		protected override object ConvertToClientError(object failedResult, bool isHtml)
//		{
//			Console.WriteLine("reached ConvertToClientError ");
//			return base.ConvertToClientError(failedResult, isHtml);
//		}

//		protected override bool EmailAlreadyExists(IAuthRepository authRepo, IUserAuth userAuth, IAuthTokens tokens = null)
//		{
//			Console.WriteLine("reached EmailAlreadyExists ");
//			return base.EmailAlreadyExists(authRepo, userAuth, tokens);
//		}

//		protected override string GetAuthRedirectUrl(IServiceBase authService, IAuthSession session)
//		{
//			Console.WriteLine("reached GetAuthRedirectUrl ");
//			string ss = base.GetAuthRedirectUrl(authService, session);
//			Console.WriteLine("reached  " + ss);

//			return ss;
//		}

//		protected override IAuthRepository GetAuthRepository(IRequest req)
//		{
//			Console.WriteLine("reached GetAuthRepository ");
//			return base.GetAuthRepository(req);
//		}

//		protected override string GetReferrerUrl(IServiceBase authService, IAuthSession session, Authenticate request = null)

//		{
//			Console.WriteLine("reached GetReferrerUrl  ");
//			string ref1 = base.GetReferrerUrl(authService, session, request);
//			Console.WriteLine("reached   " + ref1);
//			return ref1;
//		}

//		protected override void LoadUserAuthInfo(AuthUserSession userSession, IAuthTokens tokens, Dictionary<string, string> authInfo)
//		{
//			Console.WriteLine("reached LoadUserAuthInfo ");
//			base.LoadUserAuthInfo(userSession, tokens, authInfo);
//		}

//		protected override bool UserNameAlreadyExists(IAuthRepository authRepo, IUserAuth userAuth, IAuthTokens tokens = null)
//		{
//			Console.WriteLine("reached UserNameAlreadyExists ");
//			return base.UserNameAlreadyExists(authRepo, userAuth, tokens);
//		}

//		protected override IHttpResult ValidateAccount(IServiceBase authService, IAuthRepository authRepo, IAuthSession session, IAuthTokens tokens)
//		{
//			Console.WriteLine("reached ValidateAccount ");
//			return base.ValidateAccount(authService, authRepo, session, tokens);
//		}
//	}
//}