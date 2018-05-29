using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceStack;
using ExpressBase.Common.ServiceStack.Auth;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using ServiceStack.Logging;
using ServiceStack.Redis;
using System;

namespace ExpressBase.ServiceStack.Auth0
{
    public class MyCredentialsAuthProvider : CredentialsAuthProvider
    {
        private RedisClient Redis { get; set; }

        public MyCredentialsAuthProvider(IAppSettings settings) : base(settings) { }

        private ILog Logger
        {
            get { return LogManager.GetLogger(GetType()); }
        }

        public override bool TryAuthenticate(IServiceBase authService, string UserName, string password)
        {
            try
            {
                Logger.Info("In TryAuthenticate method1");

                var request = authService.Request.Dto as Authenticate;
                var cid = request.Meta.ContainsKey(TokenConstants.CID) ? request.Meta[TokenConstants.CID] : string.Empty;
                var socialId = request.Meta.ContainsKey(TokenConstants.SOCIALID) ? request.Meta[TokenConstants.SOCIALID] : string.Empty;
                var whichContext = request.Meta[TokenConstants.WC].ToLower().Trim();

                var EbConnectionFactory = authService.TryResolve<IEbConnectionFactory>() as EbConnectionFactory;

                Logger.Info("In TryAuthenticate method2");
                //string[] app_types = { "Mobile", "Web", "Bot" };
                //if (request.Meta["context"] == "tc" || request.Meta["context"] == "dc")
                //	app_types 

                //if (request.Meta.ContainsKey("signup_tok"))
                //         {
                //             cid = CoreConstants.EXPRESSBASE;
                //             _authUser = User.GetInfraVerifiedUser(EbConnectionFactory.DataDB, UserName, request.Meta["signup_tok"]);
                //         }

                User _authUser = null;
                if (request.Meta.ContainsKey("anonymous") && whichContext.Equals("bc"))
                {
                    var emailId = request.Meta.ContainsKey("emailId") ? request.Meta["emailId"] : string.Empty;//for anonymous
                    var phone = request.Meta.ContainsKey("phone") ? request.Meta["phone"] : string.Empty;//for anonymous
                    var appid = request.Meta.ContainsKey("appid") ? Convert.ToInt32(request.Meta["appid"]) : 0;//for anonymous
                    var user_ip = request.Meta.ContainsKey("user_ip") ? request.Meta["user_ip"] : string.Empty;//for anonymous
                    var user_name = request.Meta.ContainsKey("user_name") ? request.Meta["user_name"] : string.Empty;//for anonymous
                    var user_browser = request.Meta.ContainsKey("user_browser") ? request.Meta["user_browser"] : string.Empty;//for anonymous
                    var city = request.Meta.ContainsKey("city") ? request.Meta["city"] : string.Empty;//for anonymous
                    var region = request.Meta.ContainsKey("region") ? request.Meta["region"] : string.Empty;//for anonymous
                    var country = request.Meta.ContainsKey("country") ? request.Meta["country"] : string.Empty;//for anonymous
                    var latitude = request.Meta.ContainsKey("latitude") ? request.Meta["latitude"] : string.Empty;//for anonymous
                    var longitude = request.Meta.ContainsKey("longitude") ? request.Meta["longitude"] : string.Empty;//for anonymous
                    var timezone = request.Meta.ContainsKey("timezone") ? request.Meta["timezone"] : string.Empty;//for anonymous
                    var iplocationjson = request.Meta.ContainsKey("iplocationjson") ? request.Meta["iplocationjson"] : string.Empty;//for anonymous

                    _authUser = User.GetDetailsAnonymous(EbConnectionFactory.DataDB, socialId, emailId, phone, appid, whichContext, user_ip, user_name, user_browser, city, region, country, latitude, longitude, timezone, iplocationjson);

                    Logger.Info("TryAuthenticate -> anonymous");
                    Logger.Info("TryAuthenticate -> Details: " + EbConnectionFactory.DataDB.ToJson()+ socialId + emailId + phone + appid + whichContext + user_ip + user_name + user_browser + city + region + country + latitude + longitude + timezone);
                    Logger.Info("User: "+ _authUser.ToJson());

                }
                else if (!string.IsNullOrEmpty(socialId))
                {

                    _authUser = User.GetDetailsSocial(EbConnectionFactory.DataDB, socialId, whichContext);
                    Logger.Info("TryAuthenticate -> socialId");

                }
                else if (request.Meta.ContainsKey("sso") && (whichContext.Equals("dc") || whichContext.Equals("uc")))
                {

                    _authUser = User.GetDetailsSSO(EbConnectionFactory.DataDB, UserName, whichContext);
                    Logger.Info("TryAuthenticate -> sso");

                }
                else
                {
                    _authUser = User.GetDetailsNormal(EbConnectionFactory.DataDB, UserName, password, whichContext);
                    Logger.Info("TryAuthenticate -> Normal");

                }
                if (_authUser != null)
                {
                    Logger.Info("Inside Auth User Not Null");
                    if (_authUser.Email != null)
                    {
                        CustomUserSession session = authService.GetSession(false) as CustomUserSession;
                        var redisClient = authService.TryResolve<IRedisClientsManager>().GetClient();
                        session.CId = cid;
                        _authUser.CId = cid;
                        session.Uid = _authUser.UserId;
                        session.Email = _authUser.Email;
                        session.IsAuthenticated = true;
                        session.User = _authUser;
                        session.WhichConsole = whichContext;
                        session.DBVendor = EbConnectionFactory.DataDB.Vendor;
                        _authUser.wc = whichContext;
                        _authUser.AuthId = string.Format(TokenConstants.SUB_FORMAT, cid, _authUser.Email, whichContext);
                        session.UserAuthId = _authUser.AuthId;

                        var authRepo = HostContext.AppHost.GetAuthRepository(authService.Request);
                        var existingUser = (authRepo as MyRedisAuthRepository).GetUserAuth(session.UserAuthId);
                        (authRepo as MyRedisAuthRepository).UpdateUserAuth(existingUser, _authUser);
                    }
                }
                return (_authUser != null);

            } catch(Exception ee)
            {
                Logger.Info("Exception: "+ ee.ToJson());
                return false;
            }
           
            
        }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            Logger.Info("Authenticate -> Start");
            AuthenticateResponse authResponse;
            try
            {
                authResponse = base.Authenticate(authService, session, request) as AuthenticateResponse;
                if (authResponse.UserId != null)
                {
                    var _customUserSession = authService.GetSession() as CustomUserSession;
                    _customUserSession.WhichConsole = request.Meta.ContainsKey(RoutingConstants.WC) ? request.Meta[RoutingConstants.WC] : string.Empty;

                    if (!string.IsNullOrEmpty(authResponse.SessionId) && _customUserSession != null)
                    {
                        Logger.Info("In Authenticate method3");
                        return new MyAuthenticateResponse
                        {
                            UserId = _customUserSession.UserAuthId,
                            UserName = _customUserSession.UserName,
                            User = _customUserSession.User,
                            SessionId = _customUserSession.Id,

                        };
                    }
                    return authResponse;
                }
                else
                {
                    throw new Exception("er_server");
                }
            }
            catch (Exception e)
            {
                if (e.Message == "Invalid UserName or Password")
                    throw new Exception("Invalid Username or Password");
                else
                    throw new Exception("Internal Server Error");
            }
        }

        public override object Logout(IServiceBase service, Authenticate request)
        {
            return base.Logout(service, request);
        }
    }
}
