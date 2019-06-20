using ExpressBase.Common.Constants;
using ExpressBase.Common.ServiceStack.Auth;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using ServiceStack.Redis;
using ServiceStack.Web;
using System;
using System.Collections.Generic;
using System.Text;

namespace ExpressBase.ServiceStack.Auth0
{
    public class EbApiAuthProvider : ApiKeyAuthProvider
    {
        public EbApiAuthProvider(IAppSettings appSettings) : base(appSettings)
        {
        }

        public override object Authenticate(IServiceBase authService, IAuthSession session, Authenticate request)
        {
            var authRepo = HostContext.AppHost.GetAuthRepository(authService.Request);
            using (authRepo as IDisposable)
            {
                var apiKey = GetApiKey(authService.Request, request.Password);
                ValidateApiKey(apiKey);

                var userAuth = authRepo.GetUserAuth(apiKey.UserAuthId);
                if (userAuth == null)
                    throw HttpError.Unauthorized("User for ApiKey does not exist");

                if (IsAccountLocked(authRepo, userAuth))
                    throw new AuthenticationException(ErrorMessages.UserAccountLocked.Localize(authService.Request));

                PopulateSession(authRepo as IUserAuthRepository, userAuth, (session as CustomUserSession), apiKey.UserAuthId);

                if (session.UserAuthName == null)
                    session.UserAuthName = userAuth.UserName ?? userAuth.Email;

                var response = OnAuthenticated(authService, session, null, null);
                if (response != null)
                    return response;

                authService.Request.Items[Keywords.ApiKey] = apiKey;

                return new AuthenticateResponse
                {
                    UserId = session.UserAuthId,
                    UserName = session.UserName,
                    SessionId = session.Id,
                    DisplayName = session.DisplayName
                        ?? session.UserName
                        ?? $"{session.FirstName} {session.LastName}".Trim(),
                    ReferrerUrl = request.Continue,
                };
            }
        }

        public override string CreateApiKey(string environment, string keyType, int sizeBytes)
        {
            return base.CreateApiKey(environment, keyType, sizeBytes);
        }

        public override bool IsAuthorized(IAuthSession session, IAuthTokens tokens, Authenticate request = null)
        {
            return base.IsAuthorized(session, tokens, request);
        }

        public override IHttpResult OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            return base.OnAuthenticated(authService, session, tokens, authInfo);
        }

        public override void OnFailedAuthentication(IAuthSession session, IRequest httpReq, IResponse httpRes)
        {
            base.OnFailedAuthentication(session, httpReq, httpRes);
        }

        protected override ApiKey GetApiKey(IRequest req, string apiKey)
        {
            return base.GetApiKey(req, apiKey);
        }

        protected override IAuthRepository GetAuthRepository(IRequest req)
        {
            return base.GetAuthRepository(req);
        }

        protected override void LoadUserAuthInfo(AuthUserSession userSession, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            base.LoadUserAuthInfo(userSession, tokens, authInfo);
        }

        protected override void ValidateApiKey(ApiKey apiKey)
        {
            base.ValidateApiKey(apiKey);
        }

        public void PopulateSession(IUserAuthRepository authRepo, IUserAuth userAuth, CustomUserSession session, string userId)
        {
            if (authRepo == null)
                return;

            var holdSessionId = session.Id;
            session.PopulateWith(userAuth); //overwrites session.Id
            session.Id = holdSessionId;
            session.IsAuthenticated = true;
            session.UserAuthId = userId;

            string temp = userId.Substring(userId.IndexOf(CharConstants.COLON) + 1);
            session.Email = temp.Substring(0, temp.IndexOf(CharConstants.COLON));
            session.Uid = (userAuth as User).UserId;
            session.WhichConsole = userId.Substring(userId.Length - 2);
            session.Roles.Clear();
            session.Permissions.Clear();
        }
    }
}
