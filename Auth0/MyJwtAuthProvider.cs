using ExpressBase.Common.ServiceStack.Auth0;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using ServiceStack.Text;
using ServiceStack.Web;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;

namespace ExpressBase.ServiceStack.Auth0
{
    /// <summary>
    /// Used to Issue and process JWT Tokens and registers ConvertSessionToToken Service to convert Sessions to JWT Tokens
    /// </summary>
    public class MyJwtAuthProvider : JwtAuthProvider
    {
        public MyJwtAuthProvider() { }

        public MyJwtAuthProvider(IAppSettings appSettings) : base(appSettings) { }

        public override void Init(IAppSettings appSettings = null)
        {
            this.SetBearerTokenOnAuthenticateResponse = true;

            ServiceRoutes = new Dictionary<Type, string[]>
            {
                { typeof(ConvertSessionToTokenService), new[] { "/session-to-token" } },
                //{ typeof(GetAccessTokenService), new[] { "/access-token" } },
            };

            base.Init(appSettings);
        }

        public override string CreateJwtBearerToken(IAuthSession session, IEnumerable<string> roles = null, IEnumerable<string> perms = null) =>
            CreateJwtBearerToken(null, session, roles, perms);

        public override string CreateJwtBearerToken(IRequest req, IAuthSession session, IEnumerable<string> roles = null, IEnumerable<string> perms = null)
        {
            var jwtPayload = CreateJwtPayload(session, Issuer, ExpireTokensIn, Audience, roles, perms);
            CreatePayloadFilter?.Invoke(jwtPayload, session);

            if (EncryptPayload)
            {
                var publicKey = GetPublicKey(req);
                if (publicKey == null)
                    throw new NotSupportedException("PublicKey is required to EncryptPayload");

                return CreateEncryptedJweToken(jwtPayload, publicKey.Value);
            }

            var jwtHeader = CreateJwtHeader(HashAlgorithm, GetKeyId(req));
            CreateHeaderFilter?.Invoke(jwtHeader, session);

            var hashAlgoritm = GetHashAlgorithm(req);
            var bearerToken = CreateJwt(jwtHeader, jwtPayload, hashAlgoritm);
            return bearerToken;
        }

        public override JsonObject CreateJwtPayload(
            IAuthSession session, string issuer, TimeSpan expireIn,
            string audience = null,
            IEnumerable<string> roles = null,
            IEnumerable<string> permissions = null)
        {
            var now = DateTime.UtcNow;
            var jwtPayload = new JsonObject
            {
                {"iss", issuer},
                {"sub", session.UserAuthId},
                {"iat", now.ToUnixTime().ToString()},
                {"exp", now.Add(expireIn).ToUnixTime().ToString()},
            };

            if (audience != null)
                jwtPayload["aud"] = audience;

            var csession = session as CustomUserSession;

            string[] tempa = session.UserAuthId.Split('-');
            jwtPayload["email"] = tempa[1];
            jwtPayload["cid"] = tempa[0];
            jwtPayload["uid"] = csession.Uid.ToString();
            jwtPayload["wc"] = csession.WhichConsole;

            return jwtPayload;
        }
    }

    
    [DefaultRequest(typeof(GetAccessToken))]
    public class GetAccessTokenService : Service
    {
        public object Any(GetAccessToken request)
        {
            var jwtAuthProvider = (MyJwtAuthProvider)AuthenticateService.GetRequiredJwtAuthProvider();

            if (jwtAuthProvider.RequireSecureConnection && !Request.IsSecureConnection)
                throw HttpError.Forbidden(ErrorMessages.JwtRequiresSecureConnection.Localize(Request));

            if (string.IsNullOrEmpty(request.RefreshToken))
                throw new ArgumentNullException(nameof(request.RefreshToken));

            JsonObject jwtPayload;
            try
            {
                jwtPayload = jwtAuthProvider.GetVerifiedJwtPayload(Request, request.RefreshToken.Split('.'));
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new ArgumentException(ex.Message);
            }

            jwtAuthProvider.AssertJwtPayloadIsValid(jwtPayload);

            if (jwtAuthProvider.ValidateRefreshToken != null && !jwtAuthProvider.ValidateRefreshToken(jwtPayload, Request))
                throw new ArgumentException(ErrorMessages.RefreshTokenInvalid.Localize(Request), nameof(request.RefreshToken));

            var userId = jwtPayload["sub"];

            CustomUserSession session;
            if (AuthRepository is IUserAuthRepository userRepo)
            {
                var userAuth = userRepo.GetUserAuth(userId);
                if (userAuth == null)
                    throw HttpError.NotFound(ErrorMessages.UserNotExists.Localize(Request));

                if (jwtAuthProvider.IsAccountLocked(userRepo, userAuth))
                    throw new AuthenticationException(ErrorMessages.UserAccountLocked.Localize(Request));

                session = SessionFeature.CreateNewSession(Request, SessionExtensions.CreateRandomSessionId()) as CustomUserSession;
                PopulateSession(userRepo, userAuth, session, userId);
            }
            else
                throw new NotSupportedException("JWT RefreshTokens requires a registered IUserAuthRepository or an AuthProvider implementing IUserSessionSource");

            var accessToken = jwtAuthProvider.CreateJwtBearerToken(Request, session);

            return new GetAccessTokenResponse
            {
                AccessToken = accessToken
            };
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

            string temp = userId.Substring(userId.IndexOf('-') + 1);
            session.Email = temp.Substring(0, temp.IndexOf('-'));
            session.Uid = (userAuth as User).UserId;
            session.WhichConsole = userId.Substring(userId.Length - 2);
            session.Roles.Clear();
            session.Permissions.Clear();
        }
    }
}
