using ExpressBase.Common.Constants;
using ExpressBase.Common.ServiceStack.Auth;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Auth0
{
    public class EbGetAccessTokenService
    {
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
                    jwtPayload = jwtAuthProvider.GetVerifiedJwtPayload(Request, request.RefreshToken.Split(CharConstants.DOT));
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

                var userId = jwtPayload[TokenConstants.SUB];

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

                string temp = userId.Substring(userId.IndexOf(CharConstants.COLON) + 1);
                session.Email = temp.Substring(0, temp.IndexOf(CharConstants.COLON));
                session.Uid = (userAuth as User).UserId;
                session.WhichConsole = userId.Substring(userId.Length - 2);
                session.SourceIp = (userAuth as User).SourceIp;
                session.Roles.Clear();
                session.Permissions.Clear();
            }
        }
    }
}
