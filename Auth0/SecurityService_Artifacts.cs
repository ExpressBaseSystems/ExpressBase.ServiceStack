using ExpressBase.Common;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Configuration;
using System;
using System.Collections.Generic;
using ServiceStack.Web;
using System.IO;
using ExpressBase.Data;
using ServiceStack.Logging;
using System.Runtime.Serialization;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System.Globalization;
using Funq;
using ServiceStack.Redis;
using ExpressBase.ServiceStack.Auth0;
using ExpressBase.Common.Data;

namespace ExpressBase.Objects.ServiceStack_Artifacts
{
    [DataContract]
    public class CustomUserSession : AuthUserSession
    {
        [DataMember(Order = 1)]
        public string CId { get; set; }

        [DataMember(Order = 2)]
        public int Uid { get; set; }

        [DataMember(Order = 3)]
        public User User { get; set; }

        [DataMember(Order = 4)]
        public string WhichConsole { get; set; }
      

        public override bool IsAuthorized(string provider)
        {
            return true;
        }

        private static string CreateGravatarUrl(string email, int size = 64)
        {
            var md5 = System.Security.Cryptography.MD5.Create();
            var md5HadhBytes = md5.ComputeHash(email.ToUtf8Bytes());

            var sb = new System.Text.StringBuilder();
            for (var i = 0; i < md5HadhBytes.Length; i++)
                sb.Append(md5HadhBytes[i].ToString("x2"));

            string gravatarUrl = "http://www.gravatar.com/avatar/{0}?d=mm&s={1}".Fmt(sb, size);
            return gravatarUrl;
        }

        public override bool FromToken
        {
            get
            {
                return base.FromToken;
            }

            set
            {
                base.FromToken = value;
            }
        }

        public override string ProfileUrl
        {
            get
            {
                return base.ProfileUrl;
            }

            set
            {
                base.ProfileUrl = value;
            }
        }

        public override string Sequence
        {
            get
            {
                return base.Sequence;
            }

            set
            {
                base.Sequence = value;
            }
        }

        public override bool IsAuthenticated
        {
            get
            {
                return base.IsAuthenticated;
            }

            set
            {
                base.IsAuthenticated = value;
            }
        }

        public override void OnCreated(IRequest httpReq)
        {
            base.OnCreated(httpReq);
        }

        public override void OnAuthenticated(IServiceBase authService, IAuthSession session, IAuthTokens tokens, Dictionary<string, string> authInfo)
        {
            base.OnAuthenticated(authService, this, tokens, authInfo);
            ILog log = LogManager.GetLogger(GetType());

            log.Info("In OnAuthenticated method");
            //Populate all matching fields from this session to your own custom User table
            var user = session.ConvertTo<User>();
            user.Id = (session as CustomUserSession).Uid;

            foreach (var authToken in session.ProviderOAuthAccess)
            {
                if (authToken.Provider == FacebookAuthProvider.Name)
                {
                    user.UserName = authToken.DisplayName;
                    user.FirstName = authToken.FirstName;
                    user.LastName = authToken.LastName;
                    user.Email = authToken.Email;
                    //session.bea
                }
                //else if (authToken.Provider == TwitterAuthProvider.Name)
                //{
                //    user.TwitterName = user.DisplayName = authToken.UserName;
                //}
                //else if (authToken.Provider == YahooOpenIdOAuthProvider.Name)
                //{
                //    user.YahooUserId = authToken.UserId;
                //    user.YahooFullName = authToken.FullName;
                //    user.YahooEmail = authToken.Email;
                //}
            }

            //var userAuthRepo = authService.TryResolve<IAuthRepository>();
            //if (AppHost.AppConfig.AdminUserNames.Contains(session.UserAuthName)
            //    && !session.HasRole(RoleNames.Admin, userAuthRepo))
            //{
            //    var userAuth = userAuthRepo.GetUserAuth(session, tokens);
            //    userAuthRepo.AssignRoles(userAuth, roles: new[] { RoleNames.Admin });
            //}

            //Resolve the DbFactory from the IOC and persist the user info
            //using (var db = authService.TryResolve<IDbConnectionFactory>().Open())
            //{
            //    db.Save(user);
            //}
        }
    }
}
