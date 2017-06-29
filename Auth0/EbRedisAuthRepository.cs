using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Auth0
{
    public class EbRedisAuthRepository: RedisAuthRepository
    {
        new public string NamespacePrefix { get; set; }

        private string UsePrefix => NamespacePrefix ?? "";

        private IRedisClientsManager Factory { get; set; }

        private string IndexEmailToUserId => UsePrefix + "hash:UserAuth:Email>UserId";

        public EbRedisAuthRepository(IRedisClientsManager factory) : base(factory)
        {
            this.Factory = factory;
        }

        public override IUserAuth CreateUserAuth(IUserAuth newUser, string password)
        {
            //newUser.ValidateNewUser(password);

            using (var redis = this.Factory.GetClient())
            {
                //base.AssertNoExistingUser(redis, newUser);

                //var saltedHash = HostContext.Resolve<IHashProvider>();
                //string salt;
                //string hash;
                //saltedHash.GetHashAndSaltString(password, out hash, out salt);

                //newUser.Id = redis.As<IUserAuth>().GetNextSequence();
                //newUser.PasswordHash = hash;
                //newUser.Salt = salt;
                //var digestHelper = new DigestAuthFunctions();
                //newUser.DigestHa1Hash = digestHelper.CreateHa1(newUser.UserName, DigestAuthProvider.Realm, password);
                newUser.CreatedDate = DateTime.UtcNow;
                newUser.ModifiedDate = newUser.CreatedDate;

                //var userId = newUser.Id.ToString(CultureInfo.InvariantCulture);
                //redis.SetEntryInHash(IndexEmailToUserId, newUser.Email, userId);

                redis.Store(newUser);

                return newUser;
            }
        }

       new public IUserAuth UpdateUserAuth(IUserAuth existingUser, IUserAuth newUser, string password)
        {
            newUser.ValidateNewUser(password);

            using (var redis = this.Factory.GetClient())
            {
                // AssertNoExistingUser(redis, newUser, existingUser);

                //if (existingUser.UserName != newUser.UserName && existingUser.UserName != null)
                //{
                //    redis.RemoveEntryFromHash(IndexUserNameToUserId, existingUser.UserName);
                //}
                //if (existingUser.Email != newUser.Email && existingUser.Email != null)
                //{
                //    redis.RemoveEntryFromHash(IndexEmailToUserId, existingUser.Email);
                //}

                //var hash = existingUser.PasswordHash;
                //var salt = existingUser.Salt;
                //if (password != null)
                //{
                //    var saltedHash = HostContext.Resolve<IHashProvider>();
                //    saltedHash.GetHashAndSaltString(password, out hash, out salt);
                //}

                //// If either one changes the digest hash has to be recalculated
                //var digestHash = existingUser.DigestHa1Hash;
                //if (password != null || existingUser.UserName != newUser.UserName)
                //    digestHash = new DigestAuthFunctions().CreateHa1(newUser.UserName, DigestAuthProvider.Realm, password);

                newUser.Id = existingUser.Id;
                //newUser.PasswordHash = hash;
                //newUser.Salt = salt;
                //newUser.DigestHa1Hash = digestHash;
                newUser.CreatedDate = existingUser.CreatedDate;
                newUser.ModifiedDate = DateTime.UtcNow;

              //  var userId = newUser.Id.ToString(CultureInfo.InvariantCulture);
                //if (!newUser.UserName.IsNullOrEmpty())
                //{
                //    redis.SetEntryInHash(IndexUserNameToUserId, newUser.UserName, userId);
                //}
                //if (!newUser.Email.IsNullOrEmpty())
                //{
                //    redis.SetEntryInHash(IndexEmailToUserId, newUser.Email, userId);
                //}

                redis.Store(newUser);

                return newUser;
            }
        }
    }
}
