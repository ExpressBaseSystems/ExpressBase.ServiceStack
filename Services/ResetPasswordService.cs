using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using ExpressBase.Security;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Common.Data;
using ServiceStack.Auth;
using System.Data.Common;
using ExpressBase.Common.Structures;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Security;

namespace ExpressBase.ServiceStack.Services
{
    public class ResetPasswordService : EbBaseService
    {
       // public MyAuthenticateResponse MyAuthenticateResponse { get; set; }

        public TwoFactorAuthServices TwoFAService { get; set; }

        public ResetPasswordService(IEbConnectionFactory _dbf) : base(_dbf)
        {
            this.TwoFAService = base.ResolveService<TwoFactorAuthServices>();
        }

        //public GetResetPwPageResponse Get(GetResetPwPageRequest request)
        //{
        //    GetResetPwPageResponse resp = new GetResetPwPageResponse(); 
        //    try
        //    {
        //        resp.RPWToken = EbTokenGenerator.GenerateToken(request.UserAuthId);
        //        User u = GetUserObject(request.UserAuthId);
        //        u.BearerToken = request.BearerToken;
        //        u.RefreshToken = request.RefreshToken;
        //        this.Redis.Set<IUserAuth>(request.UserAuthId, u); 
        //    }
        //    catch (Exception e)
        //    {
        //        resp.ErrorMessage = e.Message;
        //        Console.WriteLine(e.Message + e.StackTrace);
        //    }
        //    return resp;
        //}

        public ResetPwResponse Post(ResetPwRequest request)
        {
            ResetPwResponse resp = new ResetPwResponse();
            try
            {
                this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
                string checkQ = string.Format("SELECT pwd FROM eb_users WHERE id = {0}", request.UserId);
                string currentPwd_db = this.EbConnectionFactory.DataDB.ExecuteScalar<string>(checkQ);
                string currentPwdOldFormat = (request.PwDetails.CurrentPassword + request.Email).ToMD5Hash();
                string NewPwdoldformat = (request.PwDetails.NewPassword + request.Email).ToMD5Hash();
                if (currentPwd_db == currentPwdOldFormat || request.PwDetails.IsForgotPw)
                {
                    if (currentPwd_db != NewPwdoldformat || request.PwDetails.IsForgotPw)
                    {
                        string NewPwdnewformat = (request.PwDetails.NewPassword.ToMD5Hash() + request.UserId.ToString() + request.SolnId).ToMD5Hash();
                        string updateQ = "UPDATE eb_users SET pwd = @oldformat , pw = @newformat , forcepwreset = 'F' WHERE id = @userid;";
                        DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                this.EbConnectionFactory.DataDB.GetNewParameter("oldformat", EbDbTypes.String,NewPwdoldformat),
                this.EbConnectionFactory.DataDB.GetNewParameter("newformat", EbDbTypes.String, NewPwdnewformat)};
                        resp.Status = (this.EbConnectionFactory.DataDB.DoNonQuery(updateQ, parameters) == 1) ? true : false;
                    }
                    else
                    {
                        resp.ErrorMessage = "You cannot reset to a previous password. Make sure you entered a new password.";
                    }
                }
                else
                {
                    resp.ErrorMessage = "Current password is incorrect";
                }
            }
            catch (Exception e)
            {
                resp.ErrorMessage = "Something went wrong";
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return resp;
        }

       
    }
}
