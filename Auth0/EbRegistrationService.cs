using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Data.Common;

namespace ExpressBase.ServiceStack.Auth0
{
    public class EbRegisterService : EbBaseService
    {
        public EbRegisterService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        public RegisterResponse Post(RegisterRequest request)
        {
            var response = new RegisterResponse(); 
            var _InfraDb = base.ResolveService<ITenantDbFactory>() as TenantDbFactory;

            DbParameter[] parameters = {
                _InfraDb.DataDB.GetNewParameter("email", System.Data.DbType.String, request.Email),
                _InfraDb.DataDB.GetNewParameter("pwd", System.Data.DbType.String, (request.Password + request.Email).ToMD5Hash())
            };

            EbDataTable dt = _InfraDb.DataDB.DoQuery("INSERT INTO eb_users (email, pwd, u_token) VALUES ( @email, @pwd, md5( @email || now())) RETURNING id, u_token;", parameters);
            
            if (dt.Rows.Count > 0)
            {
                try
                {
                    base.MessageProducer3.Publish(new EmailServicesMqRequest { Message = string.Format("http://localhost:5000/Ext/VerificationStatus?signup_tok={0}&email={1}", dt.Rows[0][1].ToString(), request.Email), TenantAccountId = request.TenantAccountId, Subject = "EXPRESSbase Signup Confirmation", To = request.Email, UserId =Convert.ToInt32(dt.Rows[0][0]) });
                    response.UserName = dt.Rows[0][1].ToString();
                    response.UserId = dt.Rows[0][0].ToString();
                }
                catch (Exception e) {
                  
                }
            }

            return response;
        }
    }
}