using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Data.Common;
using static ExpressBase.ServiceStack.EmailService;

namespace ExpressBase.ServiceStack.Auth0
{
    public class EbRegisterService : EbBaseService
    {
        public EbRegisterService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        public RegisterResponse Post(RegisterRequest request)
        {
            var response = new RegisterResponse(); 
            var _InfraDb = base.ResolveService<IEbConnectionFactory>() as EbConnectionFactory;

            DbParameter[] parameters = {
                _InfraDb.DataDB.GetNewParameter("email", System.Data.DbType.String, request.Email)
               // _InfraDb.DataDB.GetNewParameter("pwd", System.Data.DbType.String, (request.Password + request.Email).ToMD5Hash())
            };

            EbDataTable dt = _InfraDb.DataDB.DoQuery("INSERT INTO eb_users (email, u_token) VALUES ( @email, md5( @email || now())) RETURNING id, u_token;", parameters);
            
            if (dt.Rows.Count > 0)
            {
                try
                {
                    var myService = base.ResolveService<EmailServiceInternal>();
                    myService.Post(new EmailServicesMqRequest() { refid = "expressbase-expressbase-15-26-26", TenantAccountId = request.TenantAccountId, newuserid = Convert.ToInt32(dt.Rows[0][0]), To = request.Email, UserId = Convert.ToInt32(dt.Rows[0][0]) });
                    //base.MessageProducer3.Publish(new EmailServicesMqRequest { refid = "expressbase-expressbase-15-26-26", TenantAccountId = request.TenantAccountId, newuserid = Convert.ToInt32(dt.Rows[0][0]), To = request.Email, UserId = Convert.ToInt32(dt.Rows[0][0]) });
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