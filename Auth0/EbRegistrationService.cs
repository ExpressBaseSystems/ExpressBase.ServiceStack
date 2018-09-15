using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Structures;
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
                _InfraDb.DataDB.GetNewParameter("email", EbDbTypes.String, request.Email)
               // _InfraDb.DataDB.GetNewParameter("pwd", System.Data.DbType.String, (request.Password + request.Email).ToMD5Hash())
            };

            EbDataTable dt = _InfraDb.DataDB.DoQuery("INSERT INTO eb_tenants (email) VALUES ( @email) RETURNING id;", parameters);

            if (dt.Rows.Count > 0)
            {
                try
                {
                    response.UserId = dt.Rows[0][0].ToString();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception: " + e.ToString());
                }
            }

            return response;
        }
    }
}