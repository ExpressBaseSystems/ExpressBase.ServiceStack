using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.Constants;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Data;

namespace ExpressBase.ServiceStack.Services
{
    public class NotificationTestService : EbBaseService
    {
        public NotificationTestService(IEbConnectionFactory _dbf, IEbServerEventClient _sec) : base(_dbf, _sec) { }

        public NotifyTestResponse Post(NotifyTestRequest request)
        {
            NotifyTestResponse res = new NotifyTestResponse();
            this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
            {
                Msg = "LogOut",
                Selector = "cmd.onLogOut",
                ToUserAuthId = request.UserAuthId,
            });
            return res;
        }
    }
}
