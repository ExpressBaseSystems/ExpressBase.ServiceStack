using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.Constants;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Data;
using ServiceStack;

namespace ExpressBase.ServiceStack.Services
{
    public class NotificationTestService : EbBaseService
    {
        public NotificationTestService(IEbConnectionFactory _dbf, IEbServerEventClient _sec) : base(_dbf, _sec) { }

        public NotifyTestResponse Post(NotifyTestRequest request)
        {
            NotifyTestResponse res = new NotifyTestResponse();
            Notifications n = new Notifications();
            n.Add(new NotificationContents
            {
                Link = "abc.xyz",
                Title = "abc"
            });
            this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
            {
                Msg = JSON.stringify(n),
                Selector = "cmd.onNotification",
                ToUserAuthId = request.UserAuthId,
            });

            //this.ServerEventClient.Post<NotifyResponse>(new NotifySubsribtionRequest
            //{
            //    Msg = "LogOut",
            //    Selector = "cmd.onLogOut"
            //});
            return res;
        }
    }
}
