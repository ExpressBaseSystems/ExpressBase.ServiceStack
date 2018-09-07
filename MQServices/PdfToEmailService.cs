using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.EmailRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    public class PdfToEmailService : EbBaseService
    {
        public PdfToEmailService(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_dbf, _mqp, _mqc, _sec) { }

        public void Post(PdfCreateServiceMqRequest request)
        {
            MessageProducer3.Publish(new PdfCreateServiceRequest()
            {
                Refid = request.Refid,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                TenantAccountId = request.TenantAccountId
            });
        }
    }
}
