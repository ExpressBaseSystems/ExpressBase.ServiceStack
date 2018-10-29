using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class SchedulerServices :EbBaseService
    {
        public SchedulerServices(IMessageProducer _mqp, IMessageQueueClient _mqc) : base( _mqp, _mqc) { }

        public SchedulerMQResponse Post(SchedulerMQRequest request)
        {
            MessageProducer3.Publish(new SchedulerRequest { Task = request.Task });
            return null;
        }
    }
}
