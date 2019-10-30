using ExpressBase.Objects.Services;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    [Restrict(InternalOnly = true)]
    public class SqlJobInternalService : EbMqBaseService
    {
    }
}
