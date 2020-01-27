using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class IoTService : EbBaseService
    {
        public object Any(IoTDataRequest request)
        {
            string _sql = $"INSERT INTO ronds_sample(json) values({request.Data});";
            int result = this.EbConnectionFactory.ObjectsDB.DoNonQuery(_sql);
            return new IoTDataResponse();
        }
    }
}
