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
            request.SolnId = "ronds";
            string _sql = $"INSERT INTO ronds_sample(json) values({request.Data});";
            this.EbConnectionFactory = new Common.Data.EbConnectionFactory(request.SolnId,this.Redis);
            int result = this.EbConnectionFactory.ObjectsDB.DoNonQuery(_sql);
            return new IoTDataResponse();
        }
    }
}
