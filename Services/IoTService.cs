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
            request.SolnId = "ebdblvnzp5spac20200127092930";
            string _sql = $"INSERT INTO ronds_sample(json) values({request.Data});";
            try
            {
                this.EbConnectionFactory = new Common.Data.EbConnectionFactory(request.SolnId, this.Redis);
            }
            catch(Exception e)
            {
                Console.WriteLine("IoT -----------------"+ e.Message + "Stacktrace:----------"+e.StackTrace);
            }
            int result = this.EbConnectionFactory.ObjectsDB.DoNonQuery(_sql);
            return new IoTDataResponse();
        }
    }
}
