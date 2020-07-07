using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class IoTService : EbBaseService
    {       
        public object Any(IoTDataRequest request)
        {
            request.SolnId = "ebdblvnzp5spac20200127092930";
            string _sql = "INSERT INTO ronds_sample(json) values(:json);";
            try
            {
                this.EbConnectionFactory = new Common.Data.EbConnectionFactory(request.SolnId, this.Redis);
                DbParameter[] parameters = new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("json", Common.Structures.EbDbTypes.String, request.Data) };
                int result = this.EbConnectionFactory.ObjectsDB.DoNonQuery(_sql, parameters);
            }
            catch (Exception e)
            {
                Console.WriteLine("IoT -----------------" + e.Message + "Stacktrace:----------" + e.StackTrace);
            }
            return new IoTDataResponse();
        }
    }
}
