using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Common;

namespace ExpressBase.ServiceStack.Services
{
    public class RedisClientServices : EbBaseService
    {
        public RedisClientServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public LogRedisInsertResponse Post(LogRedisInsertRequest request)
        {
            string query = @"INSERT INTO eb_redis_logs (changed_by, operation, changed_at, prev_value, new_value, soln_id, key) VALUES(:usr, :opn, NOW(), :prev, :new,
                            :sln, :key);";
            DbParameter[] parameters = {
                     EbConnectionFactory.ObjectsDB.GetNewParameter("usr", EbDbTypes.Int32, request.UserId),
                EbConnectionFactory.ObjectsDB.GetNewParameter("opn", EbDbTypes.Int32, request.Operation),
                EbConnectionFactory.ObjectsDB.GetNewParameter("prev", EbDbTypes.String, request.PreviousValue),
                EbConnectionFactory.ObjectsDB.GetNewParameter("new", EbDbTypes.String, request.NewValue),
                EbConnectionFactory.ObjectsDB.GetNewParameter("sln", EbDbTypes.Int32, request.SolutionId),
                EbConnectionFactory.ObjectsDB.GetNewParameter("key", EbDbTypes.String, request.Key)
            };
            this.EbConnectionFactory.ObjectsDB.DoNonQuery(query, parameters);
            return new LogRedisInsertResponse();
        }

        public LogRedisGetResponse Get(LogRedisGetRequest request)
        {

            List<EbRedisLogs> r_logs = new List<EbRedisLogs>();
            List<DbParameter> parameters = new List<DbParameter>();
            string query = @"SELECT changed_by, operation, changed_at, soln_id, key, id FROM eb_redis_logs WHERE soln_id = :slnid";
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("slnid", EbDbTypes.Int32, request.SolutionId));
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(query, parameters.ToArray());
            foreach (var item in dt.Rows)
            {
                EbRedisLogs eb = new EbRedisLogs
                {
                    LogId = Convert.ToInt32(item[5]),
                    ChangedBy = Convert.ToInt32(item[0]),
                    ChangedAt = Convert.ToDateTime(item[2]),
                    Operation = Enum.GetName(typeof(RedisOperations), item[1]),
                    Key = (item[4]).ToString()
                };
                r_logs.Add(eb);
            }
            return new LogRedisGetResponse {Logs = r_logs };
        }

        public LogRedisViewChangesResponse Get(LogRedisViewChangesRequest request)
        {

            //EbRedisLogValues ebRedisLogValues = new EbRedisLogValues();
            List<DbParameter> parameters = new List<DbParameter>();
            string query = @"SELECT prev_value, new_value FROM eb_redis_logs WHERE id = :logid";
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("logid", EbDbTypes.Int32, request.LogId));
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(query, parameters.ToArray());
            EbRedisLogValues logValues = new EbRedisLogValues
            {
                Prev_val = dt.Rows[0][0].ToString(),
                New_val = dt.Rows[0][1].ToString()

            };
            return new LogRedisViewChangesResponse { RedisLogValues = logValues };
        }
    }
}
