using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Common.SqlProfiler;

namespace ExpressBase.ServiceStack.Services
{
    public class ProfilerServices : EbBaseService
    {
        public ProfilerServices(IEbConnectionFactory _dbf) : base(_dbf) { }
        public GetExecLogsResponse Get(GetExecLogsRequest request)
        {
            List<EbExecutionLogs> _logs = new List<EbExecutionLogs>();
            List<DbParameter> parameters = new List<DbParameter>();
            string query = @"select * from eb_executionlogs where refid=:refid";
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId));
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(query, parameters.ToArray());
            foreach (var item in dt.Rows)
            {
                EbExecutionLogs eb = new EbExecutionLogs { Id = Convert.ToInt32(item[0]), Rows = item[1].ToString(), Exec_time = Convert.ToDecimal(item[2]), Created_by = Convert.ToInt32(item[3]), Created_at = Convert.ToDateTime(item[4]), Params = Convert.ToString(item[6]) };
                _logs.Add(eb);
            }
            return new GetExecLogsResponse { Logs = _logs };
        }

        public GetProfilersResponse Get(GetProfilersRequest request)
        {
            List<EbExecutionLogs> _logs = new List<EbExecutionLogs>();
            List<DbParameter> parameters = new List<DbParameter>();
            Profiler profiler = new Profiler();
            string query = @"select id,exec_time from eb_executionlogs where exec_time=(select max(exec_time) from eb_executionlogs where refid=:refid);
                             select id,exec_time from eb_executionlogs where exec_time=(select min(exec_time) from eb_executionlogs where refid=:refid);
                             select id,exec_time from eb_executionlogs where exec_time=(select max(exec_time) from eb_executionlogs where refid=:refid and extract (month from created_at) = extract(month from current_date));
                             select id,exec_time from eb_executionlogs where exec_time=(select min(exec_time) from eb_executionlogs where refid=:refid and extract (month from created_at) = extract(month from current_date));
                             select count(*) from eb_executionlogs where refid=:refid;
                             select count(*) from eb_executionlogs where created_at::date = current_date;
                             select count(*) from eb_executionlogs where extract (month from created_at) = extract(month from current_date) and refid=:refid;";
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId));
            EbDataSet dt = EbConnectionFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
            if (dt.Tables.Count > 0)
                if (dt.Tables[0].Rows.Count > 0)
                {
                    profiler.Max_id = Convert.ToInt32(dt.Tables[0].Rows[0][0]);
                    profiler.Max_exectime = Convert.ToDecimal(dt.Tables[0].Rows[0][1]);
                    profiler.Min_id = Convert.ToInt32(dt.Tables[1].Rows[0][0]);
                    profiler.Min_exectime = Convert.ToDecimal(dt.Tables[1].Rows[0][1]);
                    profiler.Cur_Max_id = Convert.ToInt32(dt.Tables[2].Rows[0][0]);
                    profiler.Cur_Max_exectime = Convert.ToDecimal(dt.Tables[2].Rows[0][1]);
                    profiler.Cur_Min_id = Convert.ToInt32(dt.Tables[3].Rows[0][0]);
                    profiler.Cur_Min_exectime = Convert.ToDecimal(dt.Tables[3].Rows[0][1]);
                    profiler.Total_count = Convert.ToInt32(dt.Tables[4].Rows[0][0]);
                    profiler.Current_count = Convert.ToInt32(dt.Tables[5].Rows[0][0]);
                    profiler.Month_count = Convert.ToInt32(dt.Tables[6].Rows[0][0]);
                }

            return new GetProfilersResponse { Profiler = profiler };
        }
    }

}
