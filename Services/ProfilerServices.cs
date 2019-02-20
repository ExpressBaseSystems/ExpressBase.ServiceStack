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
            string query = @"SELECT id, rows, exec_time, created_by, created_at, refid, params FROM eb_executionlogs WHERE refid = :refid";
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
            string query = @"SELECT id, exec_time FROM eb_executionlogs WHERE exec_time=(SELECT MAX(exec_time) FROM eb_executionlogs WHERE refid = :refid);
                             SELECT id, exec_time FROM eb_executionlogs WHERE exec_time=(SELECT MIN(exec_time) FROM eb_executionlogs WHERE refid = :refid);
                             SELECT id, exec_time FROM eb_executionlogs WHERE exec_time=(SELECT MAX(exec_time) FROM eb_executionlogs WHERE refid = :refid AND EXTRACT (month FROM created_at) = EXTRACT(month FROM current_date));
                             SELECT id, exec_time FROM eb_executionlogs WHERE exec_time=(SELECT MIN(exec_time) FROM eb_executionlogs WHERE refid = :refid AND EXTRACT (month FROM created_at) = EXTRACT(month FROM current_date));
                             SELECT id, exec_time FROM eb_executionlogs WHERE exec_time=(SELECT MAX(exec_time) FROM eb_executionlogs WHERE refid= :refid and created_at::date = current_date);
                             SELECT id, exec_time FROM eb_executionlogs WHERE exec_time=(SELECT MIN(exec_time) FROM eb_executionlogs WHERE refid= :refid and created_at::date = current_date);
                             SELECT COUNT(*) FROM eb_executionlogs WHERE refid = :refid;
                             SELECT COUNT(*) FROM eb_executionlogs WHERE created_at::date = current_date;
                             SELECT COUNT(*) FROM eb_executionlogs WHERE extract (month FROM created_at) = extract(month FROM current_date) and refid = :refid;";
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId));
            EbDataSet dt = EbConnectionFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
            if (dt.Tables.Count > 0)
                if (dt.Tables[0].Rows.Count > 0)
                {
                    profiler.Max_id = (dt.Tables[0].Rows.Count != 0) ? Convert.ToInt32(dt.Tables[0].Rows[0][0]) : 0;
                    profiler.Max_exectime = (dt.Tables[0].Rows.Count != 0) ? Convert.ToDecimal(dt.Tables[0].Rows[0][1]) : Convert.ToDecimal(0);
                    profiler.Min_id = (dt.Tables[1].Rows.Count != 0) ? Convert.ToInt32(dt.Tables[1].Rows[0][0]) : 0;
                    profiler.Min_exectime = (dt.Tables[1].Rows.Count != 0) ? Convert.ToDecimal(dt.Tables[1].Rows[0][1]) : Convert.ToDecimal(0);
                    profiler.Cur_Mon_Max_id = (dt.Tables[2].Rows.Count != 0) ? Convert.ToInt32(dt.Tables[2].Rows[0][0]) : 0;
                    profiler.Cur_Mon_Max_exectime = (dt.Tables[2].Rows.Count != 0) ? Convert.ToDecimal(dt.Tables[2].Rows[0][1]) : Convert.ToDecimal(0);
                    profiler.Cur_Mon_Min_id = (dt.Tables[3].Rows.Count != 0) ? Convert.ToInt32(dt.Tables[3].Rows[0][0]) : 0;
                    profiler.Cur_Mon_Min_exectime = (dt.Tables[3].Rows.Count != 0) ? Convert.ToDecimal(dt.Tables[3].Rows[0][1]) : Convert.ToDecimal(0);
                    profiler.Cur_Max_id = (dt.Tables[4].Rows.Count != 0) ? Convert.ToInt32(dt.Tables[4].Rows[0][0]) : 0;
                    profiler.Cur_Max_exectime = (dt.Tables[4].Rows.Count != 0) ? Convert.ToDecimal(dt.Tables[4].Rows[0][1]) : Convert.ToDecimal(0);
                    profiler.Cur_Min_id = (dt.Tables[5].Rows.Count != 0) ? Convert.ToInt32(dt.Tables[5].Rows[0][0]) : 0;
                    profiler.Cur_Min_exectime = (dt.Tables[5].Rows.Count != 0) ? Convert.ToDecimal(dt.Tables[5].Rows[0][1]) : Convert.ToDecimal(0);
                    profiler.Total_count = (dt.Tables[6].Rows.Count != 0) ? Convert.ToInt32(dt.Tables[6].Rows[0][0]) : 0;
                    profiler.Current_count = (dt.Tables[7].Rows.Count != 0) ? Convert.ToInt32(dt.Tables[7].Rows[0][0]) : 0;
                    profiler.Month_count = (dt.Tables[8].Rows.Count != 0) ? Convert.ToInt32(dt.Tables[8].Rows[0][0]) : 0;
                }

            return new GetProfilersResponse { Profiler = profiler };
        }
    }

}
