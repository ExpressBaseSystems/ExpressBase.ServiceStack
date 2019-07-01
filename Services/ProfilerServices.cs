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
using Newtonsoft.Json;

namespace ExpressBase.ServiceStack.Services
{
    public class ProfilerServices : EbBaseService
    {
        public ProfilerServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public ProfilerQueryResponse Get(ProfilerQueryColumnRequest request)
        {
            List<EbExecutionLogs> _logs = new List<EbExecutionLogs>();
            List<DbParameter> parameters = new List<DbParameter>();
            string query = EbConnectionFactory.ObjectsDB.EB_PROFILER_QUERY_COLUMN;
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId));
            var _dt = EbConnectionFactory.ObjectsDB.DoQuery(query, parameters.ToArray());
            return new ProfilerQueryResponse { ColumnCollection = _dt.Columns, data = _dt.Rows };
        }

        public ProfilerQueryResponse Get(ProfilerQueryDataRequest request)
        {
            List<DbParameter> parameters = new List<DbParameter>();
            string query = EbConnectionFactory.ObjectsDB.EB_PROFILER_QUERY_DATA;
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId));
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("limit", EbDbTypes.Int32, request.Length));
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("offset", EbDbTypes.Int32, request.Start));
            var _ds = EbConnectionFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
            return new ProfilerQueryResponse { Draw = request.Draw, data = _ds.Tables[1].Rows, RecordsTotal = Convert.ToInt32(_ds.Tables[0].Rows[0][0]), RecordsFiltered = Convert.ToInt32(_ds.Tables[0].Rows[0][0]) };
        }

        public GetProfilersResponse Get(GetProfilersRequest request)
        {
            List<EbExecutionLogs> _logs = new List<EbExecutionLogs>();
            List<DbParameter> parameters = new List<DbParameter>();
            Profiler profiler = new Profiler();
            string query = EbConnectionFactory.ObjectsDB.EB_GET_PROFILERS;
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId));
            EbDataSet dt = EbConnectionFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
            if (dt.Tables.Count > 0)
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

        public GetLogdetailsResponse Get(GetLogdetailsRequest request)
        {
            string sql = "SELECT * FROM eb_executionlogs WHERE id = :id";
            DbParameter[] p = { EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Index) };
            EbDataTable _logdetails = EbConnectionFactory.ObjectsDB.DoQuery(sql, p);

            EbExecutionLogs logs = null;
            if (_logdetails.Rows.Count > 0)
                logs = new EbExecutionLogs { Rows = _logdetails.Rows[0][1].ToString(), Exec_time = Convert.ToInt32(_logdetails.Rows[0][2]), Username = _logdetails.Rows[0][3].ToString(), Created_at = Convert.ToDateTime(_logdetails.Rows[0][4]), Params = JsonConvert.DeserializeObject<List<JsonParams>>(_logdetails.Rows[0][6].ToString()) };
            return new GetLogdetailsResponse { logdetails = logs };
        }

        public GetChartDetailsResponse Get(GetChartDetailsRequest request)
        {
            string sql = EbConnectionFactory.ObjectsDB.EB_GET_CHART_DETAILS;
            DbParameter[] p = { EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.Refid) };
            EbDataTable _chartdetails = EbConnectionFactory.ObjectsDB.DoQuery(sql, p);
            return new GetChartDetailsResponse { data = _chartdetails.Rows };
        }

        public GetChart2DetailsResponse Get(GetChart2DetailsRequest request)
        {
            string sql = EbConnectionFactory.ObjectsDB.EB_GET_CHART_2_DETAILS;
            DbParameter[] p = { EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.Refid) };
            EbDataTable _chartdetails = EbConnectionFactory.ObjectsDB.DoQuery(sql, p);
            return new GetChart2DetailsResponse { data = _chartdetails.Rows };
        }

        public GetExplainResponse Get(GetExplainRequest request)
        {
            string query = request.Query.Split(";")[0];
            //string sql = "EXPLAIN FORMAT=json " + query + ";";  mysql
            string sql = "explain (format json, analyze on) " + query + ";";
            var parameters = DataHelper.GetParams(this.EbConnectionFactory, false, request.Params, 0, 0);
            EbDataTable _explain = EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters.ToArray<System.Data.Common.DbParameter>());
            return new GetExplainResponse { Explain = _explain.Rows[0][0].ToString() };
        }
    }
}
