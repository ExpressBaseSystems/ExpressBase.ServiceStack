using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class SqlJobServices : EbBaseService
    {

        public int LogMasterId { get; set; }

        public int UserId { get; set; }

        public bool IsRetry = false;

        public Dictionary<string, TV> GlobalParams { set; get; }

        public Dictionary<string, object> TempParams { set; get; }

        private SqlJobResponse JobResponse { get; set; }

        private EbObjectService StudioServices { set; get; }

        public SqlJobServices(IEbConnectionFactory _dbf) : base(_dbf)
        { 
            this.StudioServices = base.ResolveService<EbObjectService>();
            this.JobResponse = new SqlJobResponse();
        }
        private Dictionary<string, TV> _keyValuePairs = null;

        public Dictionary<string, TV> GetKeyvalueDict
        {
            get
            {
                if (_keyValuePairs == null)
                {
                    _keyValuePairs = new Dictionary<string, TV>();
                    foreach (string key in this.SqlJob.FirstReaderKeyColumns)
                    {
                        if (!_keyValuePairs.ContainsKey(key))
                            _keyValuePairs.Add(key, new TV { });
                    }
                    foreach (string key in this.SqlJob.ParameterKeyColumns)
                    {
                        if (!_keyValuePairs.ContainsKey(key))
                            _keyValuePairs.Add(key, new TV { });
                    }
                }                
                return _keyValuePairs;
            }
        }

        public EbSqlJob SqlJob = new EbSqlJob
        {
            RefId = "My-Testid",
            FirstReaderKeyColumns = new List<string> { "empmaster_name" },
            ParameterKeyColumns = new List<string> { "date_to_consolidate" },
            Resources = new OrderedList
            {
                new EbSqlJobReader
                {
                    Reference="ebdbllz23nkqd620180220120030-ebdbllz23nkqd620180220120030-2-2601-3480-2601-3480",
                    RouteIndex=0
                },
                new EbLoop
                {
                    RouteIndex=1,
                    InnerResources =new OrderedList
                    {
                        new  EbTransaction
                        {
                                RouteIndex=0,
                                InnerResources = new OrderedList
                                {
                                    new EbSqlJobReader
                                    {
                                        Reference="ebdbllz23nkqd620180220120030-ebdbllz23nkqd620180220120030-2-2603-3477-2603-3477",
                                        RouteIndex=0
                                    },
                                    new EbSqlProcessor
                                    {
                                        Script = new EbScript
                                        {
                                           // Code="Job.SetParam(\"val\",(Tables[0].Rows.Count>0)?Convert.ToInt32(Tables[0].Rows[0][0])/1000:0); return 100;",
                                           // Code="Job.SetParam(\"val\",200);",
                                           Code=@"  
    internal class Attendance
    {
        internal int Empmaster_id { get; set; }
        internal DateTime In_time { get; set; }
        internal DateTime Out_time { get; set; }
        internal int IWork { get; set; }
        internal int IBreak { get; set; }
        internal int IOverTime { get; set; }
        internal int IOTHours { get; set; }
        internal int IOTMinutes { get; set; }
        internal string Notes { get; set; }
        internal bool IsNightshift { get; set; }

        internal int App_att_inout_id { get; set; }

        internal Attendance(int empmaster_id)
        {
            this.Empmaster_id = empmaster_id;
        }

        internal Attendance(int empmaster_id, DateTime in_time, DateTime out_time, int iWork, int iBreak, int iOverTime, int iOTHours, int iOTMinutes, string notes, bool bNightshift)
        {
            this.Empmaster_id = empmaster_id;
            this.In_time = in_time;
            this.Out_time = out_time;
            this.IWork = iWork;
            this.IBreak = iBreak;
            this.IOverTime = iOverTime;
            this.IOTHours = iOTHours;
            this.IOTMinutes = iOTMinutes;
            this.Notes = notes;
            this.IsNightshift = bNightshift;
        }
    }
 
        DateTime dateInQuestion = Convert.ToDateTime(Params.date_to_consolidate);
        int empmaster_id =  Convert.ToInt32(Params.empid);

        DateTime dtFirstIn = dateInQuestion;
        DateTime dtLastIn = dateInQuestion;
        DateTime dtLastOut = dateInQuestion;
        string lastKnownStatus = ""UnKnown"";
        string lastKnownInOutStatus = ""UnKnown"";
        string currentStatus = ""UnKnown"";
        int iPos = 0;
        string status = ""In"";
        Attendance att = new Attendance(empmaster_id);
         EbDataTable dt_devattlogs =  Tables[0];

            if (dt_devattlogs.Rows.Count > 1)
            {

                EbDataRow row = dt_devattlogs.Rows[0];
                foreach ( EbDataRow _row_devattlogs in dt_devattlogs.Rows)
                {
                    if (iPos >= dt_devattlogs.Rows.IndexOf(row))
                    {
                        DateTime _punched_at = Convert.ToDateTime(_row_devattlogs[""punched_at""]);

                        if (iPos == dt_devattlogs.Rows.IndexOf(row))
                        {
                            currentStatus = status;
                 //  if (!att.IsNightshift)
                 dtFirstIn = _punched_at;
                            att.In_time = dtFirstIn;
                            dtLastIn = dtFirstIn;
                        }
                        if (iPos > dt_devattlogs.Rows.IndexOf(row))
                        {
                            if (lastKnownStatus == ""In"")
                            {
                                if ((_punched_at - dtLastIn).TotalMinutes > 5)
                                {
                                    currentStatus = ""Out"";
                                    dtLastOut = _punched_at;
                                    att.Out_time = dtLastOut;
                                    att.IWork += Convert.ToInt32((dtLastOut - dtLastIn).TotalMinutes);
                                }
                                else
                                    currentStatus = ""Ignored"";
                            }
                            else if (lastKnownStatus == ""Out"")
                            {
                                if ((_punched_at - dtLastOut).TotalMinutes > 5)
                                {
                                    currentStatus = ""In"";
                                    dtLastIn = _punched_at;
                                    att.IBreak += Convert.ToInt32((dtLastIn - dtLastOut).TotalMinutes);
                                }
                                else
                                    currentStatus = ""Ignored"";
                            }
                            else if (lastKnownStatus == ""Ignored"")
                            {
                                bool bDoneAnything = false;
                                if (dtLastOut > dtLastIn && (_punched_at - dtLastOut).TotalMinutes > 5)
                                {
                                    currentStatus = ""In"";
                                    dtLastIn = _punched_at;
                                    att.IBreak += Convert.ToInt32((dtLastIn - dtLastOut).TotalMinutes);
                                    bDoneAnything = true;
                                }

                                if (dtLastIn > dtLastOut && (_punched_at - dtLastIn).TotalMinutes > 5)
                                {
                                    currentStatus = ""Out"";
                                    dtLastOut = _punched_at;
                                    att.Out_time = dtLastOut;
                                    att.IWork += Convert.ToInt32((dtLastOut - dtLastIn).TotalMinutes);
                                    bDoneAnything = true;
                                }

                                if (!bDoneAnything)
                                    currentStatus = ""Ignored"";
                            }
                        }
                        _row_devattlogs[""inout""] = currentStatus;

                        //FillInOutString
                        if (currentStatus == ""In"")
                            row[""inout_s""] = ""IN"";
                        else if (currentStatus == ""Out"")
                            row[""inout_s""] = ""OUT"";
                        else if (currentStatus == ""Ignored"")
                            row[""inout_s""] = ""Ignored"";
                        else if (currentStatus == ""Excluded"")
                            row[""inout_s""] = ""Excluded"";
                        else if (currentStatus == ""Error"")
                            row[""inout_s""] = ""ERROR"";

                        if (row[""machineno""] != DBNull.Value)
                            row[""type""] = ""Device"";
                        else
                            row[""type""] = ""Manual"";

                        lastKnownStatus = currentStatus;
                        if (currentStatus == ""In"" || currentStatus == ""Out"")
                            lastKnownInOutStatus = currentStatus;
                    }
                    iPos++;
                }
                if (att.In_time != DateTime.MinValue && att.Out_time != DateTime.MinValue)
                {
                    //this.MarkPresent(att.Empmaster_id, cell, null);
                    //this.Save(devattlogs, att, dateInQuestion, break_time, bonus_ot);
                }
                //  else
                // this.MarkError(cell, att.Empmaster_id, dateInQuestion, devattlogs.Rows.Count, Convert.ToInt32(att.IWork / 60), devattlogs, string.Empty);
                // this.SetWorkBreakOT(dt_devattlogs, true, att);
            }
            else
            {
                //var DateTime_Now = CacheHelper.Get<DateTime>(CacheKeys.SYSVARS_NOW_LOCALE);
                //if (((DateTime)cell.OwningColumn.Tag).Date == DateTime_Now.Date)
                //    this.MarkUnReviewed(cell, empmaster_id, dateInQuestion, devattlogs.Rows.Count, devattlogs);
                //else
                //    this.MarkAbsent(cell, empmaster_id, dateInQuestion);
            }

Console.Write(JsonConvert.SerializeObject(att));
            Job.SetParam(""in_time"",att.In_time.ToString(""yyyy-MM-dd HH:mm""));
            Job.SetParam(""out_time"",att.Out_time.ToString(""yyyy-MM-dd HH:mm""));
            Job.SetParam(""duration"",att.IWork);
            Job.SetParam(""break_time"",att.IBreak);
            Job.SetParam(""ot_time"",att.IOverTime);
            Job.SetParam(""ot_time_approved"",(att.IOTHours * 60) + att.IOTMinutes);
            Job.SetParam(""notes"",(att.Notes == """" || att.Notes == null)? ""_"" : att.Notes);
            Job.SetParam(""night_shift"",att.IsNightshift);
            Job.SetParam(""att_date"",dateInQuestion.ToString(""yyyy-MM-dd HH:mm""));
            ",
                                            Lang = ScriptingLanguage.CSharp
                                        },
                                        RouteIndex=1,
                                    },
                                    new EbSqlJobWriter
                                    {
                                        Reference="ebdbllz23nkqd620180220120030-ebdbllz23nkqd620180220120030-4-2622-3474-2622-3474",
                                        RouteIndex=2,
                                    }
                                }
                        },
                    }
                }
            }
        };

        public Dictionary<string, TV> Proc(List<Param> plist)
        {
            Dictionary<string, TV> _fdict = new Dictionary<string, TV>();
            if (plist != null)
                foreach (Param p in plist)
                {
                    _fdict.Add(p.Name, new TV { Value = p.Value, Type = p.Type });
                }
            return _fdict;
        }


        public SqlJobResponse Any(SqlJobRequest request)
        {
            try
            {                
                UserId = request.UserId;
                string query = @" INSERT INTO eb_joblogs_master(refid, type, createdby, created_at) VALUES(:refid, :type, :createdby, NOW()) returning id;";
                DbParameter[] parameters = new DbParameter[] {
                    this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.String, this.SqlJob.RefId) ,
                    this.EbConnectionFactory.DataDB.GetNewParameter("type",EbDbTypes.Int32,this.SqlJob.Type),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby",EbDbTypes.Int32, UserId)
                };
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query, parameters);
                LogMasterId = Convert.ToInt32(dt.Rows[0][0]);
                try
                {
                    GlobalParams = Proc(request.GlobalParams);
                    int step = 0;
                    while (step < this.SqlJob.Resources.Count)
                    {
                        this.SqlJob.Resources[step].Result = GetResult(this.SqlJob.Resources[step], step, 0, 0);
                        step++;
                    }
                    this.EbConnectionFactory.DataDB.DoNonQuery(string.Format("UPDATE eb_joblogs_master SET status = 'S' WHERE id = {0};", LogMasterId));
                }
                catch (Exception e)
                {
                    DbParameter[] dbparameters = new DbParameter[] {
                    this.EbConnectionFactory.DataDB.GetNewParameter("message",EbDbTypes.String, e.Message + e.StackTrace),
                    this.EbConnectionFactory.DataDB.GetNewParameter("id",EbDbTypes.Int32,LogMasterId)
                    };
                    this.EbConnectionFactory.DataDB.DoNonQuery("UPDATE eb_joblogs_master SET status = 'FAILED', message = :message WHERE id = :id;", dbparameters);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return JobResponse;
        }

        public SqlJobsListGetResponse Get(SqlJobsListGetRequest request)
        {

            SqlJobsListGetResponse resp = new SqlJobsListGetResponse();
            try
            {

                string query = $"select logmaster_id , COALESCE (message, 'ffff') message,createdby,createdat," +
                    $"COALESCE(status, 'F') status,id, keyvalues from eb_joblogs_lines where logmaster_id =" +
                    $"(select id from eb_joblogs_master where to_char(created_at, 'dd-mm-yyyy') = '{request.Date}' and refid = '{request.Refid}'  limit 1) " +
                    $"and id not in (select retry_of from eb_joblogs_lines) order by status,id; ";
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
                int capacity1 = dt.Columns.Count - 1;

                RowColletion rc = new RowColletion();
                EbDataTable dtNew = new EbDataTable();
                Dictionary<string, TV> _columnTypeCollection = null;
                if (dt.Rows[0] != null)
                {
                    _columnTypeCollection = JsonConvert.DeserializeObject<Dictionary<string, TV>>(dt.Rows[0]["keyvalues"].ToString());
                }

                foreach (string DataCol in this.SqlJob.FirstReaderKeyColumns)
                {
                    EbDbTypes _type = (_columnTypeCollection.ContainsKey(DataCol)) ? (EbDbTypes)(Convert.ToInt32(_columnTypeCollection[DataCol].Type)) : EbDbTypes.String;
                    dt.Columns.Add(new EbDataColumn(++capacity1, DataCol, _type));
                }

                foreach (string DataCol in this.SqlJob.ParameterKeyColumns)
                {
                    EbDbTypes _type = (_columnTypeCollection.ContainsKey(DataCol)) ? (EbDbTypes)(Convert.ToInt32(_columnTypeCollection[DataCol].Type)) : EbDbTypes.String;
                    dt.Columns.Add(new EbDataColumn(++capacity1, DataCol, _type));
                }

                dtNew.Columns = dt.Columns;
                int i = 0;
                foreach (EbDataRow dr in dt.Rows)
                {

                    int Col = dt.Columns["keyvalues"].ColumnIndex;
                    dtNew.Rows.Add(dr);
                    if (dr["keyvalues"].ToString() != "")
                    {
                        Dictionary<string, TV> _list = JsonConvert.DeserializeObject<Dictionary<string, TV>>(dr["keyvalues"].ToString());

                        foreach (KeyValuePair<string, TV> _c in _list)
                        {
                            Param obj = new Param();
                            obj.Value = _c.Value.Value.ToString();
                            obj.Type = _c.Value.Type;
                            dtNew.Rows[i][++Col] = obj.ValueTo;
                        }

                    }
                    i++;
                }


                resp.SqlJobsColumns = dtNew.Columns;
                resp.SqlJobsRows = dtNew.Rows;
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: SqlFetch Exception: " + e.Message);
            }
            return resp;
        }

        public RetryJobResponse post(RetryJobRequest request)
        {
            RetryJobResponse response = new RetryJobResponse();
            response.Status = false;
            IsRetry = true;
            LogLine logline = GetLogLine(request.JoblogId);
            this.GlobalParams = logline.Params;
            LoopLocation loopLocation = this.SqlJob.GetLoop();
            try
            {
                LoopExecution(loopLocation.Loop, request.JoblogId, loopLocation.Step, loopLocation.ParentIndex, null, logline.Keyvalues);
                response.Status = true;
            }
            catch(Exception e)
            {
                response.Status = false;
                Console.WriteLine("exception" + e);
            }
            return response;
        }

        public ProcessorResponse Post(ProcessorRequest request)
        {
            DateTime dateInQuestion = new DateTime(2015, 2, 28);
            int empmaster_id = 85;
            DateTime dtFirstIn = dateInQuestion;
            DateTime dtLastIn = dateInQuestion;
            DateTime dtLastOut = dateInQuestion;
            string lastKnownStatus = "UnKnown";
            string lastKnownInOutStatus = "UnKnown";
            string currentStatus = "UnKnown";
            int iPos = 0;
            string status = "In";
            var att = new Attendance(empmaster_id);
            //string sql = @"SELECT 
            //            id, 
            //            machineno, 
            //            punched_at, 
            //            inout, 
            //            CASE WHEN inout = null THEN '' WHEN inout = 0 THEN 'OUT' WHEN inout = 1 THEN 'IN' WHEN inout = 2 THEN 'Ignored'

            //            WHEN inout = 3 THEN 'Excluded' WHEN inout = 4 THEN 'ERROR' END AS inout_s, 
            //            CASE WHEN machineno IS NULL THEN 'Manual' WHEN machineno IS NOT NULL THEN 'Device' END AS type, 
            //            sys_ignored
            //        FROM
            //            app_att_deviceattlogs
            //        WHERE
            //            userid = 85 AND
            //              (punched_at::date = '2015-02-28' OR punched_at::date = '2015-03-01') AND
            //                  (COALESCE(app_att_inout_id, 0) <= 0 OR app_att_inout_id = (SELECT id FROM app_att_inout WHERE empmaster_id = 85

            //                                                                           AND att_date = '2015-02-28'))
            //        ORDER BY
            // punched_at ASC";           " +
            string sql = @"SELECT 
                id, 
                machineno, 
                punched_at, 
                inout, 
                CASE WHEN inout = null THEN '' WHEN inout = 0 THEN 'OUT' WHEN inout = 1 THEN 'IN' WHEN inout = 2 THEN 'Ignored'

                WHEN inout = 3 THEN 'Excluded' WHEN inout = 4 THEN 'ERROR' END AS inout_s, 
                CASE WHEN machineno IS NULL THEN 'Manual' WHEN machineno IS NOT NULL THEN 'Device' END AS type, 
                sys_ignored
            FROM
                app_att_deviceattlogs
            WHERE
                userid = :id AND
                  (punched_at::date = :date_to_consolidate::date OR punched_at::date = (:date_to_consolidate::date - 1)) AND
                      (COALESCE(app_att_inout_id, 0) <= 0 OR app_att_inout_id = (SELECT id FROM app_att_inout WHERE empmaster_id = :id
      
                                                                                AND att_date = :date_to_consolidate::date limit 1))
            ORDER BY
                punched_at ASC";
            DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter(":date_to_consolidate",EbDbTypes.DateTime,dateInQuestion),
                this.EbConnectionFactory.DataDB.GetNewParameter(":id",EbDbTypes.Int32,empmaster_id)
            };
            EbDataTable dt_devattlogs = this.EbConnectionFactory.DataDB.DoQuery(sql, parameters);
            if (dt_devattlogs.Rows.Count > 1)
            {
                EbDataRow row = dt_devattlogs.Rows[0];
                foreach (EbDataRow _row_devattlogs in dt_devattlogs.Rows)
                {
                    if (iPos >= dt_devattlogs.Rows.IndexOf(row))
                    {
                        var _punched_at = Convert.ToDateTime(_row_devattlogs["punched_at"]);
                        if (iPos == dt_devattlogs.Rows.IndexOf(row))
                        {
                            currentStatus = status;
                            //  if (!att.IsNightshift)
                            dtFirstIn = _punched_at;
                            att.In_time = dtFirstIn;
                            dtLastIn = dtFirstIn;
                        }
                        if (iPos > dt_devattlogs.Rows.IndexOf(row))
                        {
                            if (lastKnownStatus == "In")
                            {
                                if ((_punched_at - dtLastIn).TotalMinutes > 5)
                                {
                                    currentStatus = "Out";
                                    dtLastOut = _punched_at;
                                    att.Out_time = dtLastOut;
                                    att.IWork += Convert.ToInt32((dtLastOut - dtLastIn).TotalMinutes);
                                }
                                else
                                    currentStatus = "Ignored";
                            }
                            else if (lastKnownStatus == "Out")
                            {
                                if ((_punched_at - dtLastOut).TotalMinutes > 5)
                                {
                                    currentStatus = "In";
                                    dtLastIn = _punched_at;
                                    att.IBreak += Convert.ToInt32((dtLastIn - dtLastOut).TotalMinutes);
                                }
                                else
                                    currentStatus = "Ignored";
                            }
                            else if (lastKnownStatus == "Ignored")
                            {
                                bool bDoneAnything = false;
                                if (dtLastOut > dtLastIn && (_punched_at - dtLastOut).TotalMinutes > 5)
                                {
                                    currentStatus = "In";
                                    dtLastIn = _punched_at;
                                    att.IBreak += Convert.ToInt32((dtLastIn - dtLastOut).TotalMinutes);
                                    bDoneAnything = true;
                                }

                                if (dtLastIn > dtLastOut && (_punched_at - dtLastIn).TotalMinutes > 5)
                                {
                                    currentStatus = "Out";
                                    dtLastOut = _punched_at;
                                    att.Out_time = dtLastOut;
                                    att.IWork += Convert.ToInt32((dtLastOut - dtLastIn).TotalMinutes);
                                    bDoneAnything = true;
                                }

                                if (!bDoneAnything)
                                    currentStatus = "Ignored";
                            }
                        }
                        _row_devattlogs["inout"] = currentStatus;

                        //FillInOutString
                        if (currentStatus == "In")
                            row["inout_s"] = "IN";
                        else if (currentStatus == "Out")
                            row["inout_s"] = "OUT";
                        else if (currentStatus == "Ignored")
                            row["inout_s"] = "Ignored";
                        else if (currentStatus == "Excluded")
                            row["inout_s"] = "Excluded";
                        else if (currentStatus == "Error")
                            row["inout_s"] = "ERROR";

                        if (row["machineno"] != DBNull.Value)
                            row["type"] = "Device";
                        else
                            row["type"] = "Manual";

                        lastKnownStatus = currentStatus;
                        if (currentStatus == "In" || currentStatus == "Out")
                            lastKnownInOutStatus = currentStatus;
                    }
                    iPos++;
                }
                if (att.In_time != DateTime.MinValue && att.Out_time != DateTime.MinValue)
                {
                    //this.MarkPresent(att.Empmaster_id, cell, null);
                    //this.Save(devattlogs, att, dateInQuestion, break_time, bonus_ot);
                }
                //  else
                // this.MarkError(cell, att.Empmaster_id, dateInQuestion, devattlogs.Rows.Count, Convert.ToInt32(att.IWork / 60), devattlogs, string.Empty);
                // this.SetWorkBreakOT(dt_devattlogs, true, att);
            }
            else
            {
                //var DateTime_Now = CacheHelper.Get<DateTime>(CacheKeys.SYSVARS_NOW_LOCALE);
                //if (((DateTime)cell.OwningColumn.Tag).Date == DateTime_Now.Date)
                //    this.MarkUnReviewed(cell, empmaster_id, dateInQuestion, devattlogs.Rows.Count, devattlogs);
                //else
                //    this.MarkAbsent(cell, empmaster_id, dateInQuestion);
            }
            string _insert_qry = string.Format("INSERT INTO app_att_inout (empmaster_id, in_time, out_time, duration, break_time, ot_time, ot_time_approved, notes, night_shift, att_date) SELECT {0}, '{1}', '{2}', {3}, {4}, {5}, {6}, '{7}', {8}, '{9}'",
                  att.Empmaster_id, att.In_time.ToString("yyyy-MM-dd HH:mm"), att.Out_time.ToString("yyyy-MM-dd HH:mm"), att.IWork, att.IBreak, att.IOverTime, (att.IOTHours * 60) + att.IOTMinutes, att.Notes, att.IsNightshift, dateInQuestion.ToString("yyyy-MM-dd HH:mm"));
            var p = this.EbConnectionFactory.DataDB.DoNonQuery(_insert_qry);
            return new ProcessorResponse();
        }

        //---------------------------------------------------------------------------------------------------------------------------------------------

        public LogLine GetLogLine(int JoblogId)
        {
            LogLine logline = null;
            string sql = @"SELECT l.* , m.refid 
                        FROM  eb_joblogs_lines l, eb_joblogs_master m
                        WHERE l.id = :id AND
                        m.id = l.logmaster_id";
            DbParameter[] parameters = new DbParameter[] {
            this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, JoblogId) };
            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(sql, parameters);
            if (dt.Rows != null && dt.Rows.Count > 0)
            {
                logline = new LogLine
                {
                    linesid = Convert.ToInt32(dt.Rows[0]["id"]),
                    masterid = Convert.ToInt32(dt.Rows[0]["logmaster_id"]),
                    Message = dt.Rows[0]["message"].ToString(),
                    Params = JsonConvert.DeserializeObject<Dictionary<string, TV>>(dt.Rows[0]["params"].ToString()),
                    Status = dt.Rows[0]["status"].ToString(),
                    Refid = dt.Rows[0]["refid"].ToString(),
                    Keyvalues = JsonConvert.DeserializeObject<Dictionary<string, TV>>(dt.Rows[0]["keyvalues"].ToString()),
                    RetryOf = Convert.ToInt32(dt.Rows[0]["retry_of"]),
                };
            }
            return logline;
        }

        public object GetResult(SqlJobResource resource, int index, int parentindex, int grandparent)
        {
            ResultWrapper res = new ResultWrapper();
            try
            {
                if (resource is EbSqlJobReader)
                    res.Result = this.ExcDataReader(resource as EbSqlJobReader, index);
                else if (resource is EbSqlJobWriter)
                    res.Result = this.ExcDataWriter(resource as EbSqlJobWriter, index);
                else if (resource is EbLoop)
                    res.Result = ExecuteLoop(resource as EbLoop, index, parentindex);
                else if (resource is EbTransaction)
                    res.Result = ExecuteTransaction(resource as EbTransaction, index);
                else if (resource is EbSqlProcessor)
                {
                    SqlJobResource _prev = null;
                    if (this.SqlJob.Resources[index] is EbTransaction)
                        _prev = (index != 0) ? (((this.SqlJob.Resources[index] as EbTransaction).InnerResources[parentindex] as EbLoop).InnerResources[index - 1]) : null;
                    else if (this.SqlJob.Resources[index] is EbLoop)
                        _prev = (index != 0) ? (((this.SqlJob.Resources[index] as EbLoop).InnerResources[parentindex] as EbTransaction).InnerResources[index - 1]) : null;

                    res.Result =  EvaluateProcessor(resource as EbSqlProcessor,_prev, this.GlobalParams);
                }
                return res.Result;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
                throw e;
            }
        }

        private object ExcDataReader(EbSqlJobReader sqlreader, int step_c)
        {
            EbObject ObjectWrapper = null;
            EbDataSet dt = null;
            try
            {
                ObjectWrapper = this.GetObjectByVer(sqlreader.Reference);
                if (ObjectWrapper == null)
                {
                    this.JobResponse.Message.Description = "DataReader not found";
                    Console.WriteLine("DataReader not found");
                }
                List<DbParameter> p = new List<DbParameter>();
                List<Param> InputParams = (ObjectWrapper as EbDataReader).GetParams(this.Redis as RedisClient);
                this.FillParams(InputParams, step_c);//fill parameter value from prev component
                foreach (Param pr in InputParams)
                {
                    p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));
                }
                dt = this.EbConnectionFactory.ObjectsDB.DoQueries((ObjectWrapper as EbDataReader).Sql, p.ToArray());
                if (dt.Tables[0].Rows.Count > 0)
                {
                    Console.WriteLine("kittippoy");
                }
                this.JobResponse.Message.Description = "Execution success";
            }
            catch (Exception e)
            {
                this.JobResponse.Message.Description = string.Format("Error at position {0}, Resource {1} failed to execute. Resource Name = '{2}'", step_c, "DataReader", ObjectWrapper.Name);
                Console.WriteLine(e.Message + e.StackTrace);
                throw e;
            }
            return dt;
        }

        private object ExcDataWriter(EbSqlJobWriter writer, int step)
        {
            EbObject ObjectWrapper = null;
            List<DbParameter> p = new List<DbParameter>();
            try
            {
                ObjectWrapper = this.GetObjectByVer(writer.Reference);
                if (ObjectWrapper == null)
                {
                    Console.WriteLine("DataWriter not found");
                }
                List<Param> InputParams = (ObjectWrapper as EbDataWriter).GetParams(null);

                this.FillParams(InputParams, step);//fill parameter value from prev component

                foreach (Param pr in InputParams)
                {
                    p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));
                }

                int status = this.EbConnectionFactory.ObjectsDB.DoNonQuery((ObjectWrapper as EbDataWriter).Sql, p.ToArray());

                if (status > 0)
                {
                    this.JobResponse.Message.Description = status + "row inserted";
                    return true;
                }
                else
                {
                    this.JobResponse.Message.Description = status + "row inserted";
                    return false;
                }
            }
            catch (Exception e)
            {
                this.JobResponse.Message.Description = string.Format("Error at position {0}, Resource {1} failed to execute. Resource Name = '{2}'", step, " DataWriter", ObjectWrapper.Name);
                Console.WriteLine(e.Message + e.StackTrace);
                throw e;
            }
        }

        public bool ExecuteLoop(EbLoop loop, int step, int parentindex)
        {
            EbDataTable _table = null;
            if (parentindex == 0 && step == 1)
                _table = (this.SqlJob.Resources[step - 1].Result as EbDataSet).Tables[0];
            else
                _table = (this.SqlJob.Resources[parentindex - 1].Result as EbDataSet).Tables[0];

            int _rowcount = _table.Rows.Count;
            for (int i = 0; i < _rowcount; i++)
            {
                try
                {
                    EbDataColumn cl = _table.Columns[0];
                    Param _outparam = new Param
                    {
                        Name = cl.ColumnName,
                        Type = cl.Type.ToString(),
                        Value = _table.Rows[i][cl.ColumnIndex].ToString()
                    };
                    this.SqlJob.Resources[step].Result = _outparam;
                    if (this.GlobalParams.ContainsKey(_outparam.Name))
                        this.GlobalParams[_outparam.Name] = new TV { Value = _outparam.Value };
                    else
                        this.GlobalParams.Add(_outparam.Name, new TV { Value = _outparam.Value });

                    LoopExecution(loop, 0, step, parentindex, _table.Rows[i], null);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message + e.StackTrace);
                }
            }
            return true;
        }

        public void LoopExecution(EbLoop loop, int retryof, int step, int parentindex, EbDataRow dataRow, Dictionary<string, TV> keyvals)
        {
            int linesid = 0;
            try
            {

                string query = @" INSERT INTO eb_joblogs_lines(logmaster_id, params, createdby, createdat, retry_of)
                                    VALUES(:logmaster_id, :params, :createdby, NOW(), :retry_of) returning id;";
                DbParameter[] parameters = new DbParameter[]
                {
                         this.EbConnectionFactory.DataDB.GetNewParameter("logmaster_id", EbDbTypes.Int32, this.LogMasterId) ,
                         this.EbConnectionFactory.DataDB.GetNewParameter("params",EbDbTypes.Json,JsonConvert.SerializeObject(this.GlobalParams)),
                         this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.Int32, UserId),
                         this.EbConnectionFactory.ObjectsDB.GetNewParameter("retry_of", EbDbTypes.Int32, retryof)
                 };
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query, parameters);
                linesid = Convert.ToInt32(dt.Rows[0][0]);
                string _keyvalues = (dataRow is null) ? JsonConvert.SerializeObject(keyvals) : FillKeys(dataRow);
                try
                {
                    for (int counter = 0; counter < loop.InnerResources.Count; counter++)
                    {
                        if (loop.InnerResources[counter] is EbSqlProcessor)
                        {
                            if (loop.InnerResources[counter - 1] is EbSqlJobReader)
                                if (((loop.InnerResources[counter - 1] as EbSqlJobReader).Result as EbDataSet).Tables[0].Rows.Count > 0)
                                    loop.InnerResources[counter].Result = this.GetResult(loop.InnerResources[counter], counter, step, parentindex);
                                else
                                    break;
                        }
                        loop.InnerResources[counter].Result = this.GetResult(loop.InnerResources[counter], counter, step, parentindex);
                    }
                    DbParameter[] e_parameters = new DbParameter[]
                    { this.EbConnectionFactory.DataDB.GetNewParameter("linesid", EbDbTypes.Int32, linesid) ,
                    this.EbConnectionFactory.DataDB.GetNewParameter("keyvalues",EbDbTypes.Json,  _keyvalues)};
                    this.EbConnectionFactory.DataDB.DoNonQuery("UPDATE eb_joblogs_lines SET status = 'S' , keyvalues = :keyvalues WHERE id = :linesid;", e_parameters);
                }
                catch (Exception e)
                {
                    DbParameter[] e_parameters = new DbParameter[]
                    {
                         this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, linesid) ,
                         this.EbConnectionFactory.DataDB.GetNewParameter("message",EbDbTypes.String,  e.Message),
                         this.EbConnectionFactory.DataDB.GetNewParameter("keyvalues",EbDbTypes.Json,  _keyvalues)
                    };
                    this.EbConnectionFactory.DataDB.DoNonQuery(@"UPDATE eb_joblogs_lines SET status = 'F', message = :message, keyvalues = :keyvalues WHERE id = :id ;", e_parameters);

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error at LoopExecution" + dataRow.ToString() + retryof + "- retryof");
                throw e;
            }
        }

        public SqlJobScript EvaluateProcessor(EbSqlProcessor processor,SqlJobResource _prevres, Dictionary<string, TV> GlobalParams)
        {
            string code = processor.Script.Code.Trim();
            EbDataSet _ds = null;
            SqlJobScript script = new SqlJobScript();

            Script valscript = CSharpScript.Create<dynamic>(code,
                ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core")
                .WithImports("System.Dynamic", "System", "System.Collections.Generic", "System.Diagnostics", "System.Linq", "Newtonsoft.Json", "ExpressBase.Common")
                .AddReferences(typeof(ExpressBase.Common.EbDataSet).Assembly),
                globalsType: typeof(SqlJobGlobals));

            if (_prevres != null)
                _ds = _prevres.Result as EbDataSet;
            try
            {
                valscript.Compile();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
                throw e;
            }

            try
            {
                SqlJobGlobals globals = new SqlJobGlobals(_ds, ref GlobalParams);

                foreach (KeyValuePair<string, TV> kp in GlobalParams)
                {
                    globals["Params"].Add(kp.Key, new NTV
                    {
                        Name = kp.Key,
                        Type = (kp.Value.Value.GetType() == typeof(JObject)) ? EbDbTypes.Object : (EbDbTypes)Enum.Parse(typeof(EbDbTypes), kp.Value.Value.GetType().Name, true),
                        Value = kp.Value.Value
                    });
                }

                script.Data = JsonConvert.SerializeObject(valscript.RunAsync(globals).Result.ReturnValue);
            }
            catch (Exception e)
            {
                throw e;
            }
            return script;
        }

        public string FillKeys(EbDataRow dataRow)
        {
            foreach (var item in this.GlobalParams)
                if (GetKeyvalueDict.ContainsKey(item.Key))
                    this.GetKeyvalueDict[item.Key] = GlobalParams[item.Key];
            for (int i = 0; i < dataRow.Count; i++)
            {
                if (GetKeyvalueDict.ContainsKey(dataRow.Table.Columns[i].ColumnName))
                    this.GetKeyvalueDict[dataRow.Table.Columns[i].ColumnName] = new TV { Value = dataRow[i].ToString(), Type = ((int)dataRow.Table.Columns[i].Type).ToString() };
            }
            return JsonConvert.SerializeObject(GetKeyvalueDict); ;
        }

        public bool ExecuteTransaction(EbTransaction txn, int step)
        {
            using (DbConnection con = this.EbConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();

                DbTransaction trans = con.BeginTransaction();
                try
                {
                    for (int counter = 0; counter < txn.InnerResources.Count; counter++)
                        if (txn.InnerResources[counter] is EbSqlProcessor)
                        {
                            if (txn.InnerResources[counter - 1] is EbSqlJobReader)
                                if (((txn.InnerResources[counter - 1] as EbSqlJobReader).Result as EbDataSet).Tables[0].Rows.Count > 0)
                                    txn.InnerResources[counter].Result = this.GetResult(txn.InnerResources[counter], counter, step, 0);
                                else
                                    break;
                        }
                        else
                            txn.InnerResources[counter].Result = this.GetResult(txn.InnerResources[counter], counter, step, 0);
                    trans.Commit();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message + e.StackTrace);
                    trans.Rollback();
                    throw e;
                }
            }
            return true;
        }

        private void FillParams(List<Param> InputParam, int step)
        {
            List<Param> OutParams = new List<Param>();
            if (step != 0)
            {
                if (this.SqlJob.Resources[step - 1].Result != null)
                    OutParams = this.SqlJob.Resources[step - 1].GetOutParams(InputParam, step);
            }
            this.TempParams = OutParams.Select(i => new { prop = i.Name, val = i.ValueTo })
                        .ToDictionary(x => x.prop, x => x.val as object);


            foreach (Param p in InputParam)
            {
                if (this.TempParams != null && this.TempParams.ContainsKey(p.Name))
                    p.Value = this.TempParams[p.Name].ToString();
                else if (this.GlobalParams.ContainsKey(p.Name))
                    p.Value = this.GlobalParams[p.Name].Value.ToString();
                else if (string.IsNullOrEmpty(p.Value))
                {
                    Console.WriteLine(string.Format("Parameter {0} must be set", p.Name));
                }
                else
                {
                    Console.WriteLine(string.Format("Parameter {0} must be set", p.Name));
                }

            }
        }

        private EbObject GetObjectByVer(string refid)
        {
            EbObjectParticularVersionResponse resp = (EbObjectParticularVersionResponse)StudioServices.Get(new EbObjectParticularVersionRequest { RefId = refid });
            return EbSerializers.Json_Deserialize(resp.Data[0].Json);
        }
    }
    
   
    internal class Attendance
    {
        internal int Empmaster_id { get; set; }
        internal DateTime In_time { get; set; }
        internal DateTime Out_time { get; set; }
        internal int IWork { get; set; }
        internal int IBreak { get; set; }
        internal int IOverTime { get; set; }
        internal int IOTHours { get; set; }
        internal int IOTMinutes { get; set; }
        internal string Notes { get; set; }
        internal bool IsNightshift { get; set; }

        internal int App_att_inout_id { get; set; }

        internal Attendance(int empmaster_id)
        {
            this.Empmaster_id = empmaster_id;
        }

        internal Attendance(int empmaster_id, DateTime in_time, DateTime out_time, int iWork, int iBreak, int iOverTime, int iOTHours, int iOTMinutes, string notes, bool bNightshift)
        {
            this.Empmaster_id = empmaster_id;
            this.In_time = in_time;
            this.Out_time = out_time;
            this.IWork = iWork;
            this.IBreak = iBreak;
            this.IOverTime = iOverTime;
            this.IOTHours = iOTHours;
            this.IOTMinutes = iOTMinutes;
            this.Notes = notes;
            this.IsNightshift = bNightshift;
        }
    }
}
