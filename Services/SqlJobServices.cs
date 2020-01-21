using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects;
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
using ExpressBase.Security;

namespace ExpressBase.ServiceStack.Services
{
    public class SqlJobServices : EbBaseService
    {
        public int LogMasterId { get; set; }

        public int UserId { get; set; }

        public string UserAuthId { get; set; }

        public string SolutionId { get; set; }

        public bool IsRetry = false;

        public Dictionary<string, TV> GlobalParams { set; get; }

        public Dictionary<string, object> TempParams { set; get; }


        Script valscript = null;

        SqlJobGlobals Globals = null;
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

        public EbSqlJob SqlJob { get; set; }

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
                EbObjectParticularVersionResponse version = (EbObjectParticularVersionResponse)this.StudioServices.Get(new EbObjectParticularVersionRequest { RefId = request.RefId });
                SqlJob = EbSerializers.Json_Deserialize(version.Data[0].Json);
                UserId = request.UserId;
                UserAuthId = request.UserAuthId;
                SolutionId = request.SolnId;
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
                    this.EbConnectionFactory.DataDB.DoNonQuery("UPDATE eb_joblogs_master SET status = 'F', message = :message WHERE id = :id;", dbparameters);
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
            EbDataTable dtNew = new EbDataTable();
            try
            {
                EbObjectParticularVersionResponse version = (EbObjectParticularVersionResponse)this.StudioServices.Get(new EbObjectParticularVersionRequest { RefId = request.RefId });
                SqlJob = EbSerializers.Json_Deserialize(version.Data[0].Json);
                string query = @"SELECT logmaster_id , COALESCE (message, 'success') message, createdby, createdat,  
                     COALESCE(status, 'F') status,id, keyvalues FROM eb_joblogs_lines WHERE logmaster_id = 
                     (SELECT id FROM eb_joblogs_master WHERE to_char(created_at, 'dd-mm-yyyy') = :date AND refid = :refid  LIMIT 1)  
                     AND id NOT IN (SELECT retry_of FROM eb_joblogs_lines) ORDER BY status, id; ";
                DbParameter[] parameters = new DbParameter[] {
                    this.EbConnectionFactory.DataDB.GetNewParameter(":date", EbDbTypes.String, request.Date ),
                     this.EbConnectionFactory.DataDB.GetNewParameter(":refid", EbDbTypes.String, request.RefId ),
                };
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query, parameters);
                if (dt.Rows.Count > 0)
                {
                    int capacity = 0;
                    Dictionary<string, TV> _columnTypeCollection = null;

                    //Adding Columns in DataTable
                    if (dt.Rows[0] != null)
                    {
                        string _tempkeyvalues = dt.Rows[0]["keyvalues"].ToString();
                        if (_tempkeyvalues == String.Empty)
                            _tempkeyvalues = dt.Rows[dt.Rows.Count - 1]["keyvalues"].ToString();
                        if (_tempkeyvalues != String.Empty)
                        {
                            _columnTypeCollection = JsonConvert.DeserializeObject<Dictionary<string, TV>>(_tempkeyvalues);
                            foreach (string DataCol in this.SqlJob.FirstReaderKeyColumns)
                            {
                                EbDbTypes _type = (_columnTypeCollection.ContainsKey(DataCol)) ? (EbDbTypes)(Convert.ToInt32(_columnTypeCollection[DataCol].Type)) : EbDbTypes.String;
                                dtNew.Columns.Add(new EbDataColumn(capacity++, DataCol, _type));
                            }

                            foreach (string DataCol in this.SqlJob.ParameterKeyColumns)
                            {
                                EbDbTypes _type = (_columnTypeCollection.ContainsKey(DataCol)) ? (EbDbTypes)(Convert.ToInt32(_columnTypeCollection[DataCol].Type)) : EbDbTypes.String;
                                dtNew.Columns.Add(new EbDataColumn(capacity++, DataCol, _type));
                            }
                        }
                    }
                    int customColumnCount = dtNew.Columns.Count;
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        dtNew.Columns.Add(new EbDataColumn(capacity++, dt.Columns[i].ColumnName, dt.Columns[i].Type));
                    }
                    int _rowCount = 0;
                    foreach (EbDataRow dr in dt.Rows)
                    {
                        dtNew.Rows.Add(dtNew.NewDataRow2());
                        if (dr["keyvalues"].ToString() != "")
                        {
                            Dictionary<string, TV> _list = JsonConvert.DeserializeObject<Dictionary<string, TV>>(dr["keyvalues"].ToString());
                            int _columnCount = 0;
                            foreach (KeyValuePair<string, TV> _c in _list)
                            {
                                Param obj = new Param();
                                obj.Value = _c.Value.Value.ToString();
                                obj.Type = _c.Value.Type;

                                dtNew.Rows[_rowCount][_columnCount++] = obj.ValueTo;
                            }
                        }
                        for (int i = 0; i < dr.Count; i++)
                        {
                            dtNew.Rows[_rowCount][i + customColumnCount] = dt.Rows[_rowCount][i];
                        }
                        _rowCount++;
                    }
                }
                resp.SqlJobsColumns = dtNew.Columns;
                resp.SqlJobsRows = dtNew.Rows;
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: SqlFetch Exception: " + e.Message + e.StackTrace);
            }
            return resp;
        }

        public RetryJobResponse Post(RetryJobRequest request)
        {
            RetryJobResponse response = new RetryJobResponse();
            response.Status = false;
            IsRetry = true;
            LogLine logline = GetLogLine(request.JoblogId);
            this.LogMasterId = logline.linesid;
            this.GlobalParams = logline.Params;
            EbObjectParticularVersionResponse version = (EbObjectParticularVersionResponse)this.StudioServices.Get(new EbObjectParticularVersionRequest { RefId = request.RefId });
            SqlJob = EbSerializers.Json_Deserialize(version.Data[0].Json);
            LoopLocation loopLocation = SqlJob.GetLoop();
            try
            {
                LoopExecution(loopLocation.Loop, request.JoblogId, loopLocation.Step, loopLocation.ParentIndex, null, logline.Keyvalues);
                response.Status = true;
            }
            catch (Exception e)
            {
                response.Status = false;
                Console.WriteLine("exception" + e);
            }
            return response;
        }

        public ProcessorResponse Post(ProcessorRequest request)
        {

            int empmaster_id = 85;
            Attendance att = new Attendance(empmaster_id);
            att.DateInQuestion = new DateTime(2015, 2, 28);
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
                punched_at::date = :date_to_consolidate::date AND
                (COALESCE(app_att_inout_id, 0) <= 0 OR app_att_inout_id = 
                (SELECT id FROM app_att_inout WHERE empmaster_id = :id AND att_date = :date_to_consolidate::date limit 1))
            ORDER BY
                punched_at ASC;
                
     SELECT
        C.empmaster_id, 
        A.shift_xid, 
        A.description, 
        A.in_time, 
        A.out_time, 
        A.break_time, 
        A.bonus_ot, 
        B.start_date, 
        B.end_date 
    FROM 
        app_att_shifts A, 
        app_att_shift_schedules B, 
        app_att_shift_schedules_lines C 
    WHERE 
        A.id=B.shift_id AND 
        B.id=C.shift_schedule_id AND 
        C.empmaster_id = :id;";
            DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter(":date_to_consolidate",EbDbTypes.DateTime,att.DateInQuestion),
                this.EbConnectionFactory.DataDB.GetNewParameter(":id",EbDbTypes.Int32,empmaster_id)
            };



            EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);
            EbDataTable dt_devattlogs = dt.Tables[0];
            EbDataRow _row_empmaster = dt.Tables[1].Rows[0];
            var in_time_temp = Convert.ToDateTime(_row_empmaster["in_time"]);
            var break_time_temp = (_row_empmaster["break_time"] != DBNull.Value) ? Convert.ToInt32(_row_empmaster["break_time"]) : 0;
            var bonus_ot_temp = (_row_empmaster["bonus_ot"] != DBNull.Value) ? Convert.ToInt32(_row_empmaster["bonus_ot"]) : 0;
            if (in_time_temp.Hour > 14)
                att.DoProcessNightShift(break_time_temp, bonus_ot_temp, dt_devattlogs);
            else
                att.DoProcessDayShift(break_time_temp, bonus_ot_temp, dt_devattlogs);

            string _insert_qry = string.Format("INSERT INTO app_att_inout (empmaster_id, in_time, out_time, duration, break_time, ot_time, ot_time_approved, notes, night_shift, att_date) SELECT {0}, '{1}', '{2}', {3}, {4}, {5}, {6}, '{7}', {8}, '{9}'",
                  att.Empmaster_id, att.In_time.ToString("yyyy-MM-dd HH:mm"), att.Out_time.ToString("yyyy-MM-dd HH:mm"), att.IWork, att.IBreak, att.IOverTime, (att.IOTHours * 60) + att.IOTMinutes, att.Notes, att.IsNightshift, att.DateInQuestion.ToString("yyyy-MM-dd HH:mm"));
            var p = this.EbConnectionFactory.DataDB.DoNonQuery(_insert_qry);
            return new ProcessorResponse();
        }

        //---------------------------------------------------------------------------------------------------------------------------------------------


        public LogLine GetLogLine(int JoblogId)
        {
            LogLine logline = null;
            string sql = @" SELECT
                                 l.* , m.refid 
                             FROM 
                                 eb_joblogs_lines l, eb_joblogs_master m
                             WHERE
                                 l.id = :id AND m.id = l.logmaster_id";
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

                else if (resource is EbSqlFormDataPusher)
                    res.Result = ExecuteDataPush(resource as EbSqlFormDataPusher, index);

                else if (resource is EbSqlProcessor)
                {
                    SqlJobResource _prev = null;
                    if (this.SqlJob.Resources[index] is EbTransaction)
                        _prev = (index != 0) ? (((this.SqlJob.Resources[index] as EbTransaction).InnerResources[parentindex] as EbLoop).InnerResources[index - 1]) : null;
                    else if (this.SqlJob.Resources[index] is EbLoop)
                        _prev = (index != 0) ? (((this.SqlJob.Resources[index] as EbLoop).InnerResources[parentindex] as EbTransaction).InnerResources[index - 1]) : null;

                    res.Result = EvaluateProcessor(resource as EbSqlProcessor, _prev, this.GlobalParams);
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
                List<Param> InputParams = (ObjectWrapper as EbDataReader).GetParams(this.Redis as RedisClient, this);
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
                        this.GlobalParams[_outparam.Name] = new TV { Type = _outparam.Type, Value = _outparam.Value };
                    else
                        this.GlobalParams.Add(_outparam.Name, new TV { Type = _outparam.Type, Value = _outparam.Value });

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

        public SqlJobScript EvaluateProcessor(EbSqlProcessor processor, SqlJobResource _prevres, Dictionary<string, TV> GlobalParams)
        {
            string code = processor.Script.Code.Trim();
            EbDataSet _ds = null;
            SqlJobScript script = new SqlJobScript();

            if (_prevres != null)
                _ds = _prevres.Result as EbDataSet;
            try
            {
                if (valscript == null) // 
                {
                    valscript = CSharpScript.Create<dynamic>(code,
                         ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core")
                         .WithImports("System.Dynamic", "System", "System.Collections.Generic", "System.Diagnostics", "System.Linq", "Newtonsoft.Json", "ExpressBase.Common")
                         .AddReferences(typeof(ExpressBase.Common.EbDataSet).Assembly),
                         globalsType: typeof(SqlJobGlobals));
                    valscript.Compile();
                }
            }
            catch (Exception e)
            {
                valscript = null;
                Console.WriteLine(e.Message + e.StackTrace);
                throw e;
            }

            try
            {
                Globals = new SqlJobGlobals(_ds, ref GlobalParams);

                foreach (KeyValuePair<string, TV> kp in GlobalParams)
                {
                    Globals["Params"].Add(kp.Key, new NTV
                    {
                        Name = kp.Key,
                        Type = (kp.Value.Value.GetType() == typeof(JObject)) ? EbDbTypes.Object : (EbDbTypes)Enum.Parse(typeof(EbDbTypes), kp.Value.Value.GetType().Name, true),
                        Value = kp.Value.Value
                    });
                }

                script.Data = JsonConvert.SerializeObject(valscript.RunAsync(Globals).Result.ReturnValue);
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

        private object ExecuteDataPush(EbSqlFormDataPusher dataPusher, int step)
        {
            WebFormServices webFormServices = base.ResolveService<WebFormServices>();
            webFormServices.Any(new InsertOrUpdateFormDataRqst
            {
                RefId = dataPusher.Pusher,
                PushJson = dataPusher.PushJson,
                UserId = UserId,
                UserAuthId = UserAuthId,
                RecordId = 0,
                UserObj = this.Redis.Get<User>(UserAuthId),
                LocId = -1,
                SolnId = SolutionId,
                WhichConsole = "uc",
                FormGlobals = new FormGlobals { Params = Globals.Params }
            });
            return new object();
        }
    }

    internal class InOutFlatCollection : List<InOutFlat>
    {
        internal int WorkTime
        {
            get
            {
                int _work = 0;

                foreach (InOutFlat _obj in this)
                    _work += _obj.WorkTime;

                return _work;
            }
        }

        internal int BreakTime
        {
            get
            {
                int _break = 0;

                foreach (InOutFlat _obj in this)
                    _break += _obj.BreakTime;

                return _break;
            }
        }
    }

    internal class InOutFlat
    {
        private InOutFlatCollection InOutFlatCollection { get; set; }
        private bool InSet { get; set; }
        private bool OutSet { get; set; }

        internal InOutFlat(InOutFlatCollection _coll)
        {
            InOutFlatCollection = _coll;
        }

        private DateTime _in;
        internal DateTime In
        {
            get { return _in; }
            set
            {
                if (!InSet)
                {
                    _in = value;
                    InSet = true;
                }
            }
        }

        private DateTime _out;
        internal DateTime Out
        {
            get { return _out; }
            set
            {
                if (!OutSet)
                {
                    _out = value;
                    OutSet = true;
                }
            }
        }

        internal int WorkTime
        {
            get
            {
                if (InSet && OutSet)
                    return Convert.ToInt32((Out - In).TotalMinutes);
                else
                    return 0;
            }
        }

        internal int BreakTime
        {
            get
            {
                int myPos = InOutFlatCollection.IndexOf(this);
                if (InOutFlatCollection.Count > myPos + 1 && InOutFlatCollection[myPos + 1].InSet)
                    return Convert.ToInt32((InOutFlatCollection[myPos + 1].In - this.Out).TotalMinutes);
                else
                    return 0;
            }
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
        internal DateTime DateInQuestion { get; set; }

        internal int IWorkTimeInMinutes = 540;
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

        public void DoProcessNightShift(int break_time, int bonus_ot, EbDataTable dt_devattlogs)
        {

        }
        public void DoProcessDayShift(int break_time, int bonus_ot, EbDataTable dt_devattlogs)
        {
            InOutStatus lastKnownStatus = InOutStatus.UnKnown;
            InOutStatus lastKnownInOutStatus = InOutStatus.UnKnown;
            InOutStatus currentStatus = InOutStatus.UnKnown;
            DateTime dtFirstIn = this.DateInQuestion;
            DateTime dtLastIn = this.DateInQuestion;
            DateTime dtLastOut = this.DateInQuestion;
            int iPos = 0;
            InOutStatus status = InOutStatus.In;
            if (dt_devattlogs.Rows.Count > 1)
            {
                EbDataRow row = dt_devattlogs.Rows[0];
                foreach (EbDataRow _row_devattlogs in dt_devattlogs.Rows)
                {
                    if (iPos >= dt_devattlogs.Rows.IndexOf(row))
                    {
                        DateTime _punched_at = Convert.ToDateTime(_row_devattlogs["punched_at"]);

                        if (iPos == dt_devattlogs.Rows.IndexOf(row))
                        {
                            currentStatus = status;
                            if (!this.IsNightshift)
                            {
                                dtFirstIn = _punched_at;
                                this.In_time = dtFirstIn;
                                dtLastIn = dtFirstIn;
                            }
                        }
                        if (iPos > dt_devattlogs.Rows.IndexOf(row))
                        {
                            if (lastKnownStatus == InOutStatus.In)
                            {
                                if ((_punched_at - dtLastIn).TotalMinutes > 5)
                                {
                                    currentStatus = InOutStatus.UnKnown;
                                    dtLastOut = _punched_at;
                                    this.Out_time = dtLastOut;
                                    this.IWork += Convert.ToInt32((dtLastOut - dtLastIn).TotalMinutes);
                                }
                                else
                                    currentStatus = InOutStatus.Ignored;
                            }
                            else if (lastKnownStatus == InOutStatus.Out)
                            {
                                if ((_punched_at - dtLastOut).TotalMinutes > 5)
                                {
                                    currentStatus = InOutStatus.In;
                                    dtLastIn = _punched_at;
                                    this.IBreak += Convert.ToInt32((dtLastIn - dtLastOut).TotalMinutes);
                                }
                                else
                                    currentStatus = InOutStatus.Ignored;
                            }
                            else if (lastKnownStatus == InOutStatus.Ignored)
                            {
                                bool bDoneAnything = false;
                                if (dtLastOut > dtLastIn && (_punched_at - dtLastOut).TotalMinutes > 5)
                                {
                                    currentStatus = InOutStatus.In;
                                    dtLastIn = _punched_at;
                                    this.IBreak += Convert.ToInt32((dtLastIn - dtLastOut).TotalMinutes);
                                    bDoneAnything = true;
                                }

                                if (dtLastIn > dtLastOut && (_punched_at - dtLastIn).TotalMinutes > 5)
                                {
                                    currentStatus = InOutStatus.Out;
                                    dtLastOut = _punched_at;
                                    this.Out_time = dtLastOut;
                                    this.IWork += Convert.ToInt32((dtLastOut - dtLastIn).TotalMinutes);
                                    bDoneAnything = true;
                                }

                                if (!bDoneAnything)
                                    currentStatus = InOutStatus.Ignored;
                            }
                        }
                        _row_devattlogs["inout"] = currentStatus;

                        this.FillInOutString(_row_devattlogs, currentStatus);
                        lastKnownStatus = currentStatus;
                        if (currentStatus == InOutStatus.In || currentStatus == InOutStatus.Out)
                            lastKnownInOutStatus = currentStatus;
                    }
                    iPos++;
                }
                if (this.In_time != DateTime.MinValue && this.Out_time != DateTime.MinValue)
                {
                    this.Save(dt_devattlogs, break_time, bonus_ot);
                }
                else
                    //  this.MarkError(cell, att.Empmaster_id, dateInQuestion, devattlogs.Rows.Count, Convert.ToInt32(att.IWork / 60), devattlogs, string.Empty);
                    this.SetWorkBreakOT(dt_devattlogs, true);
            }
            else
            {
                //    var DateTime_Now = CacheHelper.Get<DateTime>(CacheKeys.SYSVARS_NOW_LOCALE);
                //    if (((DateTime)cell.OwningColumn.Tag).Date == DateTime_Now.Date)
                //        this.MarkUnReviewed(cell, empmaster_id, dateInQuestion, devattlogs.Rows.Count, devattlogs);
                //    else
                //        this.MarkAbsent(cell, empmaster_id, dateInQuestion);
            }
        }
        internal void MarkRowForDayShift(EbDataTable dt_devattlogs, EbDataRow row, InOutStatus status)
        { }
        public void FillInOutString(EbDataRow row, InOutStatus currentStatus)
        {
            if (currentStatus == InOutStatus.In)
                row["inout_s"] = "IN";
            else if (currentStatus == InOutStatus.Out)
                row["inout_s"] = "OUT";
            else if (currentStatus == InOutStatus.Ignored)
                row["inout_s"] = "Ignored";
            else if (currentStatus == InOutStatus.Excluded)
                row["inout_s"] = "Excluded";
            else if (currentStatus == InOutStatus.Error)
                row["inout_s"] = "ERROR";

            if (row["machineno"] != DBNull.Value)
                row["type"] = "Device";
            else
                row["type"] = "Manual";
        }
        private void SetWorkBreakOT(EbDataTable dt_devattlogs, bool recalc)
        {
            if (recalc)
            {
                ReCalcWorkBreak(dt_devattlogs);
                this.IOverTime = (this.IWork > IWorkTimeInMinutes) ? this.IWork - IWorkTimeInMinutes : 0;
                this.IOTHours = Convert.ToInt32(this.IOverTime / 60);
                this.IOTMinutes = this.IOverTime % 60;
            }
        }
        private void ReCalcWorkBreak(EbDataTable dt_devattlogs)
        {
            InOutFlatCollection _coll = new InOutFlatCollection();
            foreach (EbDataRow _row_devattlogs in dt_devattlogs.Rows)
            {
                var status = (InOutStatus)Convert.ToInt32(_row_devattlogs["inout"]);
                if (status == InOutStatus.In)
                {
                    var inoutobj = new InOutFlat(_coll);
                    inoutobj.In = Convert.ToDateTime(_row_devattlogs["punched_at"]);
                    _coll.Add(inoutobj);
                }
                else if (status == InOutStatus.Out)
                {
                    if (_coll.Count > 0)
                        _coll[_coll.Count - 1].Out = Convert.ToDateTime(_row_devattlogs["punched_at"]);
                }
            }

            this.IWork = _coll.WorkTime;
            this.IBreak = _coll.BreakTime;
        }
        internal void Save(EbDataTable dt_devattlogs, int break_time, int bonus_ot)
        {
            if (this.In_time != DateTime.MinValue && this.Out_time != DateTime.MinValue)
            {
                if ((this.IWork / 60) > 6)
                {
                    if (bonus_ot > 0)
                    {
                        this.IOTHours += Convert.ToInt32(bonus_ot / 60);
                        this.IOTMinutes += bonus_ot % 60;
                    }

                    if (break_time > 0)
                        this.IBreak += break_time;
                }

                string _update_qry = string.Format("UPDATE app_att_inout SET in_time='{0}', out_time='{1}', duration={2}, break_time={3}, ot_time={4}, ot_time_approved={5}, notes='{6}', night_shift={7}, absent_type=null WHERE empmaster_id={8} AND att_date='{9}'",
                    this.In_time.ToString("yyyy-MM-dd HH:mm"), this.Out_time.ToString("yyyy-MM-dd HH:mm"), this.IWork, this.IBreak, this.IOverTime, (this.IOTHours * 60) + this.IOTMinutes, this.Notes, this.IsNightshift, this.Empmaster_id, DateInQuestion.ToString("yyyy-MM-dd HH:mm"));

                string _insert_qry = string.Format("INSERT INTO app_att_inout (empmaster_id, in_time, out_time, duration, break_time, ot_time, ot_time_approved, notes, night_shift, att_date) SELECT {0}, '{1}', '{2}', {3}, {4}, {5}, {6}, '{7}', {8}, '{9}'",
                    this.Empmaster_id, this.In_time.ToString("yyyy-MM-dd HH:mm"), this.Out_time.ToString("yyyy-MM-dd HH:mm"), this.IWork, this.IBreak, this.IOverTime, (this.IOTHours * 60) + this.IOTMinutes, this.Notes, this.IsNightshift, DateInQuestion.ToString("yyyy-MM-dd HH:mm"));

                string _sql = string.Format("WITH upsert AS ({0} RETURNING *) {1} WHERE NOT EXISTS (SELECT * FROM upsert)", _update_qry, _insert_qry);

                foreach (EbDataRow _row_devattlogs in dt_devattlogs.Rows)
                {
                    if (Convert.ToInt32(_row_devattlogs["inout"]) != (int)InOutStatus.Excluded)
                        _sql += string.Format("UPDATE app_att_deviceattlogs SET inout={0}, app_att_inout_id=(SELECT id FROM app_att_inout WHERE empmaster_id={1} AND att_date='{2}') WHERE id={3};",
                            _row_devattlogs["inout"], this.Empmaster_id, DateInQuestion.ToString("yyyy-MM-dd HH:mm"), _row_devattlogs["id"]);
                    else
                        _sql += string.Format("UPDATE app_att_deviceattlogs SET inout={0}, app_att_inout_id=null WHERE id={1};", _row_devattlogs["inout"], _row_devattlogs["id"]);

                }

                //DBHelper.Instance.ExecuteNonQuery(WhichDatabase.CONFIG, _sql);
            }
        }
    }
}
