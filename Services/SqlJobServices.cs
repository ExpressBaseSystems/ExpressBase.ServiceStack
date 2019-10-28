using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects;
using ExpressBase.Objects.Objects.SqlJobRelated;
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
using static ExpressBase.Objects.Objects.SqlJobRelated.EbSqlJob;

namespace ExpressBase.ServiceStack.Services
{
    public class SqlJobServices : EbBaseService
    {

        public int LogMasterId { get; set; }

        public int UserId { get; set; }

        public bool IsRetry = false;

        public Dictionary<string, object> GlobalParams { set; get; }

        public Dictionary<string, object> TempParams { set; get; }

        private SqlJobResponse JobResponse { get; set; }

        private EbObjectService StudioServices { set; get; }

        public SqlJobServices(IEbConnectionFactory _dbf) : base(_dbf)
        {
            this.StudioServices = base.ResolveService<EbObjectService>();
            this.JobResponse = new SqlJobResponse();
        }
        private Dictionary<string, string> _keyValuePairs = null;

        public Dictionary<string, string> GetKeyvalueDict
        {
            get
            {
                if (_keyValuePairs == null)
                {
                    _keyValuePairs = new Dictionary<string, string>();
                    foreach (string key in this.SqlJob.FirstReaderKeyColumns)
                    {
                        if (!_keyValuePairs.ContainsKey(key))
                            _keyValuePairs.Add(key, "");
                    }
                    foreach (string key in this.SqlJob.ParameterKeyColumns)
                    {
                        if (!_keyValuePairs.ContainsKey(key))
                            _keyValuePairs.Add(key, "");
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
                    Reference="ebdbllz23nkqd620180220120030-ebdbllz23nkqd620180220120030-2-2601-3455-2601-3455",
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
                                        Reference="ebdbllz23nkqd620180220120030-ebdbllz23nkqd620180220120030-2-2603-3445-2603-3445",
                                        RouteIndex=0
                                    },
                                    new EbSqlProcessor
                                    {
                                        Script = new EbScript
                                        {
                                            Code="Job.SetParam(\"val\",(Tables[0].Rows.Count>0)?Convert.ToInt32(Tables[0].Rows[0][0]):0); return 100;",
                                            Lang = ScriptingLanguage.CSharp
                                        },
                                        RouteIndex=1,
                                    },
                                    new EbSqlJobWriter
                                    {
                                        Reference="ebdbllz23nkqd620180220120030-ebdbllz23nkqd620180220120030-4-2613-3461-2613-3461",
                                        RouteIndex=2,
                                    }
                                }
                        },
                    }
                }
            }
        };

        public Dictionary<string, object> Proc(List<Param> plist)
        {
            Dictionary<string, object> _fdict = new Dictionary<string, object>();
            if (plist != null)
                foreach (Param p in plist)
                {
                    _fdict.Add(p.Name, p.Value);
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
                    this.EbConnectionFactory.DataDB.DoNonQuery(string.Format("UPDATE eb_joblogs_master SET status = 'SUCCESS' WHERE id = {0};", LogMasterId));
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
               
                string query =  $"select logmaster_id , params,COALESCE (message, 'ffff') message,createdby,createdat," +
                    $"COALESCE(status, 'FAILED') status,id from eb_joblogs_lines where logmaster_id =" +
                    $"(select id from eb_joblogs_master where to_char(created_at, 'dd-mm-yyyy') = '{request.Date}' and refid = '{request.Refid}' limit 1) order by status,id; ";
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
                int capacity1 = dt.Rows.Count;
                resp.SqlJobsColumns = dt.Columns;
                resp.SqlJobsRows = dt.Rows;

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
            IsRetry = true;
            LogLine logline = GetLogLine(request.JoblogId);
            this.GlobalParams = logline.Params;
            LoopLocation loopLocation = this.SqlJob.GetLoop();
            LoopExecution(loopLocation.Loop, request.JoblogId, loopLocation.Step, loopLocation.ParentIndex, null);
            return response;
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
                    Params = JsonConvert.DeserializeObject<Dictionary<string, object>>(dt.Rows[0]["params"].ToString()),
                    Status = dt.Rows[0]["status"].ToString(),
                    Refid = dt.Rows[0]["refid"].ToString()
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

                    res.Result = (resource as EbSqlProcessor).Evaluate(_prev, this.GlobalParams);
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
                        this.GlobalParams[_outparam.Name] = _outparam.Value;
                    else
                        this.GlobalParams.Add(_outparam.Name, _outparam.Value);

                    LoopExecution(loop, 0, step, parentindex, _table.Rows[i]);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message + e.StackTrace);
                }
            }
            return true;
        }

        public void LoopExecution(EbLoop loop, int linesid, int step, int parentindex, EbDataRow dataRow)
        {
            try
            {
                if (!IsRetry)
                {
                    string query = @" INSERT INTO eb_joblogs_lines(logmaster_id, params, createdby, createdat)
                                    VALUES(:logmaster_id, :params, :createdby, NOW()) returning id;";
                    DbParameter[] parameters = new DbParameter[]
                    {
                         this.EbConnectionFactory.DataDB.GetNewParameter("logmaster_id", EbDbTypes.Int32, this.LogMasterId) ,
                         this.EbConnectionFactory.DataDB.GetNewParameter("params",EbDbTypes.Json,JsonConvert.SerializeObject(this.GlobalParams)),
                         this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.Int32, UserId)
                     };
                    EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query, parameters);
                    linesid = Convert.ToInt32(dt.Rows[0][0]);
                }
                for (int counter = 0; counter < loop.InnerResources.Count; counter++)
                    loop.InnerResources[counter].Result = this.GetResult(loop.InnerResources[counter], counter, step, parentindex);
                 
                this.EbConnectionFactory.DataDB.DoNonQuery(string.Format("UPDATE eb_joblogs_lines SET status = 'SUCCESS' WHERE id = {0};", linesid));
            }
            catch (Exception e)
            {
                DbParameter[] parameters = new DbParameter[]
                {
                         this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, linesid) ,
                         this.EbConnectionFactory.DataDB.GetNewParameter("message",EbDbTypes.String,  e.Message),
                         this.EbConnectionFactory.DataDB.GetNewParameter("keyvalues",EbDbTypes.Json,  FillKeys(dataRow))
                };
                this.EbConnectionFactory.DataDB.DoNonQuery(@"UPDATE eb_joblogs_lines SET status = 'FAILED', message = :message, keyvalues = :keyvalues WHERE id = :id ;", parameters);
                throw e;
            }
        }

        public string FillKeys(EbDataRow dataRow)
        {
            foreach (var item in this.GlobalParams)
                if (GetKeyvalueDict.ContainsKey(item.Key))
                    this.GetKeyvalueDict[item.Key] = GlobalParams[item.Key].ToString();
            for (int i = 0; i < dataRow.Count; i++)
            {
                if (GetKeyvalueDict.ContainsKey(dataRow.Table.Columns[i].ColumnName))
                    this.GetKeyvalueDict[dataRow.Table.Columns[i].ColumnName] = dataRow[i].ToString();
            }


            //foreach (var item in dataRow)
            //    dataRow.Table.Columns[columnname].ColumnIndex
            //    if (  item != null)
            //        this.GetKeyvalueDict[item.ToString()] =  item .



            //foreach (var item in this.GetKeyvalueDict)
            //{
            //    if (GlobalParams.ContainsKey(item.Key))
            //        this.GetKeyvalueDict[item.Key] = GlobalParams[item.Key].ToString();
            //    else if (dataRow != null && dataRow[item.Key] != null)
            //        this.GetKeyvalueDict[item.Key] = dataRow[item.Key].ToString();
            //}




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
                //if (this.SqlJob.Resources[step - 1] is EbLoop)
                //{
                //    if ((this.SqlJob.Resources[step - 1] as EbLoop).InnerResources[0] is EbTransaction)
                //    {
                //        OutParams = ((this.SqlJob.Resources[step - 1] as EbLoop).InnerResources[0] as EbTransaction).InnerResources[step - 1].GetOutParams(InputParam);
                //    }
                //    else
                //        OutParams = (this.SqlJob.Resources[step - 1] as EbLoop).InnerResources[step - 1].GetOutParams(InputParam);
                //}
                //else 
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
                    p.Value = this.GlobalParams[p.Name].ToString();
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

    public class EbSqlProcessor : SqlJobResource
    {
        public EbScript Script { get; set; }

        public SqlJobScript Evaluate(SqlJobResource _prevres, Dictionary<string, object> GlobalParams)
        {
            string code = this.Script.Code.Trim();
            EbDataSet _ds = null;
            SqlJobScript script = new SqlJobScript();

            Script valscript = CSharpScript.Create<dynamic>(code,
                ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core")
                .WithImports("System.Dynamic", "System", "System.Collections.Generic", "System.Diagnostics", "System.Linq")
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

                foreach (KeyValuePair<string, object> kp in GlobalParams)
                {
                    globals["Params"].Add(kp.Key, new NTV
                    {
                        Name = kp.Key,
                        Type = (kp.Value.GetType() == typeof(JObject)) ? EbDbTypes.Object : (EbDbTypes)Enum.Parse(typeof(EbDbTypes), kp.Value.GetType().Name, true),
                        Value = kp.Value
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
        //public override List<Param> GetOutParams(List<Param> _param, int step)
        //{
        //    return _param;
        //}
    }


   


}
