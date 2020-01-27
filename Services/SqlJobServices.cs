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
using ExpressBase.Objects.Services;
using ServiceStack;
using ServiceStack.Messaging;
using System.Globalization;
using ExpressBase.Common.Singletons;
using ExpressBase.Objects.Objects.DVRelated;

namespace ExpressBase.ServiceStack.Services
{
    public class SqlJobServices : EbBaseService
    {

        public const string EB_LOC_ID = "eb_loc_id";

        public int LogMasterId { get; set; }

        public int UserId { get; set; }

        public string UserAuthId { get; set; }

        public string SolutionId { get; set; }

        public bool IsRetry = false;

        public Object FinalResult { set; get; }

        private DbConnection TransactionConnection = null;

        public Dictionary<string, TV> GlobalParams { set; get; }

        public Dictionary<string, object> TempParams { set; get; }

        private ExecuteSqlJobResponse JobResponse { get; set; }

        private EbObjectService StudioServices { set; get; }

        Script valscript = null;

        SqlJobGlobals Globals = null;

        public SqlJobServices(IEbConnectionFactory _dbf) : base(_dbf)
        {
            this.StudioServices = base.ResolveService<EbObjectService>();
            this.JobResponse = new ExecuteSqlJobResponse();
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

        public ExecuteSqlJobResponse Post(ExecuteSqlJobRequest request)
        {
            dynamic version = null;
            this.StudioServices.EbConnectionFactory = this.EbConnectionFactory;
            try
            {
                if (request.ObjId == 0 && request.RefId != String.Empty && request.RefId != null)
                {
                    version = this.StudioServices.Get(new EbObjectParticularVersionRequest { RefId = request.RefId });
                }
                else if (request.ObjId > 0)
                {
                    version = this.StudioServices.Get(new EbObjectFetchLiveVersionRequest { Id = request.ObjId });
                }
                if (version != null)
                {
                    SqlJob = EbSerializers.Json_Deserialize<EbSqlJob>(version.Data[0].Json);
                    UserId = request.UserId;
                    UserAuthId = request.UserAuthId;
                    SolutionId = request.SolnId;
                    string query = @" 
                        INSERT INTO 
                            eb_joblogs_master(refid, type, createdby, created_at, params) 
                        VALUES
                            (:refid, :type, :createdby, NOW(), :params) 
                        RETURNING id;";
                    DbParameter[] parameters = new DbParameter[] {
                    this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.String, this.SqlJob.RefId) ,
                    this.EbConnectionFactory.DataDB.GetNewParameter("type",EbDbTypes.Int32,this.SqlJob.Type),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby",EbDbTypes.Int32, UserId),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("params",EbDbTypes.Json, EbSerializers.Json_Serialize(request.GlobalParams))
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
                else
                {
                    Console.WriteLine("check if object id is correct or not.");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return JobResponse;
        }

        public ListSqlJobsResponse Get(ListSqlJobsRequest request)
        {
            ListSqlJobsResponse resp = new ListSqlJobsResponse();
            EbDataTable dtNew = new EbDataTable();
            try
            {
                EbObjectParticularVersionResponse version = (EbObjectParticularVersionResponse)this.StudioServices.Get(new EbObjectParticularVersionRequest { RefId = request.RefId });
                SqlJob = EbSerializers.Json_Deserialize(version.Data[0].Json);
                string query = @"
                    SELECT
                        logmaster_id , message, createdby, createdat, COALESCE(status, 'F') status, id, keyvalues 
                    FROM
                        eb_joblogs_lines 
                    WHERE 
                        logmaster_id IN (SELECT id FROM eb_joblogs_master WHERE to_char(created_at, 'dd-mm-yyyy') = :date AND refid = :refid) AND 
                        id NOT IN (SELECT retry_of FROM eb_joblogs_lines) 
                    ORDER BY 
                        logmaster_id DESC, status ASC; ";
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
                        try
                        {
                            if (dr["keyvalues"].ToString() != "")
                            {
                                dtNew.Rows.Add(dtNew.NewDataRow2());
                                Dictionary<string, TV> _list = JsonConvert.DeserializeObject<Dictionary<string, TV>>(dr["keyvalues"].ToString());
                                int _columnCount = 0;
                                foreach (KeyValuePair<string, TV> _c in _list)
                                {
                                    try
                                    {
                                        Param obj = new Param();
                                        obj.Value = _c.Value.Value.ToString();
                                        obj.Type = _c.Value.Type;

                                        dtNew.Rows[_rowCount][_columnCount++] = obj.ValueTo;
                                    }
                                    catch (Exception e)
                                    {
                                        Console.WriteLine("ERROR: Sql log lines - keyvalues Exception: " + e.Message + e.StackTrace);
                                    }
                                }
                                if (dt.Rows[_rowCount]["message"].ToString() == String.Empty)
                                {
                                    if (dt.Rows[_rowCount]["status"].ToString() == "S")
                                    {
                                        dt.Rows[_rowCount]["message"] = "Success";
                                    }
                                    else
                                    {
                                        dt.Rows[_rowCount]["message"] = "Failed";
                                    }
                                }

                                for (int i = 0; i < dr.Count; i++)
                                {
                                    dtNew.Rows[_rowCount][i + customColumnCount] = dt.Rows[_rowCount][i];
                                }
                                _rowCount++;
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("ERROR: Sql log lines Exception: " + e.Message + e.StackTrace);
                        }
                    }
                }
                resp.SqlJobsColumns = dtNew.Columns;
                resp.SqlJobsRows = dtNew.Rows;


                User _user = this.Redis.Get<User>(request.UserAuthId);
                DVColumnCollection DVColumnCollection = GetColumnsForSqlJob(resp.SqlJobsColumns);
                resp.SqlJobsDvColumns = EbSerializers.Json_Serialize(DVColumnCollection);
                resp.Levels = SqlPreProcessing(dtNew, DVColumnCollection, _user);
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: : Sql log lines Exception: " + e.Message + e.StackTrace);
            }
            EbSerializers.Json_Serialize(resp);
            return resp;
        }

        public DVColumnCollection GetColumnsForSqlJob(ColumnColletion __columns)
        {

            var Columns = new DVColumnCollection();
            try
            {
                foreach (EbDataColumn column in __columns)
                {
                    DVBaseColumn _col = null;
                    if (column.Type == EbDbTypes.String)
                        _col = new DVStringColumn { Data = column.ColumnIndex, Name = column.ColumnName, sTitle = column.ColumnName, Type = column.Type, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                    else if (column.Type == EbDbTypes.Int16 || column.Type == EbDbTypes.Int32 || column.Type == EbDbTypes.Int64 || column.Type == EbDbTypes.Double || column.Type == EbDbTypes.Decimal || column.Type == EbDbTypes.VarNumeric)
                        _col = new DVNumericColumn { Data = column.ColumnIndex, Name = column.ColumnName, sTitle = column.ColumnName, Type = column.Type, bVisible = true, sWidth = "100px", ClassName = "tdheight dt-body-right" };
                    else if (column.Type == EbDbTypes.Boolean)
                        _col = new DVBooleanColumn { Data = column.ColumnIndex, Name = column.ColumnName, sTitle = column.ColumnName, Type = column.Type, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                    else if (column.Type == EbDbTypes.DateTime || column.Type == EbDbTypes.Date || column.Type == EbDbTypes.Time)
                        _col = new DVDateTimeColumn { Data = column.ColumnIndex, Name = column.ColumnName, sTitle = column.ColumnName, Type = column.Type, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                    else if (column.Type == EbDbTypes.Bytea)
                        _col = new DVStringColumn { Data = column.ColumnIndex, Name = column.ColumnName, sTitle = column.ColumnName, Type = column.Type, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                    _col.RenderType = _col.Type;
                    if (column.ColumnName == "keyvalues" || column.ColumnName == "logmaster_id" || column.ColumnName == "createdby")
                        _col.bVisible = false;
                    Columns.Add(_col);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("no coloms" + e.StackTrace);
            }

            return Columns;
        }


        public List<GroupingDetails> SqlPreProcessing(EbDataTable FormattedTable, DVColumnCollection columns, User _user)
        {
            Dictionary<string, GroupingDetails> RowGrouping = new Dictionary<string, GroupingDetails> { };
            string PreviousGroupingText = "";
            int CurSortIndex = 0;
            int SerialCount = 0;
            int dvColCount = columns.Count - 1;
            int TotalLevels = 1;
            List<DVBaseColumn> RowGroupingColumns = new List<DVBaseColumn> { columns.Get("logmaster_id") };
            int RowCount = FormattedTable.Rows.Count;

            CultureInfo Culture = CultureHelper.GetSerializedCultureInfo(_user.Preference.Locale).GetCultureInfo();
            EbDataVisualization Visualization = new EbTableVisualization { Columns = columns, AutoGen = false };
            DataVisService DataVisService = base.ResolveService<DataVisService>();
            List<int> AggregateColumnIndexes = DataVisService.GetAggregateIndexes(columns);

            for (int i = 0; i < FormattedTable.Rows.Count; i++)
            {
                EbDataRow currentRow = FormattedTable.Rows[i];
                DataVisService.DoRowGrouping4Sqljob(currentRow, Visualization, Culture, _user, ref FormattedTable, false, ref RowGrouping, ref PreviousGroupingText,
                    ref CurSortIndex, ref SerialCount, i, dvColCount, TotalLevels, ref AggregateColumnIndexes, ref RowGroupingColumns, RowCount, null);
            }
            List<GroupingDetails> SortedGroupings = RowGrouping.Values.ToList();
            SortedGroupings.Sort();
            return SortedGroupings;
        }

        public RetryJobResponse Post(RetryJobRequest request)
        {
            RetryJobResponse response = new RetryJobResponse();
            UserId = request.UserId;
            UserAuthId = request.UserAuthId;
            SolutionId = request.SolnId;
            response.Status = false;
            IsRetry = true;
            LogLine logline = GetLogLine(request.JoblogId);
            this.LogMasterId = logline.masterid;
            this.GlobalParams = logline.Params;
            EbObjectParticularVersionResponse version = (EbObjectParticularVersionResponse)this.StudioServices.Get(new EbObjectParticularVersionRequest { RefId = request.RefId });
            SqlJob = EbSerializers.Json_Deserialize(version.Data[0].Json);
            LoopLocation loopLocation = SqlJob.GetLoop();
            try
            {
                ExecuteLoop(loopLocation.Loop, request.JoblogId, loopLocation.Step, loopLocation.ParentIndex, null, logline.Keyvalues);
                response.Status = true;
            }
            catch (Exception e)
            {
                response.Status = false;
                Console.WriteLine("exception" + e);
            }
            return response;
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
                    res.Result = DoLoop(resource as EbLoop, index, parentindex);

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
                if (resource is IJobFinishingControl)
                    FinalResult = res.Result;
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

                if (TransactionConnection != null)
                    dt = this.EbConnectionFactory.ObjectsDB.DoQueries(TransactionConnection, (ObjectWrapper as EbDataReader).Sql, p.ToArray());
                else
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
            int status;
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

                if (TransactionConnection != null)
                    status = this.EbConnectionFactory.ObjectsDB.DoNonQuery(TransactionConnection, (ObjectWrapper as EbDataWriter).Sql, p.ToArray());
                else
                    status = this.EbConnectionFactory.ObjectsDB.DoNonQuery((ObjectWrapper as EbDataWriter).Sql, p.ToArray());

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

        public bool DoLoop(EbLoop loop, int step, int parentindex)
        {
            EbDataTable _table = null;
            if (parentindex == 0 && step == 1)
                _table = (this.SqlJob.Resources[step - 1].Result as EbDataSet).Tables[0];
            else
                _table = (this.SqlJob.Resources[parentindex - 1].Result as EbDataSet).Tables[0];

            int _rowcount = _table.Rows.Count;
            for (int i = 0; i < _rowcount; i++)
            {
                FinalResult = null;
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


                    if (!this.GlobalParams.ContainsKey(EB_LOC_ID))
                    {
                        if (_table.Columns[EB_LOC_ID] != null)
                        {
                            this.GlobalParams.Add(EB_LOC_ID, new TV { Type = EbDbTypes.Int32.ToString(), Value = _table.Rows[i][EB_LOC_ID].ToString() });
                        }
                    }

                    ExecuteLoop(loop, 0, step, parentindex, _table.Rows[i], null);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message + e.StackTrace);
                }
            }
            return true;
        }

        public void ExecuteLoop(EbLoop loop, int retryof, int step, int parentindex, EbDataRow dataRow, Dictionary<string, TV> keyvals)
        {
            try
            {
                int counter;
                string query = @" INSERT INTO 
                                    eb_joblogs_lines(logmaster_id, createdby, createdat, retry_of)
                                VALUES
                                    (:logmaster_id, :createdby, NOW(), :retry_of) 
                                RETURNING id;";
                DbParameter[] parameters = new DbParameter[]
                {
                         this.EbConnectionFactory.DataDB.GetNewParameter("logmaster_id", EbDbTypes.Int32, this.LogMasterId) ,
                         this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.Int32, UserId),
                         this.EbConnectionFactory.ObjectsDB.GetNewParameter("retry_of", EbDbTypes.Int32, retryof)
                 };

                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query, parameters);

                int linesid = Convert.ToInt32(dt.Rows[0][0]);
                string _keyvalues = (dataRow is null) ? JsonConvert.SerializeObject(keyvals) : FillKeys(dataRow);
                try
                {
                    for (counter = 0; counter < loop.InnerResources.Count; counter++)
                    {
                        if (loop.InnerResources[counter] is EbSqlProcessor)
                        {
                            if (loop.InnerResources[counter - 1] is EbSqlJobReader)
                                if (((loop.InnerResources[counter - 1] as EbSqlJobReader).Result as EbDataSet).Tables[0].Rows.Count > 0)
                                    loop.InnerResources[counter].Result = this.GetResult(loop.InnerResources[counter], counter, step, parentindex);
                                else
                                {
                                    Console.WriteLine("Datareader returned 0 rows : " + (loop.InnerResources[counter - 1] as EbSqlJobReader).RefId + "\n" +
                                        JsonConvert.SerializeObject(dataRow));
                                    return;
                                }
                        }
                        loop.InnerResources[counter].Result = this.GetResult(loop.InnerResources[counter], counter, step, parentindex);
                    }

                    DbParameter[] e_parameters = new DbParameter[]
                    {    this.EbConnectionFactory.DataDB.GetNewParameter("linesid", EbDbTypes.Int32, linesid) ,
                         this.EbConnectionFactory.DataDB.GetNewParameter("keyvalues",EbDbTypes.Json,  _keyvalues),
                         this.EbConnectionFactory.DataDB.GetNewParameter("result",EbDbTypes.Json, JsonConvert.SerializeObject(FinalResult))
                    };
                    this.EbConnectionFactory.DataDB.DoNonQuery(@"UPDATE 
                                                                    eb_joblogs_lines 
                                                                SET
                                                                    status = 'S' , keyvalues = :keyvalues, result =:result
                                                                WHERE
                                                                    id = :linesid;", e_parameters);
                }
                catch (Exception e)
                {
                    DbParameter[] e_parameters = new DbParameter[]
                    {
                         this.EbConnectionFactory.DataDB.GetNewParameter("linesid", EbDbTypes.Int32, linesid) ,
                         this.EbConnectionFactory.DataDB.GetNewParameter("message",EbDbTypes.String,  e.Message),
                         this.EbConnectionFactory.DataDB.GetNewParameter("keyvalues",EbDbTypes.Json,  _keyvalues)
                    };
                    this.EbConnectionFactory.DataDB.DoNonQuery(@"UPDATE 
                                                                    eb_joblogs_lines 
                                                                SET 
                                                                    status = 'F', message = :message, keyvalues = :keyvalues
                                                                WHERE
                                                                    id = :linesid ;", e_parameters);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error at LoopExecution");
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
            using (TransactionConnection = this.EbConnectionFactory.DataDB.GetNewConnection())
            {
                TransactionConnection.Open();

                DbTransaction trans = TransactionConnection.BeginTransaction();
                try
                {
                    for (int counter = 0; counter < txn.InnerResources.Count; counter++)
                        if (txn.InnerResources[counter] is EbSqlProcessor)
                        {
                            if (txn.InnerResources[counter - 1] is EbSqlJobReader)
                                if (((txn.InnerResources[counter - 1] as EbSqlJobReader).Result as EbDataSet).Tables[0].Rows.Count > 0)
                                    txn.InnerResources[counter].Result = this.GetResult(txn.InnerResources[counter], counter, step, 0);
                                else
                                {
                                    Console.WriteLine("Datareader returned 0 rows : " + (txn.InnerResources[counter - 1] as EbSqlJobReader).Reference);
                                    return true;
                                }
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

            User User = this.Redis.Get<User>(UserAuthId);
            int LocId = 1;
            if (this.GlobalParams.ContainsKey(EB_LOC_ID))
            {
                LocId = Convert.ToInt32((this.GlobalParams[EB_LOC_ID]).Value);
            }
            else if (User != null && User.Preference != null && User.Preference.DefaultLocation > 0)
            {
                LocId = User.Preference.DefaultLocation;
            }
            InsertOrUpdateFormDataResp resp = webFormServices.Any(new InsertOrUpdateFormDataRqst
            {
                RefId = dataPusher.Pusher,
                PushJson = dataPusher.PushJson,
                UserId = UserId,
                UserAuthId = UserAuthId,
                RecordId = 0,
                UserObj = User,
                LocId = 1,
                SolnId = SolutionId,
                WhichConsole = "uc",
                FormGlobals = new FormGlobals { Params = Globals.Params },
                TransactionConnection =TransactionConnection
            });
            return resp;
        }
    }

    [Restrict(InternalOnly = true)]
    public class SqlJobInternalService : EbMqBaseService
    {

        public SqlJobInternalService(IMessageProducer _mqp) : base(_mqp) { }

        public void post(SqlJobInternalRequest request)
        {
            SqlJobServices SqlJobServices = base.ResolveService<SqlJobServices>();
            SqlJobServices.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            ExecuteSqlJobResponse response = SqlJobServices.Post(new ExecuteSqlJobRequest
            {
                GlobalParams = request.GlobalParams,
                ObjId = request.ObjId,
                SolnId = request.SolnId,
                UserAuthId = request.UserAuthId,
                UserId = request.UserId
            });

        }
    }

}
