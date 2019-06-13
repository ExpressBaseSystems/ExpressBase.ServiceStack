using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.IdentityModel.Tokens.Jwt;
using System.Data;
using Npgsql;
using ExpressBase.Common.Structures;
using System.Data.Common;
using System.Text.RegularExpressions;
using ExpressBase.Common.Constants;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [DefaultView("ds")]
    [Authenticate]
    public class DataSourceService : EbBaseService
    {
        public DataSourceService(IEbConnectionFactory _dbf) : base(_dbf) { }

        [CompressResponse]
        public DataSourceDataResponse Any(DataSourceDataRequest request)
        {
            this.Log.Info("data request");
            DataSourceDataResponse resp = new DataSourceDataResponse();
            try
            {
                EbObjectService myService = base.ResolveService<EbObjectService>();
                EbDataReader _ds = this.Redis.Get<EbDataReader>(request.RefId);
                string _sql = string.Empty;

                if (_ds == null)
                {
                    EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                    _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbDataReader>(request.RefId, _ds);
                }
                if (_ds != null && _ds.FilterDialogRefId != string.Empty)
                {
                    EbFilterDialog _dsf = this.Redis.Get<EbFilterDialog>(_ds.FilterDialogRefId);
                    if (_dsf == null)
                    {
                        EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = _ds.FilterDialogRefId });
                        _dsf = EbSerializers.Json_Deserialize(result.Data[0].Json);
                        Redis.Set<EbFilterDialog>(_ds.FilterDialogRefId, _dsf);
                    }
                    if (request.Params == null)
                        request.Params = _dsf.GetDefaultParams();
                }

                bool _isPaged = false;
                if (_ds != null)
                {
                    string _c = string.Empty;
                    string tempsql = string.Empty;

                    if (request.TFilters != null && request.TFilters.Count > 0)
                    {
                        foreach (TFilters _dic in request.TFilters)
                        {
                            string op = _dic.Operator; string col = _dic.Column; string val = _dic.Value;

                            if (op == "x*")
                                _c += string.Format("AND LOWER({0})::text LIKE LOWER('{1}%') ", col, val);
                            else if (op == "*x")
                                _c += string.Format("AND LOWER({0})::text LIKE LOWER('%{1}') ", col, val);
                            else if (op == "*x*")
                                _c += string.Format("AND LOWER({0})::text LIKE LOWER('%{1}%') ", col, val);
                            else if (op == "=")
                                _c += string.Format("AND LOWER({0}::text) = LOWER('{1}') ", col, val);
                            else
                                _c += string.Format("AND {0} {1} '{2}' ", col, op, val);
                        }
                    }
                    //if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.PGSQL)
                    //    _sql = _ds.Sql.Replace("@and_search", _c);
                    //else
                    //    _sql = _ds.Sql.Replace(":and_search", _c);
                    string[] sqlArray = _ds.Sql.Trim().Split(";");
                    foreach (string tsql in sqlArray)
                    {
                        int i = 0;
                        string sql = tsql.Replace("\n", "");
                        if (sql.Trim() != "")
                        {
                            string curSql = tsql;
                            //if (!curSql.ToLower().Contains(":and_search"))
                            //{
                            //    curSql = "SELECT * FROM (" + curSql + ") data WHERE 1=1 :and_search order by :orderby";
                            //}
                            //curSql = curSql.ReplaceAll(";", string.Empty);
                            //curSql = curSql.Replace(":and_search", _c) + ";";
                            MatchCollection matches = Regex.Matches(curSql, @"\;\s*SELECT\s*COUNT\(\*\)\s*FROM");
                            if (matches.Count == 0)
                            {
                                tempsql = curSql.ReplaceAll(";", string.Empty);
                                tempsql = "SELECT COUNT(*) FROM (" + tempsql + ") data" + i + ";";
                            }

                            string sql1 = curSql.ReplaceAll(";", string.Empty);
                            if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.ORACLE)
                            {
                                sql1 = "SELECT * FROM ( SELECT a.*,ROWNUM rnum FROM (" + sql1 + ")a WHERE ROWNUM <= :limit+:offset) WHERE rnum > :offset;";
                                //sql1 += "ALTER TABLE T1 DROP COLUMN rnum;SELECT * FROM T1;";
                            }
                            else
                            {
                                if (!sql1.ToLower().Contains(":limit"))
                                    sql1 = sql1 + " LIMIT :limit OFFSET :offset;";
                            }
                            curSql = sql1 + tempsql;
                            //}
                            //if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.PGSQL)
                            //{
                            //_sql = _sql.Replace("@orderby",
                            //(string.IsNullOrEmpty(request.OrderByCol)) ? "id" : string.Format("{0} {1}", request.OrderByCol, ((request.OrderByDir == 2) ? "DESC" : "ASC")));

                            //_isPaged = (_sql.ToLower().Contains("@offset") && _sql.ToLower().Contains("@limit"));

                            ////var parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, request.Length, request.Start);
                            //if (request.Params == null)
                            //    _sql = _sql.Replace("@id", "0");
                            //}
                            //else
                            //{
                            curSql = curSql.Replace(":orderby",
                           (string.IsNullOrEmpty(request.OrderByCol)) ? "1" : string.Format("{0} {1}", request.OrderByCol, ((request.OrderByDir == 2) ? "DESC" : "ASC")));

                            _isPaged = (curSql.ToLower().Contains(":offset") && curSql.ToLower().Contains(":limit"));


                            if (request.Params == null)
                                curSql = curSql.Replace(":id", "0");
                            //}
                            _sql += curSql;
                        }
                    }
                }
                IEnumerable<DbParameter> parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, request.Length, request.Start);
                Console.WriteLine("Before :  " + DateTime.Now);
                DateTime dtStart = DateTime.Now;
                Console.WriteLine("................................................datasourceDSrequeststart " + DateTime.Now);
                EbDataSet _dataset = new EbDataSet();
                try
                {
                    _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());
                }
                catch (Exception e)
                {
                    resp.ResponseStatus = new ResponseStatus { Message = e.Message };
                }
                Console.WriteLine("................................................datasourceDSrequeststart " + DateTime.Now);
                DateTime dtstop = DateTime.Now;
                Console.WriteLine("..................................totaltimeinSeconds" + dtstop.Subtract(dtStart).Seconds);

                Console.WriteLine(DateTime.Now);
                DateTime dtEnd = DateTime.Now;
                double ts = (dtEnd - dtStart).TotalMilliseconds;
                Console.WriteLine("final:::" + ts);
                int _recordsTotal = 0, _recordsFiltered = 0;
                if (_isPaged)
                {
                    Int32.TryParse(_dataset.Tables[1].Rows[0][0].ToString(), out _recordsTotal);
                    Int32.TryParse(_dataset.Tables[1].Rows[0][0].ToString(), out _recordsFiltered);
                }
                _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[1].Rows.Count;
                _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[1].Rows.Count;

                if (GetLogEnabled(request.RefId))
                {
                    TimeSpan T = _dataset.EndTime - _dataset.StartTime;
                    InsertExecutionLog(_dataset.RowNumbers, T, _dataset.StartTime, request.UserId, request.Params, request.RefId);
                }

                resp.Draw = request.Draw;
                resp.Data = _dataset.Tables[0].Rows;
                resp.RecordsTotal = _recordsTotal;
                resp.RecordsFiltered = _recordsFiltered;
                resp.Ispaged = _isPaged;
                this.Log.Info("dsresponse*****" + resp.Data);
            }
            catch (Exception e) { Console.WriteLine(e.Message + e.StackTrace); }
            return resp;
        }

        [CompressResponse]
        public DataSourceColumnsResponse Any(DataSourceColumnsRequest request)
        {
            DataSourceColumnsResponse resp = new DataSourceColumnsResponse { Columns = new List<ColumnColletion>() };

            string _dsRedisKey = string.Format("{0}_columns", request.RefId);
            try
            {
                EbDataSet _dataset = null;
                bool _isPaged = false;
                if (resp == null || resp.Columns == null || resp.Columns.Count == 0)
                {
                    EbDataReader _ds = null;
                    EbObjectService myService = base.ResolveService<EbObjectService>();
                    EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                    _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbDataReader>(request.RefId, _ds);

                    if (_ds != null)
                    {
                        string _sql = string.Empty;
                        /*if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.PGSQL)
                        {
                            _sql = _ds.Sql.Replace("@and_search", string.Empty).Replace("@orderby", "1");
                            _isPaged = (_sql.ToLower().Contains("@offset") && _sql.ToLower().Contains("@limit"));


                            if (request.Params == null)
                                _sql = _sql.Replace("@id", "0");
                        }
                        else
                        {*/
                        _sql = _ds.Sql.Replace(":and_search", string.Empty).Replace(":orderby", "1");
                        _isPaged = (_sql.ToLower().Contains(":offset") && _sql.ToLower().Contains(":limit"));


                        if (request.Params == null)
                            _sql = _sql.Replace(":id", "0");
                        // }
                        IEnumerable<DbParameter> parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, 0, 0);


                        Console.WriteLine("................................................datasourcecolumnrequeststart " + System.DateTime.Now);
                        try
                        {
                            _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());
                        }
                        catch (Exception e)
                        {
                            resp.ResponseStatus = new ResponseStatus { Message = e.Message };
                        }
                        Console.WriteLine("................................................datasourcecolumnrequestfinish " + System.DateTime.Now);

                        foreach (EbDataTable dt in _dataset.Tables)
                            resp.Columns.Add(dt.Columns);

                        resp.IsPaged = _isPaged;
                        this.Redis.Set<DataSourceColumnsResponse>(_dsRedisKey, resp);
                        //TimeSpan T = _dataset.EndTime - _dataset.StartTime;
                        //InsertExecutionLog(_dataset.RowNumbers, T, _dataset.StartTime, request.UserId, request.Params, request.RefId);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns error: " + e.Message + e.StackTrace);
                this.Redis.Remove(_dsRedisKey);
            }
            return resp;
        }

        [CompressResponse]
        public DataSourceColumnsResponse Get(DataSourceDataSetColumnsRequest request)
        {
            string _dsRedisKey = string.Format("{0}_columns", request.RefId);
            EbDataSet _dataset = null;
            DataSourceColumnsResponse resp = new DataSourceColumnsResponse();
            resp.Columns = new List<ColumnColletion>();

            EbDataReader _ds = null;
            EbObjectService myService = base.ResolveService<EbObjectService>();
            try
            {
                EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                if (result != null && result.Data != null && result.Data.Count > 0)
                {
                    _ds = EbSerializers.Json_Deserialize<EbDataReader>(result.Data[0].Json);
                    Redis.Set<EbDataReader>(request.RefId, _ds);
                    if (_ds != null)
                    {
                        string sql = string.Empty;
                        string[] sqlArray = _ds.Sql.Trim().Split(";");
                        foreach (string _sql in sqlArray)
                        {
                            if (_sql != string.Empty && !_sql.ToLower().Contains("limit") && !_sql.ToLower().Contains("offset"))
                                sql += _sql + " LIMIT :limit OFFSET :offset;";
                            else
                                sql += _sql;
                        }
                        sql = sql.Replace(":and_search", string.Empty).Replace(":orderby", "1");
                        bool _isPaged = true;

                        IEnumerable<DbParameter> parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, 0, 0);
                        try
                        {
                            _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters.ToArray<System.Data.Common.DbParameter>());
                        }
                        catch (Exception e)
                        {
                            resp.ResponseStatus = new ResponseStatus { Message = e.Message };
                        }
                        foreach (EbDataTable dt in _dataset.Tables)
                            resp.Columns.Add(dt.Columns);

                        this.Redis.Set<DataSourceColumnsResponse>(_dsRedisKey, resp);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns e.Message: " + e.Message);
                this.Redis.Remove(_dsRedisKey);
            }
            return resp;
        }

        [CompressResponse]
        public DataSourceDataSetDataResponse Any(DataSourceDataSetDataRequest request)
        {
            EbObjectService myService = base.ResolveService<EbObjectService>();

            this.Log.Info("data request");
            DataSourceDataSetDataResponse resp = new DataSourceDataSetDataResponse();
            EbDataReader _ds = this.Redis.Get<EbDataReader>(request.RefId);
            try
            {
                if (_ds == null)
                {
                    EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                    _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbDataReader>(request.RefId, _ds);
                }
                if (_ds != null && _ds.FilterDialogRefId != string.Empty)
                {
                    EbFilterDialog _dsf = this.Redis.Get<EbFilterDialog>(_ds.FilterDialogRefId);
                    if (_dsf == null)
                    {
                        EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = _ds.FilterDialogRefId });
                        _dsf = EbSerializers.Json_Deserialize(result.Data[0].Json);
                        Redis.Set<EbFilterDialog>(_ds.FilterDialogRefId, _dsf);
                    }
                    if (request.Params == null)
                        request.Params = _dsf.GetDefaultParams();
                }
                EbDataSet _dataset = new EbDataSet();
                string sql = string.Empty;
                string countsql = string.Empty;
                string[] sqlArray = _ds.Sql.Trim().Split(";");
                sql = sqlArray[request.QueryIndex];
                if (request.QueryIndex == 0)
                {
                    string _c = string.Empty;
                    string firstsql = sqlArray[0];
                    if (request.TFilters != null && request.TFilters.Count > 0)
                    {
                        foreach (TFilters _dic in request.TFilters)
                        {
                            string op = _dic.Operator; string col = _dic.Column; string val = _dic.Value; string type = _dic.Type;
                            string[] array = _dic.Value.Split("|");
                            if (array.Length == 0)
                            {
                                if (op == "x*")
                                    _c += string.Format("AND LOWER({0}) LIKE LOWER('{1}%') ", col, val);
                                else if (op == "*x")
                                    _c += string.Format("AND LOWER({0}) LIKE LOWER('%{1}') ", col, val);
                                else if (op == "*x*")
                                    _c += string.Format("AND LOWER({0}) LIKE LOWER('%{1}%') ", col, val);
                                else if (op == "=")
                                    _c += string.Format("AND LOWER({0}) = LOWER('{1}') ", col, val);
                                else
                                    _c += string.Format("AND {0} {1} '{2}' ", col, op, val);
                            }
                            else
                            {
                                string _cond = string.Empty;
                                for (int i = 0; i < array.Length; i++)
                                {
                                    if (array[i].Trim() != string.Empty)
                                    {
                                        if (type == "string")
                                        {
                                            if (op == "x*")
                                                _cond += string.Format(" LOWER({0}) LIKE LOWER('{1}%') OR", col, array[i].Trim());
                                            else if (op == "*x")
                                                _cond += string.Format(" LOWER({0}) LIKE LOWER('%{1}') OR", col, array[i].Trim());
                                            else if (op == "*x*")
                                                _cond += string.Format(" LOWER({0}) LIKE LOWER('%{1}%') OR", col, array[i].Trim());
                                            else if (op == "=")
                                                _cond += string.Format(" LOWER({0}) = LOWER('{1}') OR", col, array[i].Trim());
                                        }
                                        else
                                        {
                                            if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.ORACLE)
                                            {
                                                if (type == "date")
                                                    _cond += string.Format(" {0} {1} date '{2}' OR", col, op, array[i].Trim());
                                                else
                                                    _cond += string.Format(" {0} {1} '{2}' OR", col, op, array[i].Trim());
                                            }
                                            else
                                                _cond += string.Format(" {0} {1} '{2}' OR", col, op, array[i].Trim());
                                        }
                                    }
                                }
                                int place = _cond.LastIndexOf("OR");
                                _cond = _cond.Substring(0, place);
                                _c += "AND (" + _cond + ")";
                            }
                        }
                    }

                    if (!firstsql.ToLower().Contains(":and_search"))
                    {
                        firstsql = "SELECT * FROM (" + firstsql + "\n ) data WHERE 1=1 :and_search order by :orderby";
                    }
                    firstsql = firstsql.ReplaceAll(";", string.Empty);
                    firstsql = firstsql.Replace(":and_search", _c) + ";";

                    MatchCollection matches = Regex.Matches(firstsql, @"\;\s*SELECT\s*COUNT\(\*\)\s*FROM");
                    if (matches.Count == 0)
                    {
                        countsql = firstsql.ReplaceAll(";", string.Empty);
                        countsql = "SELECT COUNT(*) FROM (" + countsql + ") data1;";
                    }

                    firstsql = firstsql.ReplaceAll(";", string.Empty);
                    if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.ORACLE)
                    {
                        firstsql = "SELECT * FROM ( SELECT a.*,ROWNUM rnum FROM (" + firstsql + ")a WHERE ROWNUM <= :limit+:offset) WHERE rnum > :offset;";
                        //sql1 += "ALTER TABLE T1 DROP COLUMN rnum;SELECT * FROM T1;";
                    }
                    else
                    {
                        if (!firstsql.ToLower().Contains("limit") && !firstsql.ToLower().Contains("offset"))
                            firstsql = firstsql + " LIMIT :limit OFFSET :offset;";
                    }
                    sql = countsql + firstsql;
                }

                string __order = string.Empty;
                if (request.OrderBy != null)
                {
                    __order = string.Format("{0} {1}", request.OrderBy.Column, (request.OrderBy.Direction == 2) ? "DESC" : "ASC");
                }
                sql = sql.Replace(":orderby", (string.IsNullOrEmpty(__order)) ? "1" : __order);
                bool _isPaged = true;
                IEnumerable<DbParameter> parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, request.Length, request.Start);
                try
                {
                    _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters.ToArray<System.Data.Common.DbParameter>());
                }
                catch (Exception e)
                {
                    resp.ResponseStatus = new ResponseStatus { Message = e.Message };
                }
                if (GetLogEnabled(request.RefId))
                {
                    TimeSpan T = _dataset.EndTime - _dataset.StartTime;
                    InsertExecutionLog(_dataset.RowNumbers, T, _dataset.StartTime, request.UserId, request.Params, request.RefId);
                }
                int _recordsTotal = 0, _recordsFiltered = 0;
                if (_isPaged)
                {
                    if (request.QueryIndex == 0)
                    {
                        Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsTotal);
                        Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsFiltered);
                    }
                    else
                    {
                        _recordsTotal = _dataset.Tables[0].Rows.Count;
                        _recordsFiltered = _dataset.Tables[0].Rows.Count;
                    }
                }
                _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[1].Rows.Count;
                _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[1].Rows.Count;

                resp.Data = (request.QueryIndex == 0) ? _dataset.Tables[1].Rows : _dataset.Tables[0].Rows;
                resp.Draw = request.Draw;
                resp.RecordsTotal = _recordsTotal;
                resp.RecordsFiltered = _recordsFiltered;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return resp;
        }

        [CompressResponse]
        public DataSourceDataSetResponse Any(DataSourceDataSetRequest request)
        {
            this.Log.Info("data request");

            DataSourceDataSetResponse resp = new DataSourceDataSetResponse();
            string _sql = string.Empty;
            try
            {
                EbDataReader _ds = this.Redis.Get<EbDataReader>(request.RefId);
                if (_ds == null)
                {
                    EbObjectService myService = base.ResolveService<EbObjectService>();
                    EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                    _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbDataReader>(request.RefId, _ds);
                }
                if (_ds.FilterDialogRefId != string.Empty && _ds.FilterDialogRefId != null)
                {
                    EbFilterDialog _dsf = this.Redis.Get<EbFilterDialog>(_ds.FilterDialogRefId);
                    if (_dsf == null)
                    {
                        EbObjectService myService = base.ResolveService<EbObjectService>();
                        EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = _ds.FilterDialogRefId });
                        _dsf = EbSerializers.Json_Deserialize(result.Data[0].Json);
                        Redis.Set<EbFilterDialog>(_ds.FilterDialogRefId, _dsf);
                    }
                    if (request.Params == null)
                        request.Params = _dsf.GetDefaultParams();
                }
                if (_ds != null)
                {
                    string _c = string.Empty;
                    _sql = _ds.Sql;
                }

                try
                {
                    IEnumerable<DbParameter> parameters = DataHelper.GetParams(this.EbConnectionFactory, false, request.Params, 0, 0);
                    resp.DataSet = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());

                    if (GetLogEnabled(request.RefId))
                    {
                        TimeSpan T = resp.DataSet.EndTime - resp.DataSet.StartTime;
                        InsertExecutionLog(resp.DataSet.RowNumbers, T, resp.DataSet.StartTime, request.UserId, request.Params, request.RefId);
                    }
                }
                catch (Exception e)
                {
                    resp.ResponseStatus = new ResponseStatus { Message = e.Message };
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            return resp;
        }

        public SqlFuncTestResponse Post(SqlFuncTestRequest request)
        {
            SqlFuncTestResponse resp = new SqlFuncTestResponse();
            try
            {
                List<DbParameter> parameter = new List<DbParameter>();
                List<string> _params = new List<string>();
                string sql = string.Empty;
                if (request.Parameters.Count > 0)
                {
                    foreach (Param p in request.Parameters)
                    {
                        _params.Add(":" + p.Name);
                        parameter.Add(this.EbConnectionFactory.DataDB.GetNewParameter(p.Name, (EbDbTypes)Convert.ToInt32(p.Type), p.Value));
                    }
                    sql = string.Format(@"SELECT * FROM {0}({1})", request.FunctionName, string.Join(",", _params));
                    resp.Data = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameter.ToArray());
                    resp.Reponse = true;
                }
                else
                {
                    sql = string.Format(@"SELECT * FROM {0}()", request.FunctionName);
                    resp.Data = this.EbConnectionFactory.ObjectsDB.DoQuery(sql);
                    resp.Reponse = true;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception  at sql function execution::" + e.Message);
                resp.Reponse = false;
                resp.Data = null;
            }
            return resp;
        }

        public DatawriterResponse Post(DatawriterRequest request)
        {
            EbDataTable t = new EbDataTable();
            DatawriterResponse resp = new DatawriterResponse();
            try
            {
                List<DbParameter> parameter = new List<DbParameter>();
                List<string> _params = new List<string>();
                int trans = 0;
                if (request.Parameters.Count > 0)
                {
                    foreach (Param p in request.Parameters)
                    {
                        _params.Add(":" + p.Name);
                        parameter.Add(this.EbConnectionFactory.DataDB.GetNewParameter(p.Name, (EbDbTypes)Convert.ToInt32(p.Type), p.ValueTo));
                    }
                    trans = this.EbConnectionFactory.ObjectsDB.DoNonQuery(request.Sql, parameter.ToArray());
                }
                else
                {
                    trans = this.EbConnectionFactory.ObjectsDB.DoNonQuery(request.Sql);
                }
                t.Columns.Add(new EbDataColumn(0, "status", EbDbTypes.BooleanOriginal));
                t.Rows.Add(new EbDataRow());
                if (trans > 0)
                    t.Rows[0].Add(true);
                else
                    t.Rows[0].Add(false);

                resp.Data = t;
            }
            catch (Exception e)
            {
                resp.ResponseStatus = new ResponseStatus { Message = e.Message };
                t.Rows[0].Add(false);
                resp.Data = t;
            }
            return resp;
        }
    }
}