using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects;
using ExpressBase.Objects.Objects.DVRelated;
using ExpressBase.Objects.Objects.ReportRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using ServiceStack;
using ServiceStack.Logging;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace ExpressBase.ServiceStack
{
    [Authenticate]
    public class DataVisService : EbBaseService
    {
        private const string HeaderPrefix = "H_", FooterPrefix = "F_", GroupDelimiter = ":-:", AfterText = "After", BeforeText = "Before", BlankText = "(Blank)";
        public DataVisService(IEbConnectionFactory _dbf) : base(_dbf) { }

        //[CompressResponse]
        //public DataSourceDataResponse Any(DataVisDataRequest request)
        //{
        //    this.Log.Info("data request");

        //    DataSourceDataResponse dsresponse = null;

        //    EbDataVisualization _dV = request.EbDataVisualization;

        //    //if (request.WhichConsole == "uc")
        //    //    _dVSet = this.Redis.Get<EbDataVisualizationSet>(request.RefId + request.UserId.ToString());
        //    //else //dc
        //    //    _dVSet = this.Redis.Get<EbDataVisualizationSet>(request.RefId);

        //    _dV.AfterRedisGet(this.Redis as RedisClient);

        //    string _sql = null;

        //    if (_dV.EbDataSource != null)
        //    {
        //        StringBuilder _sb = new StringBuilder();

        //        if (request.TFilters != null)
        //        {
        //            foreach (Dictionary<string, string> _dic in request.TFilters)
        //            {
        //                var op = _dic["o"]; var col = _dic["c"]; var val = _dic["v"];

        //                if (op == "x*")
        //                    _sb.Append(string.Format("LOWER({0})::text LIKE LOWER('{1}%') ", col, val));
        //                else if (op == "*x")
        //                    _sb.Append(string.Format("LOWER({0})::text LIKE LOWER('%{1}') ", col, val));
        //                else if (op == "*x*")
        //                    _sb.Append(string.Format("LOWER({0})::text LIKE LOWER('%{1}%') ", col, val));
        //                else if (op == "=")
        //                    _sb.Append(string.Format("LOWER({0}::text) = LOWER('{1}') ", col, val));
        //                else
        //                    _sb.Append(string.Format("{0} {1} '{2}' ", col, op, val));
        //            }
        //        }

        //        string __innerSql = _dV.EbDataSource.SqlDecoded();
        //        string _where = (_sb.Length > 0) ? "WHERE " + string.Join(" AND ", _sb) : string.Empty;
        //        string _orderby = (string.IsNullOrEmpty(request.OrderByCol)) ? "1" : string.Format("{0} {1}", request.OrderByCol, ((request.OrderByDir == 2) ? "DESC" : "ASC"));
        //        _sql = string.Format("WITH __OUTER99 AS ({0}) SELECT * FROM __OUTER99 {1} {2}", __innerSql, _where, _orderby);

        //        this.Log.Info("_ds *****" + _sql);
        //    }

        //    var parameters = new List<System.Data.Common.DbParameter>();
        //    bool _isPaged = (_sql.ToLower().Contains("@offset") && _sql.ToLower().Contains("@limit"));
        //    if (_isPaged)
        //    {
        //        parameters.AddRange(new System.Data.Common.DbParameter[]
        //        {
        //            this.EbConnectionFactory.ObjectsDB.GetNewParameter("@limit", EbDbTypes.Int32, request.Length),
        //            this.EbConnectionFactory.ObjectsDB.GetNewParameter("@offset", EbDbTypes.Int32, request.Start),
        //        });
        //    }

        //    if (request.Params != null)
        //    {
        //        foreach (Dictionary<string, string> param in request.Params)
        //            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(string.Format("{0}", param["name"]), (EbDbTypes)Convert.ToInt32(param["type"]), param["value"]));
        //    }

        //    var _dataset = _dV.DoQueries4DataVis(_sql, this.EbConnectionFactory, parameters.ToArray());

        //    //-- 
        //    int _recordsTotal = 0, _recordsFiltered = 0;
        //    if (_isPaged)
        //    {
        //        Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsTotal);
        //        Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsFiltered);
        //    }
        //    _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[0].Rows.Count;
        //    _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[0].Rows.Count;
        //    //-- 

        //    dsresponse = new DataSourceDataResponse
        //    {
        //        Draw = request.Draw,
        //        Data = (_dataset.Tables.Count > 1) ? _dataset.Tables[1].Rows : _dataset.Tables[0].Rows,
        //        RecordsTotal = _recordsTotal,
        //        RecordsFiltered = _recordsFiltered
        //    };
        //    this.Log.Info("dsresponse*****" + dsresponse.Data);

        //    return dsresponse;
        //}

        //[CompressResponse]
        //public DataSourceColumnsResponse Any(DataVisColumnsRequest request)
        //{
        //    EbDataVisualization _dV = request.EbDataVisualization;
        //    _dV.AfterRedisGet(this.Redis as RedisClient);
        //    var _ds = _dV.EbDataSource;

        //    string _dsRedisKey = string.Format("{0}_columns", _dV.DataSourceRefId);

        //    EbDataSet _dataset = null;
        //    bool _isPaged = false;
        //    DataSourceColumnsResponse resp = this.Redis.Get<DataSourceColumnsResponse>(_dsRedisKey);

        //    if (resp == null || resp.Columns == null || resp.Columns.Count == 0)
        //    {
        //        resp = new DataSourceColumnsResponse();
        //        resp.Columns = new List<ColumnColletion>();

        //        if (_ds != null)
        //        {
        //            Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns Sql: " + _ds.SqlDecoded());

        //            string _sql = _ds.SqlDecoded().Replace("@and_search", string.Empty).Replace("@orderby", "1");
        //            _isPaged = (_sql.ToLower().Contains("@offset") && _sql.ToLower().Contains("@limit"));

        //            var parameters = new List<System.Data.Common.DbParameter>();
        //            if (_isPaged)
        //            {
        //                parameters.AddRange(new System.Data.Common.DbParameter[]
        //                {
        //                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("@limit", EbDbTypes.Int32, 0),
        //                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("@offset", EbDbTypes.Int32, 0)
        //                });
        //            }

        //            if (request.Params != null)
        //            {
        //                foreach (Dictionary<string, string> param in request.Params)
        //                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param["name"]), (EbDbTypes)Convert.ToInt32(param["type"]), param["value"]));
        //            }

        //            Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns Parameters Added");

        //            try
        //            {
        //                _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray());

        //                foreach (var dt in _dataset.Tables)
        //                    resp.Columns.Add(dt.Columns);

        //                resp.IsPaged = _isPaged;
        //                this.Redis.Set<DataSourceColumnsResponse>(_dsRedisKey, resp);
        //            }
        //            catch (Exception e)
        //            {
        //                Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns e.Message: " + e.Message);
        //                this.Redis.Remove(_dsRedisKey);
        //            }
        //        }
        //    }

        //    return resp;
        //}

        public object Any(EbObjectWithRelatedDVRequest request)
        {
            EbDataVisualization dsobj = null;
            var myService = base.ResolveService<EbObjectService>();
            var res = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.Refid });
            dsobj = EbSerializers.Json_Deserialize(res.Data[0].Json);
            dsobj.Status = res.Data[0].Status;
            dsobj.VersionNumber = res.Data[0].VersionNumber;
            var _Tags = res.Data[0].Tags;
            var arr = _Tags.Split(",");
            foreach (var item in arr)
                _Tags = "'" + item + "',";
            _Tags = _Tags.Remove(_Tags.Length - 1);
            _Tags = _Tags.Remove(_Tags.Length - 1);
            _Tags = _Tags.Remove(0, 1);

            List<EbObjectWrapper> dvList = new List<EbObjectWrapper>();
            if (request.DsRefid != dsobj.DataSourceRefId)
            {
                var resultlist = (EbObjectRelationsResponse)myService.Get(new EbObjectRelationsRequest { DominantId = dsobj.DataSourceRefId });
                var rlist = resultlist.Data;
                foreach (var element in rlist)
                {
                    if (element.EbObjectType == (int)EbObjectTypes.TableVisualization || element.EbObjectType == (int)EbObjectTypes.ChartVisualization)
                    {
                        dvList.Add(element);
                    }
                }

            }

            List<EbObjectWrapper> dvTaggedList = new List<EbObjectWrapper>();
            if (request.Refid != null)
            {
                var resultlist = (EbObjectTaggedResponse)myService.Get(new EbObjectTaggedRequest { Tags = _Tags });
                var rlist = resultlist.Data;
                foreach (var element in rlist)
                {
                    if (element.EbObjectType == (int)EbObjectTypes.TableVisualization || element.EbObjectType == (int)EbObjectTypes.ChartVisualization)
                    {
                        dvTaggedList.Add(element);
                    }
                }

            }

            return new EbObjectWithRelatedDVResponse { Dsobj = dsobj, DvList = dvList, DvTaggedList = dvTaggedList };
        }

        [CompressResponse]
        public DataSourceDataResponse Any(TableDataRequest request)
        {
            this.Log.Info("data request");

            EbDataVisualization _dV = request.EbDataVisualization;

            DataSourceDataResponse dsresponse = null;

            var _ds = this.Redis.Get<EbDataReader>(request.RefId);

            if (_ds == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                Redis.Set<EbDataReader>(request.RefId, _ds);
            }
            if (_ds.FilterDialogRefId != string.Empty)
            {
                var _dsf = this.Redis.Get<EbFilterDialog>(_ds.FilterDialogRefId);
                if (_dsf == null)
                {
                    var myService = base.ResolveService<EbObjectService>();
                    var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = _ds.FilterDialogRefId });
                    _dsf = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbFilterDialog>(_ds.FilterDialogRefId, _dsf);
                }
                if (request.Params == null)
                    request.Params = _dsf.GetDefaultParams();
            }
            string _sql = string.Empty;
            string tempsql = string.Empty;

            if (_ds != null)
            {
                string _c = string.Empty;

                if (request.TFilters != null && request.TFilters.Count > 0)
                {
                    foreach (TFilters _dic in request.TFilters)
                    {
                        var op = _dic.Operator; var col = _dic.Column; var val = _dic.Value; var type = _dic.Type;
                        var array = _dic.Value.Split("|");
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
                                if (array[i].Trim() != "")
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

                if (!_ds.Sql.ToLower().Contains(":and_search"))
                {
                    _ds.Sql = "SELECT * FROM (" + _ds.Sql + ") data WHERE 1=1 :and_search order by :orderby";
                }
                _ds.Sql = _ds.Sql.ReplaceAll(";", string.Empty);
                _sql = _ds.Sql.Replace(":and_search", _c) + ";";
                //}
                if (request.Ispaging)
                {
                    var matches = Regex.Matches(_sql, @"\;\s*SELECT\s*COUNT\(\*\)\s*FROM");
                    if (matches.Count == 0)
                    {
                        tempsql = _sql.ReplaceAll(";", string.Empty);
                        tempsql = "SELECT COUNT(*) FROM (" + tempsql + ") data1;";
                    }

                    var sql1 = _sql.ReplaceAll(";", string.Empty);
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
                    _sql = sql1 + tempsql;
                }
            }
            bool _isPaged = false;

            string __order = string.Empty;
            if (request.OrderBy != null && request.OrderBy.Count > 0)
            {
                foreach (OrderBy order in request.OrderBy)
                {
                    __order += string.Format("{0} {1},", order.Column, (order.Direction == 2) ? "DESC" : "ASC");
                }
                int indx = __order.LastIndexOf(",");
                __order = __order.Substring(0, indx);
            }
            _sql = _sql.Replace(":orderby", (string.IsNullOrEmpty(__order)) ? "1" : __order);

            _isPaged = (_sql.ToLower().Contains(":offset") && _sql.ToLower().Contains(":limit"));

            if (request.Params == null)
                _sql = _sql.Replace(":id", "0");
            //}
            var parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, request.Length, request.Start);
            Console.WriteLine("Before :  " + DateTime.Now);
            var dtStart = DateTime.Now;
            Console.WriteLine("................................................datasourceDSrequeststart " + DateTime.Now);
            var _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());
            Console.WriteLine("................................................datasourceDSrequeststart " + DateTime.Now);
            var dtstop = DateTime.Now;
            Console.WriteLine("..................................totaltimeinSeconds" + dtstop.Subtract(dtStart).Seconds);

            //-- 
            Console.WriteLine(DateTime.Now);
            var dtEnd = DateTime.Now;
            var ts = (dtEnd - dtStart).TotalMilliseconds;
            Console.WriteLine("final:::" + ts);
            int _recordsTotal = 0, _recordsFiltered = 0;
            if (_isPaged)
            {
                Int32.TryParse(_dataset.Tables[_dataset.Tables.Count - 1].Rows[0][0].ToString(), out _recordsTotal);
                Int32.TryParse(_dataset.Tables[_dataset.Tables.Count - 1].Rows[0][0].ToString(), out _recordsFiltered);
            }
            _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[_dataset.Tables.Count - 1].Rows.Count;
            _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[_dataset.Tables.Count - 1].Rows.Count;
            //-- 
            EbDataTable _formattedDataTable = null;
            //LevelInfoCollection _levels = new LevelInfoCollection();
            //List<GroupingDetails> _levels = new List<GroupingDetails>();
            List<GroupingDetails> _levels = new List<GroupingDetails>();
            if (_dataset.Tables.Count > 0 && _dV != null)
            {
                _formattedDataTable = PreProcessing(ref _dataset, request.Params, _dV, request.UserInfo, ref _levels);
                //_levels = GetGroupInfo2(_dataset.Tables[0], _dV);
            }
            List<string> _permission = new List<string>();
            if (request.dvRefId != null)
                _permission = PermissionCheck(request.UserInfo, request.dvRefId);
            dsresponse = new DataSourceDataResponse
            {
                Draw = request.Draw,
                Data = _dataset.Tables[0].Rows,
                FormattedData = (_formattedDataTable != null) ? _formattedDataTable.Rows : null,
                RecordsTotal = _recordsTotal,
                RecordsFiltered = _recordsFiltered,
                Ispaged = _isPaged,
                Levels = _levels,
                Permission = _permission
            };
            this.Log.Info("dsresponse*****" + dsresponse.Data);
            var x = EbSerializers.Json_Serialize(dsresponse);
            return dsresponse;
        }

        [CompressResponse]
        public DataSourceColumnsResponse Any(TableColumnsRequest request)
        {
            string _dsRedisKey = string.Format("{0}_columns", request.RefId);
            EbDataSet _dataset = null;
            bool _isPaged = false;
            DataSourceColumnsResponse resp = null;
            //DataSourceColumnsResponse resp = this.Redis.Get<DataSourceColumnsResponse>(_dsRedisKey);

            if (resp == null || resp.Columns == null || resp.Columns.Count == 0)
            {
                resp = new DataSourceColumnsResponse();
                resp.Columns = new List<ColumnColletion>();
                //EbDataSource _ds = null;
                var _ds = this.Redis.Get<EbDataReader>(request.RefId);
                if (_ds == null)
                {
                    var myService = base.ResolveService<EbObjectService>();
                    var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                    _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbDataReader>(request.RefId, _ds);
                }
                if (_ds.FilterDialogRefId != string.Empty)
                {
                    var _dsf = this.Redis.Get<EbFilterDialog>(_ds.FilterDialogRefId);
                    if (_dsf == null)
                    {
                        var myService = base.ResolveService<EbObjectService>();
                        var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = _ds.FilterDialogRefId });
                        _dsf = EbSerializers.Json_Deserialize(result.Data[0].Json);
                        Redis.Set<EbFilterDialog>(_ds.FilterDialogRefId, _dsf);
                    }
                    if (request.Params == null)
                        request.Params = _dsf.GetDefaultParams();
                }

                if (_ds != null)
                {
                    string _sql = string.Empty;

                    _sql = _ds.Sql/*Decoded()*/.Replace(":and_search", string.Empty).Replace(":orderby", "1");
                    _isPaged = (_sql.ToLower().Contains(":offset") && _sql.ToLower().Contains(":limit"));


                    if (request.Params == null)
                        _sql = _sql.Replace(":id", "0");
                    //}
                    var parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, 0, 0);

                    try
                    {
                        Console.WriteLine("................................................datasourcecolumnrequeststart " + System.DateTime.Now);
                        _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());

                        Console.WriteLine("................................................datasourcecolumnrequestfinish " + System.DateTime.Now);

                        foreach (var dt in _dataset.Tables)
                            resp.Columns.Add(dt.Columns);

                        resp.IsPaged = _isPaged;
                        this.Redis.Set<DataSourceColumnsResponse>(_dsRedisKey, resp);
                    }
                    catch (Exception e)
                    {
                        Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns e.Message: " + e.Message);
                        this.Redis.Remove(_dsRedisKey);
                    }
                }
            }

            return resp;
        }

        public void CustomColumDoCalc(ref EbDataSet _dataset, List<Param> Parameters, EbDataVisualization _dv, User _user, ref List<GroupingDetails> _levels)
        {
            dynamic result = null;

            foreach (DVBaseColumn col in _dv.Columns)
            {
                if (col.IsCustomColumn)
                    _dataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = col.Data, ColumnName = col.Name, Type = col.Type });
            }

            Globals globals = new Globals();
            if (Parameters != null)
            {
                foreach (Param p in Parameters)
                {
                    globals["Params"].Add(p.Name, new NTV { Name = p.Name, Type = (EbDbTypes)Convert.ToInt32(p.Type), Value = p.ValueTo });
                }
            }

            var __customCols = _dv.Columns.Where(c => (c.IsCustomColumn == true || !string.IsNullOrEmpty(c.Formula))).ToList();

            for (int i = 0; i < _dataset.Tables[0].Rows.Count; i++)
            {
                foreach (DVBaseColumn customCol in __customCols)
                {
                    try
                    {
                        foreach (FormulaPart formulaPart in customCol.FormulaParts)
                        {
                            object __value = null;
                            var __partType = _dataset.Tables[0].Columns[formulaPart.FieldName].Type;
                            if (__partType == EbDbTypes.Decimal || __partType == EbDbTypes.Int32)
                                __value = (_dataset.Tables[0].Rows[i][formulaPart.FieldName] != DBNull.Value) ? _dataset.Tables[0].Rows[i][formulaPart.FieldName] : 0;
                            else
                                __value = _dataset.Tables[0].Rows[i][formulaPart.FieldName];

                            globals[formulaPart.TableName].Add(formulaPart.FieldName, new NTV { Name = formulaPart.FieldName, Type = __partType, Value = __value });
                        }
                    }
                    catch (Exception e)
                    {
                        Log.Info("c# Script Exception........." + e.StackTrace);
                    }

                    try
                    {
                        if (customCol is DVNumericColumn)
                            result = Convert.ToDecimal(customCol.GetCodeAnalysisScript().RunAsync(globals).Result.ReturnValue);
                        else
                            result = customCol.GetCodeAnalysisScript().RunAsync(globals).Result.ReturnValue.ToString();
                    }
                    catch (Exception e)
                    {
                        Log.Info("c# Script Exception........." + e.StackTrace);
                    }

                    _dataset.Tables[0].Rows[i][customCol.Name] = result;
                }
            }
        }

        public EbDataTable PreProcessing(ref EbDataSet _dataset, List<Param> Parameters, EbDataVisualization _dv, User _user, ref List<GroupingDetails> _levels)
        {
            var _user_culture = CultureInfo.GetCultureInfo(_user.Preference.Locale);
            var colCount = _dataset.Tables[0].Columns.Count;
            if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.ORACLE && _dv.IsPaging)
            {
                _dataset.Tables[0].Columns.RemoveAt(colCount - 1);// rownum deleted for oracle
                for (int i = 0; i < _dataset.Tables[0].Rows.Count; i++)
                {
                    _dataset.Tables[0].Rows[i].RemoveAt(colCount - 1);
                }
            }

            this.CustomColumDoCalc(ref _dataset, Parameters, _dv, _user, ref _levels);

            EbDataTable _formattedTable = _dataset.Tables[0].GetEmptyTable();

            for (int i = 0; i < _dataset.Tables[0].Rows.Count; i++)
            {
                _formattedTable.Rows.Add(_formattedTable.NewDataRow2());
                foreach (DVBaseColumn col in _dv.Columns)
                {
                    var cults = col.GetColumnCultureInfo(_user_culture);
                    object _unformattedData = _dataset.Tables[0].Rows[i][col.Data];
                    object _formattedData = _unformattedData;

                    if (col.Type == EbDbTypes.Date)
                    {
                        _unformattedData = (_unformattedData == DBNull.Value) ? DateTime.MinValue : _unformattedData;
                        _formattedData = (((DateTime)_unformattedData).Date != DateTime.MinValue) ? Convert.ToDateTime(_unformattedData).ToString("d", cults.DateTimeFormat) : string.Empty;
                        _dataset.Tables[0].Rows[i][col.Data] = Convert.ToDateTime(_unformattedData).ToString("yyyy-MM-dd");
                    }
                    else if (col.Type == EbDbTypes.Decimal || col.Type == EbDbTypes.Int32)
                        _formattedData = Convert.ToDecimal(_unformattedData).ToString("N", cults.NumberFormat);


                    if (!string.IsNullOrEmpty(col.LinkRefId))
                    {
                        if (col.LinkType == LinkTypeEnum.Popout)
                            _formattedData = "<a href='#' oncontextmenu='return false' class ='tablelink' data-link='" + col.LinkRefId + "'>" + _formattedData + "</a>";
                        else if (col.LinkType == LinkTypeEnum.Inline)
                            _formattedData = _formattedData + "&nbsp; <a href = '#' oncontextmenu = 'return false' class ='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-inline='true' data-data='" + _formattedData + "'><i class='fa fa-caret-down'></i></a>";
                        else if (col.LinkType == LinkTypeEnum.Both)
                            _formattedData = "<a href='#' oncontextmenu='return false' class ='tablelink' data-link='" + col.LinkRefId + "'>" + _formattedData + "</a>" + "&nbsp; <a href ='#' oncontextmenu='return false' class='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-inline='true' data-data='" + _formattedData + "'> <i class='fa fa-caret-down'></i></a>";
                    }
                    if (col.Type == EbDbTypes.String && (col as DVStringColumn).RenderAs == StringRenderType.Link && col.LinkType == LinkTypeEnum.Tab)/////////////////
                    {
                        _formattedData = "<a href='../leadmanagement/" + _dataset.Tables[0].Rows[i][0] + "' target='_blank'>" + _formattedData + "</a>";
                    }
                    if (!_user.Roles.Contains(SystemRoles.SolutionOwner.ToString()) && !_user.Roles.Contains(SystemRoles.SolutionAdmin.ToString()))
                    {
                        if (col.HideDataRowMoreThan > 0 && col.HideDataRowMoreThan < _dataset.Tables[0].Rows.Count)
                        {
                            _formattedData = "********";
                        }
                    }
                    _formattedTable.Rows[i][col.Data] = _formattedData;

                }
            }
            //List<LevelDetails> Levels;
            if ((_dv as EbTableVisualization) != null)
            {
                if ((_dv as EbTableVisualization).RowGroupCollection.Count > 0)
                {
                    if ((_dv as EbTableVisualization).CurrentRowGroup.GetType().Name == "SingleLevelRowGroup")
                        //_levels = GetGroupInfoSingleLevel(_dataset.Tables[0], _dv, _user_culture);
                        _levels = RowGroupingCommon(_dataset.Tables[0], _dv, _user_culture, false);
                    else if ((_dv as EbTableVisualization).CurrentRowGroup.GetType().Name == "MultipleLevelRowGroup")
                        //_levels = GetGroupInfoMultiLevel(_dataset.Tables[0], _dv, _user_culture);//GetGroupInfoMultiLevel(_dataset.Tables[0], _dv, _user_culture);
                        _levels = RowGroupingCommon(_dataset.Tables[0], _dv, _user_culture, true);
                }
            }


            return _formattedTable;
        }

        public List<GroupingDetails> RowGroupingCommon(EbDataTable Table, EbDataVisualization Visualization, CultureInfo Culture, bool IsMultiLevelRowGrouping = false)
        {
            Dictionary<string, GroupingDetails> RowGrouping = new Dictionary<string, GroupingDetails>();

            int TotalLevels = (IsMultiLevelRowGrouping) ? (Visualization as EbTableVisualization).CurrentRowGroup.RowGrouping.Count : 1,
            CurSortIndex = 0;

            List<int> AggregateColumnIndexes = GetAggregateIndexes(Visualization.Columns);
            List<DVBaseColumn> RowGroupingColumns = new List<DVBaseColumn>((Visualization as EbTableVisualization).CurrentRowGroup.RowGrouping);
            int ColCount = Visualization.Columns.Count;
            string PreviousGroupingText = string.Empty;

            for (int i = 0; i < Table.Rows.Count; i++)
            {
                CurSortIndex += TotalLevels + 30;

                EbDataRow currentRow = Table.Rows[i];
                int delimCount = 1;
                string TempGroupingText = CreateCollectionKey(currentRow, IsMultiLevelRowGrouping, BlankText, TotalLevels, RowGroupingColumns, i, ref delimCount);

                if (TempGroupingText.Equals(PreviousGroupingText) == false)
                {
                    CreateHeaderAndFooterPairs(currentRow, AggregateColumnIndexes, RowGroupingColumns, RowGrouping, Visualization.Columns, TotalLevels, IsMultiLevelRowGrouping, Culture, TempGroupingText, ref CurSortIndex, ColCount);

                    HeaderGroupingDetails HeaderObject = RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails;
                    HeaderObject.SetRowIndex(i);
                    HeaderObject.InsertionType = BeforeText;

                    (RowGrouping[FooterPrefix + TempGroupingText] as FooterGroupingDetails).InsertionType = BeforeText;

                    if (i > 0)
                    {
                        (RowGrouping[FooterPrefix + PreviousGroupingText] as FooterGroupingDetails).SetRowIndex(i);

                        if (IsMultiLevelRowGrouping && i == Table.Rows.Count - 1 &&
                            (RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails).GroupingCount == 1 &&
                            (RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails).LevelCount == 0)
                        {
                            SetFinalFooterRow(currentRow, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, i, TempGroupingText, CurSortIndex);
                        }
                    }
                }
                else
                {
                    (RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails).GroupingCount++;
                    if (i == Table.Rows.Count - 1)
                    {
                        SetFinalFooterRow(currentRow, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, i, TempGroupingText, CurSortIndex);
                    }
                }

                (RowGrouping[FooterPrefix + TempGroupingText] as FooterGroupingDetails).SetValue(currentRow);

                PreviousGroupingText = TempGroupingText;
            }
            List<GroupingDetails> SortedGroupings = RowGrouping.Values.ToList();
            SortedGroupings.Sort();
            return SortedGroupings;
        }

        private void SetFinalFooterRow(EbDataRow currentRow, List<DVBaseColumn> rowGroupingColumns, bool IsMultiLevelRowGrouping, Dictionary<string, GroupingDetails> RowGrouping, int i, string TempGroupingText, int CurSortIndex)
        {
            List<string> GroupingKeys = CreateRowGroupingKeys(currentRow, rowGroupingColumns, IsMultiLevelRowGrouping);
            (RowGrouping[FooterPrefix + TempGroupingText] as FooterGroupingDetails).InsertionType = AfterText;
            (RowGrouping[FooterPrefix + TempGroupingText] as FooterGroupingDetails).SetRowIndex(i);
            if (IsMultiLevelRowGrouping)
            {
                GroupingKeys.Reverse();
                foreach (string key in GroupingKeys)
                {
                    (RowGrouping[FooterPrefix + key] as FooterGroupingDetails).InsertionType = AfterText;
                    (RowGrouping[FooterPrefix + key] as FooterGroupingDetails).SetRowIndex(i);
                    (RowGrouping[FooterPrefix + key] as FooterGroupingDetails).SetSortIndex(CurSortIndex);
                }



                //FooterGroupingDetails finalFooter = (RowGrouping[FooterPrefix + TempGroupingText] as FooterGroupingDetails);//.SetRowIndex(i);
                FooterGroupingDetails finalFooter = RowGrouping[FooterPrefix + TempGroupingText.Split(GroupDelimiter)[0]] as FooterGroupingDetails;
                //finalFooter.InsertionType = AfterText;
                //finalFooter.SetRowIndex(i);
                finalFooter.SetSortIndex(1);
            }
        }

        private List<int> GetAggregateIndexes(DVColumnCollection VisualizationColumns)
        {
            List<int> AggregateColumnIndexes = new List<int>();
            foreach (DVBaseColumn _column in VisualizationColumns)
            {
                if (_column is DVNumericColumn && (_column as DVNumericColumn).Aggregate)
                    AggregateColumnIndexes.Add(_column.Data);
            }
            return AggregateColumnIndexes;
        }

        private string CreateCollectionKey(EbDataRow row, bool IsMultiLevelRowGrouping, string BlankText, int TotalLevels, List<DVBaseColumn> RowGroupingColumns, int i, ref int delimCount)
        {
            string TempGroupingText = string.Empty;
            foreach (DVBaseColumn Column in RowGroupingColumns)
            {
                string tempValue = row[Column.Data].ToString().Trim();
                TempGroupingText += (tempValue.Trim().Equals(string.Empty) || tempValue.Trim().IsNullOrEmpty()) ? BlankText : tempValue.Trim();
                TempGroupingText += (IsMultiLevelRowGrouping && delimCount == TotalLevels) ? string.Empty : GroupDelimiter;

                if (IsMultiLevelRowGrouping)
                    delimCount++;
            }

            if (!IsMultiLevelRowGrouping)
                TempGroupingText = TempGroupingText.Substring(0, TempGroupingText.Length - 3);

            return TempGroupingText;
        }

        private void CreateHeaderAndFooterPairs(EbDataRow CurrentRow, List<int> AggregateIndexes,
            List<DVBaseColumn> _rowGroupingColumns, Dictionary<string, GroupingDetails> rowGrouping, DVColumnCollection VisualizationColumns,
            int TotalLevels, bool IsMultiLevelGrouping, CultureInfo culture, string TempGroupingText, ref int CurSortIndex, int ColumnCount)
        {
            List<string> TempKey = CreateRowGroupingKeys(CurrentRow, _rowGroupingColumns, (TotalLevels > 1) ? true : false);
            if (IsMultiLevelGrouping)
            {
                for (int j = 0; j < TotalLevels; j++)
                {
                    string headerKey = HeaderPrefix + TempKey[j];
                    string footerKey = FooterPrefix + TempKey[j];

                    if (!rowGrouping.ContainsKey(headerKey))
                    {
                        rowGrouping.Add(headerKey, new HeaderGroupingDetails { CollectionKey = headerKey, RowGrouping = rowGrouping, RowGroupingColumns = _rowGroupingColumns });
                        rowGrouping.Add(footerKey, new FooterGroupingDetails(TotalLevels, AggregateIndexes, VisualizationColumns, culture) { CollectionKey = footerKey, RowGrouping = rowGrouping });

                        rowGrouping[headerKey].GroupingCount++;
                        rowGrouping[headerKey].ColumnCount = ColumnCount;
                        (rowGrouping[headerKey] as HeaderGroupingDetails).TotalLevels = TotalLevels;
                        rowGrouping[headerKey].IsMultiLevel = IsMultiLevelGrouping;
                        rowGrouping[footerKey].IsMultiLevel = IsMultiLevelGrouping;
                    }
                }
                (rowGrouping[HeaderPrefix + TempKey[TotalLevels - 1]] as HeaderGroupingDetails).SetSortIndex(CurSortIndex);
                CurSortIndex += TotalLevels + 1;
                (rowGrouping[FooterPrefix + TempKey[TotalLevels - 1]] as FooterGroupingDetails).SetSortIndex(CurSortIndex);
            }
            else
            {
                if (!rowGrouping.ContainsKey(HeaderPrefix + TempKey.Last()))
                {
                    string headerKey = HeaderPrefix + TempKey.Last();
                    string footerKey = FooterPrefix + TempKey.Last();

                    rowGrouping.Add(headerKey, new HeaderGroupingDetails { CollectionKey = headerKey, RowGrouping = rowGrouping, RowGroupingColumns = _rowGroupingColumns });
                    rowGrouping.Add(footerKey, new FooterGroupingDetails(TotalLevels, AggregateIndexes, VisualizationColumns, culture) { CollectionKey = footerKey, RowGrouping = rowGrouping });
                    (rowGrouping[headerKey] as HeaderGroupingDetails).ColumnCount = ColumnCount;
                    (rowGrouping[headerKey] as HeaderGroupingDetails).SetSortIndex(CurSortIndex);
                    (rowGrouping[footerKey] as FooterGroupingDetails).SetSortIndex(++CurSortIndex);
                }
            }
        }

        private static List<string> CreateRowGroupingKeys(EbDataRow CurrentRow, List<DVBaseColumn> RowGroupingColumns, bool IsMultiLevelRowGrouping)
        {
            List<string> TempKey = new List<string>();
            string TempStr = string.Empty;
            foreach (var column in RowGroupingColumns)
            {
                if (IsMultiLevelRowGrouping)
                {
                    TempKey.Add(((TempKey.Count > 0) ? TempKey.Last() + GroupDelimiter : string.Empty) + ((CurrentRow[column.Data].ToString().Trim().IsNullOrEmpty()) ? BlankText : CurrentRow[column.Data].ToString().Trim()));
                }
                else
                {
                    TempStr += (TempStr.Equals(string.Empty)) ? CurrentRow[column.Data] : GroupDelimiter + ((CurrentRow[column.Data].ToString().Trim().IsNullOrEmpty()) ? BlankText : CurrentRow[column.Data].ToString().Trim());
                }
            }

            if (!IsMultiLevelRowGrouping)
                TempKey.Add(TempStr);

            return TempKey;
        }

        public int GetCurrentLevel(string CurrentString, string PreviousString, bool isEnd, int currentRowIndex, int totalLevels, bool IsMultiLevel)
        {
            if (IsMultiLevel)
            {
                if (!isEnd)
                {
                    string[] CurrentSplit = CurrentString.Split(GroupDelimiter);
                    string[] PreviousSplit = PreviousString.Split(GroupDelimiter);
                    if (CurrentSplit.Length == PreviousSplit.Length)
                    {
                        for (int i = 0; i < CurrentSplit.Length; i++)
                        {
                            if (!CurrentSplit[i].Equals(PreviousSplit[i]))
                                return i;
                        }
                    }
                    else
                    {
                        return -1;
                    }
                }
                return (currentRowIndex == 0) ? totalLevels - 1 : 0;
            }

            return 1;
        }

        public List<string> PermissionCheck(User _user, string refId)
        {
            List<string> permList = new List<string>();
            var x = refId.Split("-");
            var objid = x[3].PadLeft(5,'0');
            List<string> liteperm = new List<string>();
            if (_user.Roles.Contains(SystemRoles.SolutionOwner.ToString()) || _user.Roles.Contains(SystemRoles.SolutionAdmin.ToString()))
            {
                foreach (var _eboperation in (TVOperations.Instance as EbOperations).Enumerator)
                {
                    if (_eboperation.ToString() == OperationConstants.EXCEL_EXPORT)
                    {
                        permList.Add("Excel");
                    }
                    else if (_eboperation.ToString() == OperationConstants.SUMMARIZE)
                    {
                        permList.Add("SUMMARIZE");
                    }
                    else if (_eboperation.ToString() == OperationConstants.PDF_EXPORT)
                    {
                        permList.Add("Pdf");
                    }
                }
            }
            else
            {
                foreach (var _permission in _user.Permissions)
                {
                    liteperm.Add(_permission.Substring(4, 11));
                }
                foreach (var _eboperation in (TVOperations.Instance as EbOperations).Enumerator)
                {
                    var _perm = EbObjectTypes.TableVisualization.IntCode.ToString().PadLeft(2, '0') + "-" + objid + "-" + _eboperation.IntCode.ToString().PadLeft(2, '0');
                    if (liteperm.Contains(_perm))
                    {
                        if (_eboperation.ToString() == OperationConstants.EXCEL_EXPORT)
                        {
                            permList.Add("Excel");
                        }
                    }
                }
            }
            return permList;
        }

        [CompressResponse]
        public DataSourceDataResponse Post(InlineTableDataRequest request)
        {
            DataSourceDataResponse dsresponse = null;
            EbDataVisualization _dV = request.EbDataVisualization;
            var _ds = this.Redis.Get<EbDataReader>(request.RefId);
            string _sql = string.Empty;

            if (_ds == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                Redis.Set<EbDataReader>(request.RefId, _ds);
            }
            if (_ds.FilterDialogRefId != string.Empty && _ds.FilterDialogRefId != null)
            {
                var _dsf = this.Redis.Get<EbFilterDialog>(_ds.FilterDialogRefId);
                if (_dsf == null)
                {
                    var myService = base.ResolveService<EbObjectService>();
                    var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = _ds.FilterDialogRefId });
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
                _sql = _ds.Sql;
            }
            var parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, request.Length, request.Start);
            Console.WriteLine("Before :  " + DateTime.Now);
            var dtStart = DateTime.Now;
            Console.WriteLine("................................................datasourceDSrequeststart " + DateTime.Now);
            var _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());
            Console.WriteLine("................................................datasourceDSrequeststart " + DateTime.Now);
            var dtstop = DateTime.Now;
            Console.WriteLine("..................................totaltimeinSeconds" + dtstop.Subtract(dtStart).Seconds);

            //-- 
            Console.WriteLine(DateTime.Now);
            var dtEnd = DateTime.Now;
            var ts = (dtEnd - dtStart).TotalMilliseconds;
            Console.WriteLine("final:::" + ts);
            int _recordsTotal = 0, _recordsFiltered = 0;
            if (_isPaged)
            {
                Int32.TryParse(_dataset.Tables[1].Rows[0][0].ToString(), out _recordsTotal);
                Int32.TryParse(_dataset.Tables[1].Rows[0][0].ToString(), out _recordsFiltered);
            }
            _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[1].Rows.Count;
            _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[1].Rows.Count;
            //-- 
            //EbDataTable _formattedDataTable = null;
            //List<GroupingDetails> _levels = new List<GroupingDetails>();
            //if (_dataset.Tables.Count > 0 && _dV != null)
            //{
            //    _formattedDataTable = PreProcessing(ref _dataset, request.Params, _dV, request.UserInfo, ref _levels);
            //    //_levels = GetGroupInfo2(_dataset.Tables[0], _dV);
            //}
            dsresponse = new DataSourceDataResponse
            {
                Data = _dataset.Tables[0].Rows,
                //FormattedData = (_formattedDataTable != null) ? _formattedDataTable.Rows : null,
                RecordsTotal = _recordsTotal,
                RecordsFiltered = _recordsFiltered,
                Ispaged = _isPaged
            };
            this.Log.Info("dsresponse*****" + dsresponse.Data);
            var x = EbSerializers.Json_Serialize(dsresponse);
            return dsresponse;
        }
    }
}

