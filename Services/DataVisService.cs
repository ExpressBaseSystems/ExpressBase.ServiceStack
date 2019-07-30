using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects;
using ExpressBase.Objects.Objects.DVRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using ServiceStack;
using ServiceStack.Logging;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using OfficeOpenXml;
using System.IO;
using Newtonsoft.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using System.Threading.Tasks;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Singletons;
using ExpressBase.Common.Helpers;
using ExpressBase.Common.LocationNSolution;

namespace ExpressBase.ServiceStack
{
    [Authenticate]
    public class DataVisService : EbBaseService
    {
        private const string HeaderPrefix = "H_", FooterPrefix = "F_", GroupDelimiter = ":-:", AfterText = "After", BeforeText = "Before", BlankText = "(Blank)";

        private Eb_Solution _ebSolution = null;

        private bool _replaceEbColumns = true;


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

        public object Any(UpdateTreeColumnRequest request)
        {
            var _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(request.sql);
            return new object();
        }

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
            try
            {
                this.Log.Info("data request");

                EbDataVisualization _dV = request.EbDataVisualization;

                DataSourceDataResponse dsresponse = null;
                this._replaceEbColumns = request.ReplaceEbColumns;

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
                            var op = _dic.Operator.Trim(); var col = _dic.Column; var val = _dic.Value; var type = _dic.Type;
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
                        _ds.Sql = "SELECT * FROM (" + _ds.Sql + "\n ) data WHERE 1=1 :and_search order by :orderby";
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
                        __order += string.Format("{0} {1},", order.Column, (order.Direction == 1) ? "DESC" : "ASC");
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
                Console.WriteLine("................................................dataviz datarequest start " + DateTime.Now);
                var _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());
                Console.WriteLine("................................................dataviz datarequest end " + DateTime.Now);
                var dtstop = DateTime.Now;
                Console.WriteLine("..................................totaltimeinSeconds" + dtstop.Subtract(dtStart).Seconds);
                if (GetLogEnabled(request.RefId))
                {
                    TimeSpan T = _dataset.EndTime - _dataset.StartTime;
                    InsertExecutionLog(_dataset.RowNumbers, T, _dataset.StartTime, request.UserId, request.Params, request.RefId);
                }
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
                PrePrcessorReturn ReturnObj = new PrePrcessorReturn();
                List<GroupingDetails> _levels = new List<GroupingDetails>();
                object xx = new object();
                if (_dataset.Tables.Count > 0 && _dV != null)
                {
                    _ebSolution = request.eb_Solution;
                    ReturnObj = PreProcessing(ref _dataset, request.Params, _dV, request.UserInfo, ref _levels, request.IsExcel);
                }

                List<string> _permission = new List<string>();
                if (request.dvRefId != null)
                    _permission = PermissionCheck(request.UserInfo, request.dvRefId);
                dsresponse = new DataSourceDataResponse
                {
                    Draw = request.Draw,
                    Data = (ReturnObj.rows != null) ? ReturnObj.rows : _dataset.Tables[0].Rows,
                    FormattedData = (ReturnObj.FormattedTable != null) ? ReturnObj.FormattedTable.Rows : null,
                    RecordsTotal = _recordsTotal,
                    RecordsFiltered = _recordsFiltered,
                    Ispaged = _isPaged,
                    Levels = _levels,
                    Permission = _permission,
                    Summary = ReturnObj.Summary,
                    excel_file = ReturnObj.excel_file,
                    TableName = _dataset.Tables[0].TableName,
                    Tree = ReturnObj.tree
                };
                this.Log.Info(" dataviz dataresponse*****" + dsresponse.Data);
                var x = EbSerializers.Json_Serialize(dsresponse);
                return dsresponse;
            }
            catch(Exception e)
            {
                Log.Info("Datviz service Exception........." + e.StackTrace);
            }
            return null;
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

                        if (GetLogEnabled(request.RefId))
                        {
                            TimeSpan T = _dataset.EndTime - _dataset.StartTime;
                            InsertExecutionLog(_dataset.RowNumbers, T, _dataset.StartTime, request.UserId, request.Params, request.RefId);
                        }
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

        public void PreCustomColumDoCalc(ref EbDataSet _dataset, List<Param> Parameters, EbDataVisualization _dv, Globals globals)
        {
            foreach (DVBaseColumn col in _dv.Columns)
            {
                if (col.IsCustomColumn)
                    _dataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = col.Data, ColumnName = col.Name, Type = col.Type });
            }

            if (Parameters != null)
            {
                foreach (Param p in Parameters)
                {
                    globals["Params"].Add(p.Name, new NTV { Name = p.Name, Type = (EbDbTypes)Convert.ToInt32(p.Type), Value = p.ValueTo });
                }
            }
        }

        public void CustomColumDoCalc4Row(EbDataRow _datarow, EbDataVisualization _dv, Globals globals, DVBaseColumn customCol)
        {
            dynamic result = null;

            try
            {
                foreach (FormulaPart formulaPart in customCol.FormulaParts)
                {
                    object __value = null;
                    var __partType = _datarow.Table.Columns[formulaPart.FieldName].Type;
                    if (__partType == EbDbTypes.Decimal || __partType == EbDbTypes.Int32)
                        __value = (_datarow[formulaPart.FieldName] != DBNull.Value) ? _datarow[formulaPart.FieldName] : 0;
                    else
                        __value = _datarow[formulaPart.FieldName];

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
                else if (customCol is DVBooleanColumn)
                    result = Convert.ToBoolean(customCol.GetCodeAnalysisScript().RunAsync(globals).Result.ReturnValue);
                else
                    result = customCol.GetCodeAnalysisScript().RunAsync(globals).Result.ReturnValue.ToString();
            }
            catch (Exception e)
            {
                Log.Info("c# Script Exception........." + e.StackTrace);
            }

            _datarow[customCol.Name] = result;
        }

        public PrePrcessorReturn PreProcessing(ref EbDataSet _dataset, List<Param> Parameters, EbDataVisualization _dv, User _user, ref List<GroupingDetails> _levels, Boolean _isexcel)
        {
            try
            {
                var _user_culture = CultureHelper.GetSerializedCultureInfo(_user.Preference.Locale).GetCultureInfo();

                var colCount = _dataset.Tables[0].Columns.Count;
                if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.ORACLE && _dv.IsPaging)
                {
                    _dataset.Tables[0].Columns.RemoveAt(colCount - 1);// rownum deleted for oracle
                    for (int i = 0; i < _dataset.Tables[0].Rows.Count; i++)
                    {
                        _dataset.Tables[0].Rows[i].RemoveAt(colCount - 1);
                    }
                }

                Globals globals = new Globals();
                this.PreCustomColumDoCalc(ref _dataset, Parameters, _dv, globals);

                EbDataTable _formattedTable = _dataset.Tables[0].GetEmptyTable();
                _formattedTable.Columns.Add(_formattedTable.NewDataColumn(_dv.Columns.Count, "serial", EbDbTypes.Int32));
                Dictionary<int, List<object>> Summary = new Dictionary<int, List<object>>();

                bool bObfuscute = (!_user.Roles.Contains(SystemRoles.SolutionOwner.ToString()) && !_user.Roles.Contains(SystemRoles.SolutionAdmin.ToString()));
                bool isRowgrouping = false;
                bool IsMultiLevelRowGrouping = false;
                Dictionary<string, GroupingDetails> RowGrouping = new Dictionary<string, GroupingDetails>();
                int TotalLevels = 0, CurSortIndex = 0;
                List<int> AggregateColumnIndexes = GetAggregateIndexes(_dv.Columns);
                List<DVBaseColumn> RowGroupingColumns = new List<DVBaseColumn>();
                int dvColCount = _dv.Columns.Count;
                string PreviousGroupingText = string.Empty;
                int SerialCount = 0, PrevRowIndex = 0;
                bool isTree = false;
                FileInfo file = null;
                ExcelPackage package = null;
                ExcelWorksheet worksheet = null;
                byte[] bytes = null;

                TreeData<EbDataRow> tree = new TreeData<EbDataRow>();
                List<DVBaseColumn> dependencyTable = this.CreateDependencyTable(_dv);

                RowColletion rows = _dataset.Tables[0].Rows;
                if ((_dv as EbTableVisualization) != null)
                {
                    if ((_dv as EbTableVisualization).RowGroupCollection.Count > 0 && (_dv as EbTableVisualization).CurrentRowGroup.RowGrouping.Count > 0 && !(_dv as EbTableVisualization).DisableRowGrouping)
                    {
                        isRowgrouping = true;
                        RowGroupingColumns = (_dv as EbTableVisualization).CurrentRowGroup.RowGrouping;
                        if ((_dv as EbTableVisualization).CurrentRowGroup.GetType().Name == "SingleLevelRowGroup")
                            TotalLevels = 1;
                        else if ((_dv as EbTableVisualization).CurrentRowGroup.GetType().Name == "MultipleLevelRowGroup")
                        {
                            TotalLevels = (_dv as EbTableVisualization).CurrentRowGroup.RowGrouping.Count;
                            IsMultiLevelRowGrouping = true;
                        }
                    }
                    string sFileName = _dv.DisplayName + ".xlsx";

                    if (_isexcel)
                    {
                        file = PreExcelCalculation(sFileName);
                        package = new ExcelPackage(file);
                        worksheet = package.Workbook.Worksheets.Add("Report");
                        PreExcelAddHeader(ref worksheet, _dv);
                    }

                    var Treecol = this.Check4Tree((_dv as EbTableVisualization));
                    if (Treecol != null)
                    {
                        isTree = true;
                        tree = TreeGeneration(_formattedTable, _dataset.Tables[0], Treecol);
                        rows = (tree.RowsOrdered as RowColletion);
                        int i = 0;
                        foreach (Node<EbDataRow> Nodedr in tree.Tree)
                        {
                            DataTable2FormatedTable(Nodedr.Item, _dv, dependencyTable, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, ref worksheet, i, rows.Count, Nodedr.IsGroup, Nodedr.Level, isTree);
                            if (Nodedr.Children.Count > 0)
                            {
                                RecursiveGetTreeChilds(Nodedr, _dv, dependencyTable, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, ref worksheet, ref i, rows.Count, isTree);
                            }
                            i++;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < rows.Count; i++)
                        {
                            DataTable2FormatedTable(rows[i], _dv, dependencyTable, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, ref worksheet, i, rows.Count);
                            if (isRowgrouping)
                                DoRowGroupingCommon(rows[i], _dv, dependencyTable, _user_culture, _user, ref _formattedTable, IsMultiLevelRowGrouping, ref RowGrouping, ref PreviousGroupingText, ref CurSortIndex, ref SerialCount, i, dvColCount, TotalLevels, ref AggregateColumnIndexes, ref RowGroupingColumns, rows.Count);
                        }
                    }

                    List<GroupingDetails> SortedGroupings = RowGrouping.Values.ToList();
                    SortedGroupings.Sort();
                    _levels = SortedGroupings;
                    if (_isexcel)
                        bytes = package.GetAsByteArray();
                }
                else
                {
                    if ((_dv as EbChartVisualization) != null)
                    {
                        for (int i = 0; i < rows.Count; i++)
                        {
                            DataTable2FormatedTable(rows[i], _dv, dependencyTable, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, ref worksheet, i, rows.Count);

                        }
                    }
                }
                return new PrePrcessorReturn { FormattedTable = _formattedTable, Summary = Summary, excel_file = bytes, rows = rows, tree = tree.Tree };
            }
            catch (Exception e)
            {
                Log.Info("Before PreProcessing in datatable  Exception........." + e.StackTrace);
            }
            return null;
        }

        public List<DVBaseColumn> CreateDependencyTable(EbDataVisualization _dv)
        {
            List<DVBaseColumn> noncustom = _dv.Columns.Where(item => !item.IsCustomColumn).ToList<DVBaseColumn>();
            List<DVBaseColumn> custom = _dv.Columns.Where(item => item.IsCustomColumn).ToList<DVBaseColumn>();
            List<DVBaseColumn> Columns = new List<DVBaseColumn>(noncustom);
            foreach (DVBaseColumn col in custom)
            {
                List<DVBaseColumn> curcustom = new List<DVBaseColumn>();

                foreach (FormulaPart formulaPart in col.FormulaParts)
                {
                    List<DVBaseColumn> curObj = noncustom.Where(item => item.Name == formulaPart.FieldName).ToList<DVBaseColumn>();
                    if (curObj.Count == 0)
                    {
                        curObj = custom.Where(item => item.Name == formulaPart.FieldName).ToList<DVBaseColumn>();
                        RecursiveCustomColumn(ref Columns, curObj[0], noncustom, custom);
                    }
                }

                curcustom = Columns.Where(item => item.Name == col.Name).ToList<DVBaseColumn>();
                if (curcustom.Count == 0)
                    Columns.Add(col);
            }
            return Columns;
        }

        public void RecursiveCustomColumn(ref List<DVBaseColumn> Columns, DVBaseColumn _column, List<DVBaseColumn> noncustom, List<DVBaseColumn> custom)
        {
            foreach (FormulaPart formulaPart in _column.FormulaParts)
            {
                List<DVBaseColumn> curObj = noncustom.Where(item => item.Name == formulaPart.FieldName).ToList<DVBaseColumn>();
                if (curObj.Count == 0)
                {
                    curObj = custom.Where(item => item.Name == formulaPart.FieldName).ToList<DVBaseColumn>();
                    RecursiveCustomColumn(ref Columns, curObj[0], noncustom, custom);
                    List<DVBaseColumn> curcustom = Columns.Where(item => item.Name == curObj[0].Name).ToList<DVBaseColumn>();
                    if (curcustom.Count == 0)
                        Columns.Add(curObj[0]);
                }
            }
        }

        public DVBaseColumn Check4Tree(EbTableVisualization dv)
        {
            return dv.Columns.FirstOrDefault(e => e.IsTree == true);
        }

        public void FormatTreecolData(Node<EbDataRow> Node, ref EbDataTable _formattedTable, DVBaseColumn treecol, int i)
        {
            _formattedTable.Rows[i][treecol.Data] = GetTreeHtml(_formattedTable.Rows[i][treecol.Data], Node.IsGroup, Node.Level);

        }

        public void RecursiveGetTreeChilds(Node<EbDataRow> Nodedr, EbDataVisualization _dv, List<DVBaseColumn> dependencyTable, CultureInfo _user_culture, User _user, ref EbDataTable _formattedTable, ref Globals globals, bool bObfuscute, bool _isexcel, ref Dictionary<int, List<object>> Summary, ref ExcelWorksheet worksheet, ref int i, int count, bool isTree)
        {
            foreach (Node<EbDataRow> dr in Nodedr.Children)
            {
                i++;
                DataTable2FormatedTable(dr.Item, _dv, dependencyTable, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, ref worksheet, i, count, dr.IsGroup, dr.Level, isTree);
                if (dr.Children.Count > 0)
                {
                    RecursiveGetTreeChilds(dr, _dv, dependencyTable, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, ref worksheet, ref i, count, isTree);
                }

            }

        }

        public void DataTable2FormatedTable(EbDataRow row, EbDataVisualization _dv, List<DVBaseColumn> dependencyTable, CultureInfo _user_culture, User _user, ref EbDataTable _formattedTable, ref Globals globals, bool bObfuscute, bool _isexcel, ref Dictionary<int, List<object>> Summary, ref ExcelWorksheet worksheet, int i, int count, bool isgroup = false, int level = 0, bool isTree = false)
        {
            try
            {
                _formattedTable.Rows.Add(_formattedTable.NewDataRow2());
                _formattedTable.Rows[i][_formattedTable.Columns.Count - 1] = i + 1;
                int j = 0;
                foreach (DVBaseColumn col in dependencyTable)
                {
                    if (col.IsCustomColumn)
                        CustomColumDoCalc4Row(row, _dv, globals, col);
                    bool AllowLinkifNoData = true;
                    var cults = col.GetColumnCultureInfo(_user_culture);
                    object _unformattedData = row[col.Data];
                    object _formattedData = _unformattedData;

                    if (col.Type == EbDbTypes.Date)
                    {
                        DateTimeformat(_unformattedData, ref _formattedData, ref row, col, cults, _user);
                    }
                    else if (col.Type == EbDbTypes.Decimal || col.Type == EbDbTypes.Int32 || col.Type == EbDbTypes.Int64)
                    {
                        if ((col as DVNumericColumn).SuppresIfZero && (_isexcel == false))
                        {
                            _formattedData = (Convert.ToDecimal(_unformattedData) == 0) ? string.Empty : Convert.ToDecimal(_unformattedData).ToString("N", cults.NumberFormat);

                        }
                        else
                            _formattedData = Convert.ToDecimal(_unformattedData).ToString("N", cults.NumberFormat);
                        if (((col as DVNumericColumn).RenderAs == NumericRenderType.ProgressBar) && (_isexcel == false))
                            _formattedData = "<div class='progress'><div class='progress-bar' role='progressbar' aria-valuenow='" + _formattedData + "' aria-valuemin='0' aria-valuemax='100' style='width:" + _unformattedData.ToString() + "%'>" + _formattedData + "</div></div>";

                        SummaryCalc(ref Summary, col, _unformattedData, cults);
                    }
                    else if (col.Type == EbDbTypes.String && (_isexcel == false))
                    {
                        
                        if ((col as DVStringColumn).RenderAs == StringRenderType.Marker)
                            _formattedData = "<a href = '#' class ='columnMarker' data-latlong='" + _unformattedData + "'><i class='fa fa-map-marker fa-2x' style='color:red;'></i></a>";

                    }
                    else if (col.Type == EbDbTypes.Boolean)
                    {

                    }
                    string info = (col.AllowedCharacterLength > 0) ? col.sTitle + " : " + row[col.Data] + "</br>" : string.Empty;
                    if (col.InfoWindow.Count > 0)
                    {
                        foreach (DVBaseColumn _column in col.InfoWindow)
                        {
                            if (_column.Name != col.Name)
                                info += _column.sTitle + " : " + row[_column.Data] + "</br>";
                        }
                    }
                    if (!string.IsNullOrEmpty(info))
                    {
                        _formattedData = _unformattedData.ToString().Truncate(col.AllowedCharacterLength);
                        _formattedData = "<span class='columntooltip' data-toggle='popover' data-content='" + info.ToBase64() + "'>" + _formattedData +"</span>";
                    }
                    if (col.HideLinkifNoData)
                    {
                        if (_formattedData.ToString() == string.Empty)
                            AllowLinkifNoData = false;
                    }

                    if (this._replaceEbColumns)
                    {
                        if (col.Name == "eb_created_by" || col.Name == "eb_lastmodified_by" || col.Name == "eb_loc_id")
                        {
                            ModifyEbColumns(col, ref _formattedData, _unformattedData);
                        }
                    }

                    if (!string.IsNullOrEmpty(col.LinkRefId) && (_isexcel == false))
                    {
                        if (AllowLinkifNoData)
                        {
                            if (_formattedData.ToString() == string.Empty)
                                _formattedData = "...";
                            if (col.LinkType == LinkTypeEnum.Popout)
                                _formattedData = "<a href='#' oncontextmenu='return false' class ='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "'>" + _formattedData + "</a>";
                            else if (col.LinkType == LinkTypeEnum.Inline)
                                _formattedData = _formattedData + "&nbsp; <a  href= '#' oncontextmenu= 'return false' class ='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-inline='true' data-data='" + _formattedData + "'><i class='fa fa-caret-down'></i></a>";
                            else if (col.LinkType == LinkTypeEnum.Both)
                                _formattedData = "<a href='#' oncontextmenu='return false' class ='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "'>" + _formattedData + "</a>" + "&nbsp; <a  href ='#' oncontextmenu='return false' class='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-inline='true' data-data='" + _formattedData + "'> <i class='fa fa-caret-down'></i></a>";
                            else if (col.LinkType == LinkTypeEnum.Popup)
                                _formattedData = "<a  href= '#' oncontextmenu= 'return false' class ='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-popup='true' data-data='" + _formattedData + "'>" + _formattedData + "</a>";
                        }
                    }
                    if (col.Type == EbDbTypes.String && (col as DVStringColumn).RenderAs == StringRenderType.Link && col.LinkType == LinkTypeEnum.Tab && (_isexcel == false))/////////////////
                    {
                        _formattedData = "<a href='../leadmanagement/" + row[0] + "' target='_blank'>" + _formattedData + "</a>";
                    }

                    if (bObfuscute && (_isexcel == false))
                    {
                        if (col.HideDataRowMoreThan > 0 && col.HideDataRowMoreThan < count)
                        {
                            _formattedData = "********";
                        }
                    }


                    this.conditinallyformatColumn(col, ref _formattedData, _unformattedData);

                    _formattedTable.Rows[i][col.Data] = _formattedData;
                    if (_isexcel)
                        worksheet.Cells[i + 2, j + 1].Value = _formattedData;

                    if (i + 1 == count)
                    {
                        SummaryCalcAverage(ref Summary, col, cults, count);
                    }
                    j++;
                }

                if (isTree)
                {
                    var treecol = _dv.Columns.FirstOrDefault(e => e.IsTree == true);
                    _formattedTable.Rows[i][treecol.Data] = GetTreeHtml(_formattedTable.Rows[i][treecol.Data], isgroup, level);
                }
            }
            catch (Exception e)
            {
                Log.Info("PreProcessing in datatable Exception........." + e.StackTrace);
            }

        }

        public void ModifyEbColumns(DVBaseColumn col, ref object _formattedData, object _unformattedData)
        {
            if (col.Name == "eb_created_by" || col.Name == "eb_lastmodified_by")
            {
                try
                {
                    int user_id = Convert.ToInt32(_unformattedData);
                    if (this._ebSolution.Users != null && this._ebSolution.Users.ContainsKey(user_id))
                    {
                        _formattedData = this._ebSolution.Users[user_id];
                    }
                }
                catch(Exception e)
                {
                    _formattedData = _unformattedData.ToString();
                }
            }
            else if (col.Name == "eb_loc_id")
            {
                int loc_id = Convert.ToInt32(_unformattedData);
                if (this._ebSolution.Locations.ContainsKey(loc_id))
                {
                    _formattedData = this._ebSolution.Locations[loc_id].ShortName;
                }
            }
        }

        public void DateTimeformat(object _unformattedData, ref object _formattedData, ref EbDataRow row, DVBaseColumn col, CultureInfo cults, User _user)
        {
            _unformattedData = (_unformattedData == DBNull.Value) ? DateTime.MinValue : _unformattedData;
            if ((col as DVDateTimeColumn).Format == DateFormat.Date)
            {
                _formattedData = (((DateTime)_unformattedData).Date != DateTime.MinValue) ? Convert.ToDateTime(_unformattedData).ToString("d", cults.DateTimeFormat) : string.Empty;
                row[col.Data] = Convert.ToDateTime(_unformattedData).ToString("yyyy-MM-dd");
            }
            else if ((col as DVDateTimeColumn).Format == DateFormat.DateTime)
            {
                if ((col as DVDateTimeColumn).ConvretToUsersTimeZone)
                    _formattedData = (((DateTime)_unformattedData).Date != DateTime.MinValue) ? Convert.ToDateTime(_unformattedData).ConvertFromUtc(_user.Preference.TimeZone).ToString(cults.DateTimeFormat.ShortDatePattern + " " + cults.DateTimeFormat.ShortTimePattern) : string.Empty;
                else
                    _formattedData = (((DateTime)_unformattedData).Date != DateTime.MinValue) ? Convert.ToDateTime(_unformattedData).ToString(cults.DateTimeFormat.ShortDatePattern + " " + cults.DateTimeFormat.ShortTimePattern) : string.Empty;
                row[col.Data] = Convert.ToDateTime(_unformattedData);
            }
        }

        public void conditinallyformatColumn(DVBaseColumn col, ref object _formattedData, object _unformattedData)
        {
            foreach (ColumnCondition cond in col.ConditionalFormating )
            {
                if (cond.CompareValues(_unformattedData))
                {
                    _formattedData = "<div class='conditionformat' style='background-color:" + cond.BackGroundColor + ";color:" + cond.FontColor + ";'>" + _formattedData + "</div>";
                }
            }

        }

        public bool NumericCompareValues(NumericCondition cond, object _unformattedData)
        {
            if (cond.Operator == NumericOperators.Equals)
            {
                return Convert.ToInt32(_unformattedData) == Convert.ToInt32(cond.Value);
            }
            else if (cond.Operator == NumericOperators.LessThan)
            {
                return Convert.ToInt32(_unformattedData) < Convert.ToInt32(cond.Value);
            }
            else if (cond.Operator == NumericOperators.GreaterThan)
            {
                return Convert.ToInt32(_unformattedData) > Convert.ToInt32(cond.Value);
            }
            else if (cond.Operator == NumericOperators.LessThanOrEqual)
            {
                return Convert.ToInt32(_unformattedData) <= Convert.ToInt32(cond.Value);
            }
            else if (cond.Operator == NumericOperators.GreaterThanOrEqual)
            {
                return Convert.ToInt32(_unformattedData) >= Convert.ToInt32(cond.Value);
            }
            else if (cond.Operator == NumericOperators.Between)
            {
                return Convert.ToInt32(_unformattedData) >= Convert.ToInt32(cond.Value) && Convert.ToInt32(_unformattedData) <= Convert.ToInt32(cond.Value1);
            }
            return false;
        }

        public bool DateCompareValues(DateCondition cond, object _unformattedData)
        {
            DateTime data = Convert.ToDateTime(_unformattedData);
            DateTime value = Convert.ToDateTime(cond.Value);

            if (cond.Operator == NumericOperators.Equals)
            {
                return data == value;
            }
            else if (cond.Operator == NumericOperators.LessThan)
            {
                return data < value;
            }
            else if (cond.Operator == NumericOperators.GreaterThan)
            {
                return data > value;
            }
            else if (cond.Operator == NumericOperators.LessThanOrEqual)
            {
                return data <= value;
            }
            else if (cond.Operator == NumericOperators.GreaterThanOrEqual)
            {
                return data >= value;
            }
            else if (cond.Operator == NumericOperators.Between)
            {
                return data >= value && data <= Convert.ToDateTime(cond.Value1);
            }
            return false;
        }

        public bool StringCompareValues(StringCondition cond, object _unformattedData)
        {
            string data = _unformattedData.ToString().Trim().ToLower();
            string searchval = cond.Value.Trim().ToLower().ToString();

            if (cond.Operator == StringOperators.Startwith)
            {
                return data.StartsWith(searchval);
            }
            else if (cond.Operator == StringOperators.EndsWith)
            {
                return data.EndsWith(searchval);
            }
            else if (cond.Operator == StringOperators.Contains)
            {
                return data.Contains(searchval);
            }
            else if (cond.Operator == StringOperators.Equals)
            {
                return data == searchval;
            }
            return false;
        }

        public bool BooleanCompareValues(BooleanCondition cond, object _unformattedData)
        {
            return false;
        }

        public string GetTreeHtml(object data, bool isgroup, int level)
        {
            string html = string.Empty;
            for (int i = 0; i < level; i++)
            {
                html += "&emsp;&emsp;";
            }
            if (isgroup)
            {
                string classs = string.Empty;
                if (level == 0)
                    classs = "levelzero";
                html += "<i class='fa fa-minus-square-o groupform " + classs + "' style='cursor:pointer;' data-group=" + isgroup + " data-level=" + level + ">&nbsp; &nbsp;<b>" + data + "</b></i>";
            }
            else
                html += "<i class='itemform' data-group=" + isgroup + " data-level=" + level + ">" + data + "</i>";

            return html;
        }

        public void SummaryCalc(ref Dictionary<int, List<object>> Summary, DVBaseColumn col, object _unformattedData, CultureInfo cults)
        {
            if ((col as DVNumericColumn).Aggregate)
            {
                if (Summary.Keys.Contains(col.Data))
                {
                    Summary[col.Data][0] = (Convert.ToDecimal(Summary[col.Data][0]) + Convert.ToDecimal(_unformattedData)).ToString("N", cults.NumberFormat);
                }
                else
                {
                    Summary.Add(col.Data, new List<object> { 0, 0 });
                    Summary[col.Data][0] = (Convert.ToDecimal(Summary[col.Data][0]) + Convert.ToDecimal(_unformattedData)).ToString("N", cults.NumberFormat);
                }
            }
        }

        public void SummaryCalcAverage(ref Dictionary<int, List<object>> Summary, DVBaseColumn col, CultureInfo cults, int count)
        {
            if (Summary.Keys.Contains(col.Data))
            {
                Summary[col.Data][1] = (Convert.ToDecimal(Summary[col.Data][0]) / count).ToString("N", cults.NumberFormat);
            }
        }

        //public List<GroupingDetails> RowGroupingCommon(EbDataTable Table, EbDataVisualization Visualization, CultureInfo Culture, ref EbDataTable FormattedTable, bool IsMultiLevelRowGrouping = false)
        //{
        //    Dictionary<string, GroupingDetails> RowGrouping = new Dictionary<string, GroupingDetails>();

        //    int TotalLevels = (IsMultiLevelRowGrouping) ? (Visualization as EbTableVisualization).CurrentRowGroup.RowGrouping.Count : 1,
        //    CurSortIndex = 0;

        //    List<int> AggregateColumnIndexes = GetAggregateIndexes(Visualization.Columns);
        //    List<DVBaseColumn> RowGroupingColumns = new List<DVBaseColumn>((Visualization as EbTableVisualization).CurrentRowGroup.RowGrouping);
        //    int ColCount = Visualization.Columns.Count;
        //    string PreviousGroupingText = string.Empty;
        //    int SerialCount = 0, PrevRowIndex = 0;
        //    for (int i = 0; i < Table.Rows.Count; i++)
        //    {
        //        CurSortIndex += TotalLevels + 30;

        //        EbDataRow currentRow = Table.Rows[i];
        //        int delimCount = 1;
        //        string TempGroupingText = CreateCollectionKey(currentRow, IsMultiLevelRowGrouping, BlankText, TotalLevels, RowGroupingColumns, i, ref delimCount);

        //        if (TempGroupingText.Equals(PreviousGroupingText) == false)
        //        {
        //            SerialCount = 0;
        //            FormattedTable.Rows[i][Table.Columns.Count] = ++SerialCount;
        //            CreateHeaderAndFooterPairs(currentRow, AggregateColumnIndexes, RowGroupingColumns, RowGrouping, Visualization.Columns, TotalLevels, IsMultiLevelRowGrouping, Culture, TempGroupingText, ref CurSortIndex, ColCount);

        //            HeaderGroupingDetails HeaderObject = RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails;
        //            HeaderObject.SetRowIndex(i);
        //            HeaderObject.InsertionType = BeforeText;

        //            (RowGrouping[FooterPrefix + TempGroupingText] as FooterGroupingDetails).InsertionType = BeforeText;

        //            if (i > 0)
        //            {
        //                (RowGrouping[FooterPrefix + PreviousGroupingText] as FooterGroupingDetails).SetRowIndex(i);

        //                if (IsMultiLevelRowGrouping && i == Table.Rows.Count - 1 &&
        //                    (RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails).GroupingCount == 1 &&
        //                    (RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails).LevelCount == 0)
        //                {
        //                    SetFinalFooterRow(currentRow, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, i, TempGroupingText, CurSortIndex);
        //                }
        //            }
        //            if (!IsMultiLevelRowGrouping && i == PrevRowIndex + 1 && i == Table.Rows.Count - 1)
        //            {
        //                SetFinalFooterRow(currentRow, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, i, TempGroupingText, CurSortIndex);
        //            }
        //        }
        //        else
        //        {
        //            FormattedTable.Rows[i][Table.Columns.Count] = ++SerialCount;

        //            (RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails).GroupingCount++;
        //            if (i == Table.Rows.Count - 1)
        //            {
        //                SetFinalFooterRow(currentRow, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, i, TempGroupingText, CurSortIndex);
        //            }
        //        }

        //        (RowGrouping[FooterPrefix + TempGroupingText] as FooterGroupingDetails).SetValue(currentRow);

        //        PreviousGroupingText = TempGroupingText;
        //        PrevRowIndex = i;
        //    }
        //    List<GroupingDetails> SortedGroupings = RowGrouping.Values.ToList();
        //    SortedGroupings.Sort();
        //    return SortedGroupings;
        //}

        public void DoRowGroupingCommon(EbDataRow currentRow, EbDataVisualization Visualization, List<DVBaseColumn> dependencyTable, CultureInfo Culture, User _user, ref EbDataTable FormattedTable, bool IsMultiLevelRowGrouping, ref Dictionary<string, GroupingDetails> RowGrouping, ref string PreviousGroupingText, ref int CurSortIndex, ref int SerialCount, int PrevRowIndex, int dvColCount, int TotalLevels, ref List<int> AggregateColumnIndexes, ref List<DVBaseColumn> RowGroupingColumns, int RowCount)
        {
            CurSortIndex += TotalLevels + 30;

            int delimCount = 1;
            string TempGroupingText = CreateCollectionKey(currentRow, IsMultiLevelRowGrouping, BlankText, TotalLevels, RowGroupingColumns, PrevRowIndex, ref delimCount, Culture, _user);

            if (TempGroupingText.Equals(PreviousGroupingText) == false)
            {
                SerialCount = 0;
                FormattedTable.Rows[PrevRowIndex][dvColCount] = ++SerialCount;
                CreateHeaderAndFooterPairs(currentRow, AggregateColumnIndexes, RowGroupingColumns, RowGrouping, Visualization.Columns, TotalLevels, IsMultiLevelRowGrouping, Culture, TempGroupingText, ref CurSortIndex, dvColCount, _user);

                HeaderGroupingDetails HeaderObject = RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails;
                HeaderObject.SetRowIndex(PrevRowIndex);
                HeaderObject.InsertionType = BeforeText;

                (RowGrouping[FooterPrefix + TempGroupingText] as FooterGroupingDetails).InsertionType = BeforeText;

                if (PrevRowIndex > 0)
                {
                    (RowGrouping[FooterPrefix + PreviousGroupingText] as FooterGroupingDetails).SetRowIndex(PrevRowIndex);

                    if (IsMultiLevelRowGrouping && PrevRowIndex == RowCount - 1 &&
                        (RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails).GroupingCount == 1 &&
                        (RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails).LevelCount == 0)
                    {
                        SetFinalFooterRow(currentRow, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, PrevRowIndex, TempGroupingText, CurSortIndex, Culture, _user);
                    }
                }
                if ( PrevRowIndex == RowCount - 1)
                {
                    SetFinalFooterRow(currentRow, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, PrevRowIndex, TempGroupingText, CurSortIndex, Culture, _user);
                }
            }
            else
            {
                FormattedTable.Rows[PrevRowIndex][dvColCount] = ++SerialCount;

                (RowGrouping[HeaderPrefix + TempGroupingText] as HeaderGroupingDetails).GroupingCount++;
                if (PrevRowIndex == RowCount - 1)
                {
                    SetFinalFooterRow(currentRow, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, PrevRowIndex, TempGroupingText, CurSortIndex, Culture, _user);
                }
            }

                (RowGrouping[FooterPrefix + TempGroupingText] as FooterGroupingDetails).SetValue(currentRow);

            PreviousGroupingText = TempGroupingText;
        }

        private void SetFinalFooterRow(EbDataRow currentRow, List<DVBaseColumn> rowGroupingColumns, bool IsMultiLevelRowGrouping, Dictionary<string, GroupingDetails> RowGrouping, int i, string TempGroupingText, int CurSortIndex,  CultureInfo _user_culture, User _user)
        {
            List<string> GroupingKeys = CreateRowGroupingKeys(currentRow, rowGroupingColumns, IsMultiLevelRowGrouping, _user_culture, _user);
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

        private string CreateCollectionKey(EbDataRow row, bool IsMultiLevelRowGrouping, string BlankText, int TotalLevels, List<DVBaseColumn> RowGroupingColumns, int i, ref int delimCount, CultureInfo _user_culture, User _user)
        {
            string TempGroupingText = string.Empty;
            foreach (DVBaseColumn Column in RowGroupingColumns)
            {
                string tempValue = row[Column.Data].ToString().Trim();
                if (Column.Type == EbDbTypes.Date)
                    tempValue = GetFormattedDate(row, Column, _user_culture, _user);
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
            int TotalLevels, bool IsMultiLevelGrouping, CultureInfo culture, string TempGroupingText, ref int CurSortIndex, int ColumnCount, User _user)
        {
            List<string> TempKey = CreateRowGroupingKeys(CurrentRow, _rowGroupingColumns, (TotalLevels > 1) ? true : false, culture,_user);
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

        private static List<string> CreateRowGroupingKeys(EbDataRow CurrentRow, List<DVBaseColumn> RowGroupingColumns, bool IsMultiLevelRowGrouping, CultureInfo _user_culture, User _user)
        {
            List<string> TempKey = new List<string>();
            string TempStr = string.Empty;
            foreach (DVBaseColumn column in RowGroupingColumns)
            {
                string tempvalue = CurrentRow[column.Data].ToString().Trim();
                if (column.Type == EbDbTypes.Date)
                    tempvalue = GetFormattedDate(CurrentRow, column, _user_culture, _user);
                if (IsMultiLevelRowGrouping)
                {
                    TempKey.Add(((TempKey.Count > 0) ? TempKey.Last() + GroupDelimiter : string.Empty) + ((tempvalue.IsNullOrEmpty()) ? BlankText : tempvalue));
                }
                else
                {
                    TempStr += (TempStr.Equals(string.Empty)) ? ((tempvalue.IsNullOrEmpty()) ? BlankText : tempvalue): GroupDelimiter + ((tempvalue.IsNullOrEmpty()) ? BlankText : tempvalue);
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

        public static string GetFormattedDate(EbDataRow row, DVBaseColumn Column, CultureInfo _user_culture, User _user)
        {
            object _unformattedData = row[Column.Data];
            var cults = Column.GetColumnCultureInfo(_user_culture);
            DVDateTimeColumn col = (Column as DVDateTimeColumn);
            if (col.Format == DateFormat.Date)
            {
                return Convert.ToDateTime(_unformattedData).ToString("d", cults.DateTimeFormat).Trim();

            }
            else if (col.Format == DateFormat.DateTime)
            {
                if (col.ConvretToUsersTimeZone)
                    return Convert.ToDateTime(_unformattedData).ConvertFromUtc(_user.Preference.TimeZone).ToString(cults.DateTimeFormat.ShortDatePattern + " " + cults.DateTimeFormat.ShortTimePattern).Trim();
                else
                    return Convert.ToDateTime(_unformattedData).ToString(cults.DateTimeFormat.ShortDatePattern + " " + cults.DateTimeFormat.ShortTimePattern).Trim();

            }
            return string.Empty;
        }

        public List<string> PermissionCheck(User _user, string refId)
        {
            List<string> permList = new List<string>();
            var x = refId.Split("-");
            var objid = x[3].PadLeft(5, '0');
            List<string> liteperm = new List<string>();
            if (_user.Roles.Contains(SystemRoles.SolutionOwner.ToString()) || _user.Roles.Contains(SystemRoles.SolutionAdmin.ToString()))
            {
                permList.Add("Excel");
                permList.Add("SUMMARIZE");
                permList.Add("Pdf");
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

        public TreeData<EbDataRow> TreeGeneration(EbDataTable _formattedtable, EbDataTable _unformatted, DVBaseColumn col)
        {
            if (col.ParentColumn.Count > 0 && col.GroupingColumn.Count > 0)
            {
                var tree = _unformatted.Enumerate().ToTree(row => true,
                           (parent, child) => Convert.ToInt32(parent["id"]) == Convert.ToInt32(child[col.ParentColumn[0].Name]),
                           col.GroupingColumn[0].Name);
                return tree;
            }
            return new TreeData<EbDataRow>();
        }

        [CompressResponse]
        public DataSourceDataResponse Post(InlineTableDataRequest request)
        {
            DataSourceDataResponse dsresponse = null;
            EbDataVisualization _dV = request.EbDataVisualization;
            this._replaceEbColumns = request.ReplaceEbColumns;
            var _ds = this.Redis.Get<EbDataReader>(request.RefId);
            string _sql = string.Empty;
            request.IsExcel = false;
            this._ebSolution = request.eb_solution;
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
            if (GetLogEnabled(request.RefId))
            {
                TimeSpan T = _dataset.EndTime - _dataset.StartTime;
                InsertExecutionLog(_dataset.RowNumbers, T, _dataset.StartTime, request.UserId, request.Params, request.RefId);
            }
            //-- 
            Console.WriteLine(DateTime.Now);
            var dtEnd = DateTime.Now;
            var ts = (dtEnd - dtStart).TotalMilliseconds;
            Console.WriteLine("final:::" + ts);
            int _recordsTotal = 0, _recordsFiltered = 0;
            if (_isPaged)
            {
                Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsTotal);
                Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsFiltered);
            }
            _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[0].Rows.Count;
            _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[0].Rows.Count;
            //-- 
            EbDataTable _formattedDataTable = null;
            List<GroupingDetails> _levels = new List<GroupingDetails>();
            PrePrcessorReturn returnObj = new PrePrcessorReturn();
            if (_dataset.Tables.Count > 0 && _dV != null)
            {
                returnObj = PreProcessing(ref _dataset, request.Params, _dV, request.UserInfo, ref _levels, request.IsExcel);
            }
            dsresponse = new DataSourceDataResponse
            {
                Data = _dataset.Tables[0].Rows,
                FormattedData = (returnObj.FormattedTable != null) ? returnObj.FormattedTable.Rows : null,
                RecordsTotal = _recordsTotal,
                RecordsFiltered = _recordsFiltered,
                Ispaged = _isPaged
            };
            this.Log.Info("dsresponse*****" + dsresponse.Data);
            var x = EbSerializers.Json_Serialize(dsresponse);
            return dsresponse;
        }

        public FileInfo PreExcelCalculation(string sFileName)
        {
            MemoryStream stream = new MemoryStream();
            FileInfo file = new FileInfo(Path.Combine(sFileName));
            return file;
        }

        public void PreExcelAddHeader(ref ExcelWorksheet worksheet, EbDataVisualization _dv)
        {
            for (var i = 0; i < _dv.Columns.Count; i++)
            {
                worksheet.Cells[1, i + 1].Value = _dv.Columns[i].Name;
            }
        }

    }

    public class PrePrcessorReturn
    {
        public EbDataTable FormattedTable;
        public Dictionary<int, List<object>> Summary;
        public byte[] excel_file;
        public RowColletion rows;
        public List<Node<EbDataRow>> tree;
    }
}
