using ExpressBase.Common;
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

            var _ds = this.Redis.Get<EbDataSource>(request.RefId);

            if (_ds == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                Redis.Set<EbDataSource>(request.RefId, _ds);
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
            List<GroupingDetails> _levels = new List<GroupingDetails>();
            if (_dataset.Tables.Count > 0 && _dV != null)
            {
                _formattedDataTable = PreProcessing(ref _dataset, _dV, request.UserInfo, ref _levels);
                //_levels = GetGroupInfo2(_dataset.Tables[0], _dV);
            }

            dsresponse = new DataSourceDataResponse
            {
                Draw = request.Draw,
                Data = _dataset.Tables[0].Rows,
                FormattedData = (_formattedDataTable != null) ? _formattedDataTable.Rows : null,
                RecordsTotal = _recordsTotal,
                RecordsFiltered = _recordsFiltered,
                Ispaged = _isPaged,
                Levels = _levels
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
                var _ds = this.Redis.Get<EbDataSource>(request.RefId);
                if (_ds == null)
                {
                    var myService = base.ResolveService<EbObjectService>();
                    var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                    _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbDataSource>(request.RefId, _ds);
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

        public EbDataTable PreProcessing(ref EbDataSet _dataset, EbDataVisualization _dv, User _user, ref List<GroupingDetails> _levels)
        {
            dynamic result = null;
            var _user_culture = CultureInfo.GetCultureInfo(_user.Preference.Locale);
            var colCount = _dataset.Tables[0].Columns.Count;
            //Dictionary<string, int> dict = new Dictionary<string, int>();
            if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.ORACLE && _dv.IsPaging)
            {
                _dataset.Tables[0].Columns.RemoveAt(colCount - 1);// rownum deleted for oracle
                for (int i = 0; i < _dataset.Tables[0].Rows.Count; i++)
                {
                    _dataset.Tables[0].Rows[i].RemoveAt(colCount - 1);
                }
            }

            EbDataTable _formattedTable = new EbDataTable();
            DVColumnCollection colColl = new DVColumnCollection();
            foreach(DVBaseColumn col in _dv.Columns)
            {
                colColl.Add(col.ShallowCopy());
            }
            colColl.Sort(new Comparison<DVBaseColumn>((x, y) => Decimal.Compare(x.Data, y.Data)));
            foreach (DVBaseColumn col in colColl)
            {
                var cults = col.GetColumnCultureInfo(_user_culture);
                _formattedTable.Columns.Add(new EbDataColumn { Type = col.Type, ColumnIndex = col.Data, ColumnName = col.Name });

                if (col.IsCustomColumn || (col.Formula != null && col.Formula != ""))
                {
                    string[] _dataFieldsUsed;
                    Script valscript = CSharpScript.Create<dynamic>(col.Formula, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic"), globalsType: typeof(Globals));
                    valscript.Compile();
                    _dataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = col.Data, ColumnName = col.Name, Type = col.Type });
                    for (int i = 0; i < _dataset.Tables[0].Rows.Count; i++)
                    {
                        Globals globals = new Globals();
                        var matches = Regex.Matches(col.Formula, @"T[0-9]{1}.\w+").OfType<Match>().Select(m => m.Groups[0].Value).Distinct();
                        _dataFieldsUsed = new string[matches.Count()];
                        int j = 0;
                        foreach (var match in matches)
                            _dataFieldsUsed[j++] = match;
                        try
                        {
                            foreach (string calcfd in _dataFieldsUsed)
                            {
                                string TName = calcfd.Split('.')[0];
                                string fName = calcfd.Split('.')[1];
                                globals[TName].Add(fName, new NTV { Name = fName, Type = _dataset.Tables[0].Columns[fName].Type, Value = _dataset.Tables[0].Rows[i][fName] });
                            }
                        }
                        catch (Exception e)
                        {
                        }
                        try
                        {
                            result = valscript.RunAsync(globals).Result.ReturnValue.ToString();
                        }
                        catch (Exception) { }
                        _dataset.Tables[0].Rows[i].Insert(col.Data, result);
                    }
                }

                for (int i = 0; i < _dataset.Tables[0].Rows.Count; i++)
                {
                    object _unformattedData = _dataset.Tables[0].Rows[i][col.Data];
                    object _formattedData = _unformattedData;

                    if (col.Data == 0)
                        _formattedTable.Rows.Add(new EbDataRow());

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
                            _formattedData = "<a href = '#' oncontextmenu = 'return false' class ='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-inline='true' data-data='" + _formattedData + "'><i class='fa fa-plus'></i></a>" + _formattedData;
                        else if (col.LinkType == LinkTypeEnum.Both)
                            _formattedData = "<a href ='#' oncontextmenu='return false' class='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-inline='true' data-data='" + _formattedData + "'> <i class='fa fa-plus'></i></a>" + "&nbsp;  <a href='#' oncontextmenu='return false' class ='tablelink' data-link='" + col.LinkRefId + "'>" + _formattedData + "</a>";
                    }
                    if (col.Type == EbDbTypes.String && (col as DVStringColumn).RenderAs == StringRenderType.Link && col.LinkType == LinkTypeEnum.Tab)/////////////////
                    {
                        _formattedData = "<a href='../custompage/leadmanagement?ac=" + _dataset.Tables[0].Rows[i][0] + "' target='_blank'>" + _formattedData + "</a>";
                    }

                    _formattedTable.Rows[i].Insert(col.Data, _formattedData);

                }
            }
            //List<LevelDetails> Levels;
            if ((_dv as EbTableVisualization).RowGroupCollection.Count > 0)
            {
                if ((_dv as EbTableVisualization).CurrentRowGroup.GetType().Name == "SingleLevelRowGroup")
                    //_levels = GetGroupInfoRecursive(_dataset.Tables[0], _dv, _user_culture, false);
                    _levels = RowGroupingSingleLevel(_dataset.Tables[0], _dv, _user_culture, false);
                else if ((_dv as EbTableVisualization).CurrentRowGroup.GetType().Name == "MultipleLevelRowGroup")
                    //_levels = GetGroupInfoRecursive(_dataset.Tables[0], _dv, _user_culture, true);//GetGroupInfoMultiLevel(_dataset.Tables[0], _dv, _user_culture);
                    _levels = RowGroupingSingleLevel(_dataset.Tables[0], _dv, _user_culture, true);
            }


            return _formattedTable;
        }

        public string GetHeaderRecursive(GroupingDetails level, List<string> GroupsList, int colCount, RowGroupParent currentGroup = null)
        {
            //string headerHtml = string.Empty;

            var str = "<tr class='group' group='" + level + "'>";
            for (int itr = 0; itr < level.CurrentLevel; itr++)
                str += "<td> &nbsp;</td>";
            string tempstr = string.Empty;
            
            int i = -1;
            foreach (DVBaseColumn CurCol in currentGroup.RowGrouping)
            {
                i++;
                //tempstr = GetStringFromColumn(CurCol, level.LevelText, GroupsList[i]);
                if (CurCol.LinkRefId != null)
                    tempstr += CurCol.sTitle + ": <b data-rowgroup='true' data-colname='" + CurCol.Name + "' data-coltype='" + 
                        CurCol.Type + "+'' data-data='" + ((GroupsList[i] != null) ? GroupsList[i] : level.LevelText) + 
                        "' ><a href = '#' oncontextmenu='return false' class='tablelink' data-colindex='" + CurCol.Data +
                        "' data-link='" + CurCol.LinkRefId + "' tabindex='0'>" + level.LevelText + "</a></b>";
                else
                    tempstr += CurCol.sTitle + ": <b data-rowgroup='true' data-colname='" + CurCol.Name + "' data-coltype='" + CurCol.Type + "' data-data='" + ((GroupsList[i] != null) ? GroupsList[i] : level.LevelText) + "'>" + ((GroupsList[i] != null) ? GroupsList[i] : level.LevelText) + "</b>";
            }

            str += "<td><i class='fa fa-minus-square-o' style='cursor:pointer;'></i></td><td colspan=" + colCount + ">" + tempstr;
            return str;

            //return headerHtml;
        }

        public string UpdateHeaderRecursive(GroupingDetails level)
        {
            return level.Html + level.LevelCount;
        }

        public string GetFooterRecursive(GroupingDetails level)
        {
            string footerHtml = string.Empty;



            return footerHtml;
        }

        public string UpdateFooterRecursive(GroupingDetails level)
        {
            string footerHtml = string.Empty + level.Html;



            return footerHtml;
        }

        /// <summary>
        /// Method for row grouping part of preprocessing. The method iterates over the EbDataTable object exactly once
        /// to return the row grouped preprocessed data. 
        /// </summary>
        /// <param name="Table"></param>
        /// <param name="Visualization"></param>
        /// <param name="Culture"></param>
        /// <returns></returns>
        public List<GroupingDetails> RowGroupingSingleLevel(EbDataTable Table, EbDataVisualization Visualization, CultureInfo Culture, bool IsMultiLevelRowGrouping=false)
        {
            List<GroupingDetails> RowGrouping = new List<GroupingDetails>();
            const string AfterText = "After", BeforeText = "Before", BlankText= "(Blank)";
            int finalHeaderIndex = 0;
            ///Total number of levels - taken for multiple level row grouping. 
            ///Not used in single level, and always equals 1.
            int TotalLevels = (IsMultiLevelRowGrouping) ? (Visualization as EbTableVisualization).CurrentRowGroup.RowGrouping.Count : 1,
            TotalColumnCount = (Visualization as EbTableVisualization).Columns.Count;

            List<DVBaseColumn> RowGroupingColumns = new List<DVBaseColumn>((Visualization as EbTableVisualization).CurrentRowGroup.RowGrouping);

            GroupingDetails PreviousGrouping = new GroupingDetails();
            string PreviousGroupingText = string.Empty;
            Dictionary<int, int> LevelCount = new Dictionary<int, int>();
            Dictionary<int, double> NumericSum = new Dictionary<int, double>();
            Dictionary<int, double> NumericMax = new Dictionary<int, double>();
            Dictionary<int, double> NumericMin = new Dictionary<int, double>();
            Dictionary<int, double> NumericAvg = new Dictionary<int, double>();

            foreach (DVBaseColumn _column in Visualization.Columns)
            {
                if (_column.Type == EbDbTypes.Int32 || _column.Type == EbDbTypes.Int64 || _column.Type == EbDbTypes.Decimal || _column.Type == EbDbTypes.Int16)
                {
                    NumericSum.Add(_column.Data, 0.0);
                    NumericMax.Add(_column.Data, 0.0);
                    NumericMin.Add(_column.Data, 0.0);
                    NumericAvg.Add(_column.Data, 0.0);
                }
            }

            int CurrentLevel = 1;

            for (int i = 0; i < Table.Rows.Count; i++)
            {
                GroupingDetails CurrentGrouping = new GroupingDetails();
                string TempGroupingText = string.Empty;
                int delimCount = 1;
                foreach (DVBaseColumn Column in RowGroupingColumns)
                {
                    string tempValue = Table.Rows[i][Column.Data].ToString().Trim();
                    CurrentGrouping.GroupingString += (tempValue == string.Empty) ? BlankText : tempValue;//gets combination of both current and parent column value

                    TempGroupingText += (tempValue == string.Empty) ? BlankText : tempValue;//gets combination of both current and parent column value
                    TempGroupingText += (delimCount == TotalLevels) ? string.Empty : ":-:";
                    delimCount++;

                    CurrentGrouping.GroupingTexts.Add((tempValue == string.Empty) ? BlankText : tempValue);
                }

                //if(i==0)
                //{
                //    DrawHeader(CurrentGrouping, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, TotalColumnCount, TotalLevels);
                //    finalHeaderIndex = RowGrouping.Count - 1;

                //    if (LevelCount.Keys.Contains(CurrentLevel))
                //        LevelCount[CurrentLevel] += 1;
                //    else
                //        LevelCount[CurrentLevel] = 1;

                //    var IntegerKeys = NumericSum.Keys.ToList<int>();

                //    foreach (var _key in IntegerKeys)
                //    {
                //        NumericSum[_key] += Convert.ToDouble(Table.Rows[i][_key]);
                //        NumericMax[_key] += Convert.ToDouble(Table.Rows[i][_key]);
                //        NumericMin[_key] += Convert.ToDouble(Table.Rows[i][_key]);
                //        NumericAvg[_key] += Convert.ToDouble(Table.Rows[i][_key]);
                //    }
                //}

                if (TempGroupingText.Equals(PreviousGroupingText) == false)
                {
                    if (i == 0)
                    {
                        CurrentGrouping.GroupingCount = 1;
                        CurrentGrouping.CurrentLevel = CurrentLevel;
                        CurrentGrouping.RowIndex = i;
                        CurrentGrouping.IsHeader = true;
                        CurrentGrouping.InsertionType = BeforeText;
                    }

                    else
                    {
                        CurrentLevel = GetCurrentLevel(TempGroupingText, PreviousGroupingText, false);
                        RowGrouping.Last().Html = UpdateHeader(RowGrouping.Last());
                        DrawFooter(RowGrouping, BeforeText, TotalLevels, TotalColumnCount, CurrentLevel, i);

                        CurrentGrouping.GroupingCount = 1;
                        CurrentGrouping.CurrentLevel = CurrentLevel;
                        CurrentGrouping.RowIndex = i;
                        CurrentGrouping.IsHeader = true;
                        CurrentGrouping.InsertionType = BeforeText;
                        CurrentGrouping.Html = string.Empty;
                        finalHeaderIndex = RowGrouping.Count - 1;
                    }

                    if (i < Table.Rows.Count - 1)
                        DrawHeader(CurrentGrouping, RowGroupingColumns, IsMultiLevelRowGrouping, RowGrouping, TotalColumnCount, TotalLevels);
                }
                else if (TempGroupingText.Equals(PreviousGroupingText) == true)
                {

                    RowGrouping.Last().GroupingCount++;

                    var IntegerKeys = NumericSum.Keys.ToList<int>();
                    foreach (var _key in IntegerKeys)
                    {
                        NumericSum[_key] += Convert.ToDouble(Table.Rows[i][_key]);
                        NumericMax[_key] += Convert.ToDouble(Table.Rows[i][_key]);
                        NumericMin[_key] += Convert.ToDouble(Table.Rows[i][_key]);
                        NumericAvg[_key] += Convert.ToDouble(Table.Rows[i][_key]);
                    }


                    if (i == Table.Rows.Count - 1)
                    {
                        CurrentLevel = GetCurrentLevel(TempGroupingText, PreviousGroupingText, true);
                        RowGrouping.Last().Html = UpdateHeader(RowGrouping.Last());
                        DrawFooter(RowGrouping, BeforeText, TotalLevels, TotalColumnCount, CurrentLevel, i);
                    }
                }
                //if (i==Table.Rows.Count-1)
                //{
                //    CurrentLevel = TotalLevels;//GetCurrentLevel(TempGroupingText, PreviousGroupingText);

                //    RowGrouping[finalHeaderIndex].Html = UpdateHeader(RowGrouping[finalHeaderIndex]);

                //    for (int iter = 0; iter < CurrentLevel; iter++)
                //    {
                //        GroupingDetails FinalFooters = new GroupingDetails()
                //        {
                //            LevelCount = 1,
                //            IsHeader = false,
                //            Html = GetFooter(TotalColumnCount),
                //            InsertionType = AfterText,
                //            RowIndex = i,
                //            CurrentLevel = CurrentLevel,
                //            GroupingCount = RowGrouping[finalHeaderIndex].GroupingCount
                //        };
                //        RowGrouping.Add(new GroupingDetails(FinalFooters));
                //    }
                //}
                    PreviousGroupingText = TempGroupingText;
            }
            return RowGrouping;
        }

        private void DrawFooter(List<GroupingDetails> RowGrouping, string BeforeText, int TotalLevels, int TotalColumnCount, int CurrentLevel, int TableRowIndex)
        {
            for (int j = TotalLevels; j >= CurrentLevel; j--)
            {
                GroupingDetails FooterObject = new GroupingDetails()
                {
                    IsHeader = false,
                    Html = GetFooter(TotalColumnCount, "Level" + j + RowGrouping.Where(r => (r.IsHeader)).Last().GroupingTexts[j-1]),
                    InsertionType = BeforeText,
                    RowIndex = TableRowIndex,
                };
                RowGrouping.Add(new GroupingDetails(FooterObject));
            }
        }

        public int GetCurrentLevel(string CurrentString, string PreviousString, bool isEnd)
        {
            if (!isEnd)
            {
                string[] CurrentSplit = CurrentString.Split(":-:");
                string[] PreviousSplit = PreviousString.Split(":-:");
                if (CurrentSplit.Length == PreviousSplit.Length)
                {
                    for (int i = 1; i <= CurrentSplit.Length; i++)
                    {
                        if (!CurrentSplit[i - 1].Equals(PreviousSplit[i - 1]))
                            return i;
                    }
                }
            }

            return 1;
        }

        public string DrawHeader(GroupingDetails GroupingObject, List<DVBaseColumn> RowGroupingColumns, bool IsMultiLevelRowGrouping, List<GroupingDetails> groupings, int ColumnCount, int TotalLevels)
        {
            string str = string.Empty;
            var _Colcount = ColumnCount;
                for (int j = GroupingObject.CurrentLevel-1; j < TotalLevels; j++)
                {
                    str = "<tr class='group' group='" + GroupingObject.CurrentLevel + "'>";
                    for (int itr = 0; itr < GroupingObject.CurrentLevel; itr++)
                        str += "<td> &nbsp;</td>";
                    str += "<td><i class='fa fa-minus-square-o' style='cursor:pointer;'></i></td><td colspan=" + _Colcount + ">" + GroupingObject.GroupingTexts[j];

                GroupingDetails HeaderObject = new GroupingDetails()
                {
                    Html = str,
                    InsertionType = "Before",
                    IsHeader = true,
                    RowIndex = GroupingObject.RowIndex,
                    CurrentLevel = j + 1,
                    GroupingTexts = new List<string>(GroupingObject.GroupingTexts)
                };
                groupings.Add(HeaderObject);
                }
            //else
            //{
            //    string tempstr = string.Empty;
            //    str = "<tr class='group' group='" + GroupingObject.CurrentLevel + "'>";
            //    for (int itr = 0; itr < GroupingObject.CurrentLevel; itr++)
            //        str += "<td> &nbsp;</td>";
            //    foreach (string groupString in GroupingObject.GroupingTexts)
            //    {
            //        tempstr += groupString;
            //        if (groupString.Equals(GroupingObject.GroupingTexts.Last()) == false)
            //            tempstr += " - ";
            //    }
            //    str += "<td><i class='fa fa-minus-square-o' style='cursor:pointer;'></i></td><td colspan=" + _Colcount + ">" + tempstr;
            //    i = 1;
            //    GroupingDetails HeaderObject = new GroupingDetails()
            //    {
            //        Html = str,
            //        InsertionType = "Before",
            //        IsHeader = true,
            //        RowIndex = GroupingObject.RowIndex,
            //        CurrentLevel = i + 1,
            //    };
            //    groupings.Add(HeaderObject);
            //}

            return str;
        }
        
        public string UpdateHeader(GroupingDetails GroupingObject)
        {
            return GroupingObject.Html + ": " + GroupingObject.GroupingCount + "</tr>";
        }

        public string GetFooter(int ColumnsCount, string FooterText)
        {
            string Footer = string.Empty;
            Footer += "<tr class='group-sum'><td colspan='" + ColumnsCount+ "'></td><td>" + FooterText +"</td></tr>";
            return Footer;
        }



        /// <summary>
        /// Function for preprocessing data table and finding the header and footer structure for row grouping.
        /// For single level row grouping, it functions by iterating over the table exactly once.
        /// For multi-level row grouping, the single level grouping functionality is recursively applied at different levels.
        /// </summary>
        /// <param name="Table">The data table to be processed.</param>
        /// <param name="Visualisation">The object containing metadata for data visualization.</param>
        /// <param name="_user_culture"></param>
        /// <param name="_multipleLevelGrouping">Flag to check whether multi-level or single-level grouping must be performed.</param>
        /// <param name="levelIterator">The current level of processing, to be used when recursing over the function in multi-level grouping.</param>
        /// <param name="parentLevel">The level of the parent element. Same for single level grouping and different grouping of the same level 
        ///                 in multi-level row grouping.</param>
        /// <returns></returns>
        public List<GroupingDetails> GetGroupInfoRecursive(EbDataTable Table, EbDataVisualization Visualisation, CultureInfo _user_culture, bool _multipleLevelGrouping = false, int levelIterator=1, int parentLevel=0)
        {
            string AfterText = "After", BeforeText = "Before";
            ///The list containing details of preprocessed elements.
            ///
            List<GroupingDetails> RowGroupingLevelsDetails = new List<GroupingDetails>();
            ///The list of group strings in a level.
            ///Each element here is saved into each element of RowGroupingLevelsDetails as LevelText property.
            List<string> GroupsList = new List<string>();
            ///<summary>
            /// Total number of levels in the row grouping.
            ///</summary>
            int TotalLevels = (Visualisation as EbTableVisualization).CurrentRowGroup.RowGrouping.Count;

            try
            {
                ///The object that stores the previous grouping element for comparison. 
                ///Based on this changes in grouping at the current level can be identified.
                GroupingDetails PreviousGrouping = new GroupingDetails();
                string _previousGroupingText = string.Empty;
                //CurLevel.LevelCount = 0;
                for (int i = 0; i < Table.Rows.Count; i++)
                {
                    ///The object that stores the details of the currently processed row from the data table.
                    ///Later passed on to PreviousGrouping object once it is processed and acquires new data to process.
                    GroupingDetails CurLevel = new GroupingDetails();

                    CurLevel.RowIndex = i;//set row index to the param. At first call it's always 0.
                    CurLevel.CurrentLevel = levelIterator;//Represents the current level for multiple RG. Default is set to 1. Changed during recursion
                    CurLevel.InsertionType = BeforeText;
                    CurLevel.ParentLevel = parentLevel;//Represents parent level. Default is 0. Incremented in recursion.
                    CurLevel.ParentLevelText = string.Empty;//Used to detect level change. Will be updated only when the parent level text is changed in data table.
                    CurLevel.Html = string.Empty;//html content generated to be passed to front end. Can be either header/footer. Header needs to be updated to 
                    string TempGroupingText = string.Empty, TempParentLevelText = string.Empty;
                    int textsIndex = 0;
                    foreach (DVBaseColumn Column in (Visualisation as EbTableVisualization).CurrentRowGroup.RowGrouping)
                    {
                        if (textsIndex>=levelIterator)
                        {
                            textsIndex++;
                            GroupsList.Add((Table.Rows[i][Column.Data].ToString().Trim() == "") ? "(Blank)" : Table.Rows[i][Column.Data].ToString().Trim());
                            CurLevel.LevelText = (Table.Rows[i][Column.Data].ToString().Trim() == "") ? "(Blank)" : Table.Rows[i][Column.Data].ToString().Trim();//Gets the current column value
                            TempGroupingText += (Table.Rows[i][Column.Data].ToString().Trim() == "") ? "(Blank)" : Table.Rows[i][Column.Data].ToString().Trim();//gets combination of both current and parent column value
                            break;
                        }
                    }

                    int tempIndex = TempGroupingText.IndexOf(CurLevel.LevelText);
                    TempParentLevelText = TempGroupingText.Substring(0, (tempIndex));
                    CurLevel.ParentLevelText = TempParentLevelText;
                    CurLevel.LevelText = GroupsList[i];

                    //if RowGroupingLevelsDetails is empty we need to create a new level and new grouping.
                    if (RowGroupingLevelsDetails.Count == 0)
                    {
                        RowGroupingLevelsDetails.Add(new GroupingDetails()
                        {
                            CurrentLevel = CurLevel.CurrentLevel,
                            RowIndex = CurLevel.RowIndex,
                            LevelText = CurLevel.LevelText,
                            ParentLevelText = CurLevel.ParentLevelText,
                            InsertionType = BeforeText,
                            LevelCount = 1,//s(_multipleLevelGrouping && TotalLevels > levelIterator) ? (CurLevel.LevelCount + 1) : CurLevel.LevelCount,
                            ParentLevel = CurLevel.ParentLevel,
                            Html = GetHeaderRecursive(CurLevel, GroupsList,
                                1/*TotalLevels*/, (Visualisation as EbTableVisualization).CurrentRowGroup),
                            IsHeader = true,
                            GroupingCount = CurLevel.GroupingCount
                        });
                        RowGroupingLevelsDetails.Last().LevelCount += 1;
                    }
                    else//if RowGroupingLevelsDetails is not empty we check its elements to see which has to be updated.
                    {
                        //if the parent level text+current grouping text combination is not the same for previous row and same row, we create new row grouping
                        //if the parent level texts of previous level and current level are not same, we increment the level
                        if (_previousGroupingText.Equals(TempGroupingText) == false)
                        {
                            if(RowGroupingLevelsDetails.Last().IsHeader)
                                RowGroupingLevelsDetails.Last().Html = UpdateHeaderRecursive(RowGroupingLevelsDetails.Last());
                            RowGroupingLevelsDetails.Add(new GroupingDetails(CurLevel));
                            RowGroupingLevelsDetails.Last().Html = GetHeaderRecursive(CurLevel, GroupsList, 1/*TotalLevels*/, (Visualisation as EbTableVisualization).CurrentRowGroup);
                            GroupingDetails NewLevel = new GroupingDetails(CurLevel);
                            NewLevel.IsHeader = false;
                            NewLevel.InsertionType = BeforeText;
                            NewLevel.Html = GetFooterRecursive(NewLevel);
                            RowGroupingLevelsDetails.Add(new GroupingDetails(NewLevel));
                        }
                        else
                        {
                            CurLevel.LevelCount++;
                            //RowGroupingLevelsDetails.Last().LevelCount += 1;
                        }
                    }
                    _previousGroupingText = TempGroupingText;
                    //RowGroupingLevelsDetails.Last().LevelCount += 1;
                    PreviousGrouping = new GroupingDetails(CurLevel);
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("Exception Thrown in Row Grouping: " + ex);
            }
            return RowGroupingLevelsDetails;
        }

        public LevelInfoCollection GetGroupInfoRecursiveTest(EbDataTable Table, EbDataVisualization Visualisation, CultureInfo _user_culture, bool _multipleLevelGrouping = false, string Last = null, int LevelCount = 1)//TO DO: set defaults
        {
            List<RowGroupParent> RowGroupColl = (Visualisation as EbTableVisualization).RowGroupCollection;
            LevelInfoCollection Levels = new LevelInfoCollection();
            var _colCount = Visualisation.Columns.Count;
            Dictionary<int, double> IntIndex = new Dictionary<int, double>();
            foreach (DVBaseColumn _column in Visualisation.Columns)
            {
                if (_column.Type == EbDbTypes.Int32 || _column.Type == EbDbTypes.Int64 || _column.Type == EbDbTypes.Decimal || _column.Type == EbDbTypes.Int16)
                    IntIndex.Add(_column.Data, 0);
            }

            string _last = Last;
            int _savedindex = 0;
            int _lastrow = 0;
            int count = 0;

            LevelInfoCollection rec = null;
            bool _isNextMultipleLevelGrouping = false;
            if (_multipleLevelGrouping)
            {
                if (RowGroupColl.Count >= LevelCount)
                    _isNextMultipleLevelGrouping = true;
            }

            for (int i = 0; i < Table.Rows.Count; i++, count++)
            {
                string _new_colData = string.Empty;
                foreach (DVBaseColumn col in RowGroupColl[0].RowGrouping)
                    _new_colData += (Table.Rows[i][col.Data].ToString().Trim() == "") ? "(Blank)" : Table.Rows[i][col.Data].ToString().Trim();

                if (_new_colData.Trim() != _last) // new group
                {
                    LevelInfo _lvl = Levels.Update(_savedindex, count, 1);
                    if (_lvl != null)
                    {
                        if (_multipleLevelGrouping)
                            rec = GetGroupInfoRecursiveTest(Table, Visualisation, _user_culture,_isNextMultipleLevelGrouping, Last, LevelCount + 1);
                        UpdateHeaderHtml(_lvl, 1, 1);
                        Levels.Add(new LevelInfo()
                        {
                            RowIndex = i,
                            GroupString = GetFooterHtml(IntIndex, Visualisation, _user_culture)
                        });
                    }

                    count = 0;

                    if (_multipleLevelGrouping)
                    {
                        LevelInfo _lastlvl = Levels.Update(_savedindex, count, 1);
                        if (_lastlvl != null)
                        {
                            UpdateHeaderHtml(_lastlvl, 1, 1);
                            Levels.Add(new LevelInfo()
                            {
                                RowIndex = _lastrow,
                                GroupString = GetFooterHtml(IntIndex, Visualisation, _user_culture),
                                Type = "After"
                            });
                        }
                        //return Levels;
                    }
                    //else
                    //{
                    Levels.Add(new LevelInfo()
                    {
                        RowIndex = i,
                        GroupString = GetHeaderHtml(_new_colData, _colCount),
                        Count = count
                    });
                    //}

                    var IntegerKeys = IntIndex.Keys.ToList<int>();

                    foreach (var Key in IntegerKeys)
                    {
                        IntIndex[Key] = Convert.ToDouble(Table.Rows[i][Key]);
                    }
                }
                else //same group
                {
                    var IntegerKeys = IntIndex.Keys.ToList<int>();

                    foreach (var Key in IntegerKeys)
                    {
                        IntIndex[Key] += Convert.ToDouble(Table.Rows[i][Key]);
                    }
                }

                _last = _new_colData;
                _lastrow = i;
                if (count == 0)
                    _savedindex = i;
            }
            if (!_multipleLevelGrouping)
            {
                LevelInfo _lastlvl = Levels.Update(_savedindex, count, 1);
                if (_lastlvl != null)
                {
                    UpdateHeaderHtml(_lastlvl, 1, 1);
                    Levels.Add(new LevelInfo()
                    {
                        RowIndex = _lastrow,
                        GroupString = GetFooterHtml(IntIndex, Visualisation, _user_culture),
                        Type = "After"
                    });
                }
                return Levels;
            }
            return null;
        }

        public LevelInfoCollection GetGroupInfoSingleLevel(EbDataTable _table, EbDataVisualization _dv, CultureInfo _user_culture)
        {
            RowGroupParent _currentGroup = (_dv as EbTableVisualization).CurrentRowGroup;
            Dictionary<int, string> _dict = new Dictionary<int, string>();
            var Colcount = _dv.Columns.Count;
            List<string> groupList = new List<string>();
            LevelInfoCollection _levels = new LevelInfoCollection();
            Dictionary<int, double> IntIndex = new Dictionary<int, double>();

            foreach (DVBaseColumn col in _dv.Columns)
            {
                if (col.Type == EbDbTypes.Int32 || col.Type == EbDbTypes.Int64 || col.Type == EbDbTypes.Decimal || col.Type == EbDbTypes.Int16)
                    IntIndex.Add(col.Data, 0);
            }

            string _last = null;
            int _savedindex = 0;
            int _lastrow = 0;
            int count = 0;
            for (int i = 0; i < _table.Rows.Count; i++, count++)
            {
                string _new_colData = string.Empty;
                groupList.Clear();
                foreach (DVBaseColumn col in _currentGroup.RowGrouping)
                {
                    groupList.Add((_table.Rows[i][col.Data].ToString().Trim() == "") ? "(Blank)" : _table.Rows[i][col.Data].ToString().Trim());
                    _new_colData += (_table.Rows[i][col.Data].ToString().Trim() == "") ? "(Blank)" : _table.Rows[i][col.Data].ToString().Trim();
                }

                if (_new_colData.Trim() != _last) // new group
                {
                    LevelInfo _lvl = _levels.Update(_savedindex, count,1);
                    if (_lvl != null)
                    {
                        UpdateHeaderHtml(_lvl, 1, 1);
                        _levels.Add(new LevelInfo()
                        {
                            Level = 1,
                            RowIndex = i,
                            GroupString = GetFooterHtml(IntIndex, _dv, _user_culture)
                        });
                    }

                    count = 0;

                    _levels.Add(new LevelInfo()
                    {
                        Level = 1,
                        RowIndex = i,
                        GroupString = GetHeaderHtml(_new_colData, Colcount, 1, null, _currentGroup, groupList),
                        Count = count
                    });

                    var IntegerKeys = IntIndex.Keys.ToList<int>();

                    foreach (var Key in IntegerKeys)
                    {
                        IntIndex[Key] = Convert.ToDouble(_table.Rows[i][Key]);
                    }
                }
                else //same group
                {
                    var IntegerKeys = IntIndex.Keys.ToList<int>();

                    foreach (var Key in IntegerKeys)
                    {
                        IntIndex[Key] += Convert.ToDouble(_table.Rows[i][Key]);
                    }
                }

                _last = _new_colData;
                _lastrow = i;
                if (count == 0)
                    _savedindex = i;
            }

            LevelInfo _lastlvl = _levels.Update(_savedindex, count, 1);
            if (_lastlvl != null)
            {
                UpdateHeaderHtml(_lastlvl, 1, 1);
                _levels.Add(new LevelInfo()
                {
                    RowIndex = _lastrow,
                    GroupString = GetFooterHtml(IntIndex, _dv, _user_culture),
                    Type = "After"
                });
            }

            return _levels;
        }

        public LevelInfoCollection GetGroupInfoMultiLevel(EbDataTable _table, EbDataVisualization _dv,CultureInfo _user_culture)
        {
            RowGroupParent _currentGroup = (_dv as EbTableVisualization).CurrentRowGroup;
            Dictionary<int, string> _dict = new Dictionary<int, string>();
            var Colcount = _dv.Columns.Count;

            int MaxLevel = _currentGroup.RowGrouping.Count;

            LevelInfoCollection _levels = new LevelInfoCollection();
            Dictionary<int, double> IntIndex = new Dictionary<int, double>();

            foreach (DVBaseColumn col in _dv.Columns)
            {
                if (col.Type == EbDbTypes.Int32 || col.Type == EbDbTypes.Int64 || col.Type == EbDbTypes.Decimal || col.Type == EbDbTypes.Int16)
                    IntIndex.Add(col.Data, 0);
            }
            LevelInfoCollection PreviousLevel = new LevelInfoCollection();
            LevelInfoCollection CurrentLevel = new LevelInfoCollection(); 
            string _last = null;
            int _savedindex = 0;
            int _lastrow = 0;
            int count = 0;
            int rowColCount = 0;
            foreach (DVBaseColumn col in _currentGroup.RowGrouping)
            {
                rowColCount++;
                _savedindex = 0;
                _lastrow = 0;
                count = 0;
                _last = null;
                PreviousLevel.Clear();
                CurrentLevel.Clear();
                for (int i = 0; i < _table.Rows.Count; i++, count++)
                {
                    string _new_colData = string.Empty;

                    _new_colData += (_table.Rows[i][col.Data].ToString().Trim() == "") ? "(Blank)" : _table.Rows[i][col.Data].ToString().Trim();

                    if (_new_colData.Trim() != _last) // new group
                    {
                        LevelInfo _lvl = _levels.Update(_savedindex, count, rowColCount);
                        if (_lvl != null)
                        {
                            UpdateHeaderHtml(_lvl, MaxLevel, rowColCount);
                            _levels.Add(new LevelInfo()
                            {
                                Level = rowColCount,
                                RowIndex = i-1,
                                GroupString = GetFooterHtml(IntIndex, _dv, _user_culture, rowColCount),
                                Type = "After",
                            });
                        }

                        count = 0;

                        _levels.Add(new LevelInfo()
                        {
                            Level = rowColCount,
                            RowIndex = i,
                            GroupString = GetHeaderHtml(_new_colData, Colcount, rowColCount, col, null),
                            Count = count,
                            LevelText = _new_colData
                        });

                        CurrentLevel.Add(new LevelInfo()
                        {
                            Level = rowColCount,
                            RowIndex = i,
                            GroupString = GetHeaderHtml(_new_colData, Colcount, rowColCount, col, null),
                            Count = count,
                            LevelText = _new_colData
                        });

                        var IntegerKeys = IntIndex.Keys.ToList<int>();

                        foreach (var Key in IntegerKeys)
                        {
                            IntIndex[Key] = Convert.ToDouble(_table.Rows[i][Key]);
                        }
                    }
                    else //same group
                    {
                        int inx = (i == 1) ? 0 : i; 
                        LevelInfo Pre_lvl = _levels.PreviousLevelCheck(inx, rowColCount-1);
                        LevelInfo Cur_lvl = null;
                        if (rowColCount > 1)
                             Cur_lvl = _levels.CurrentLevelDataCheck(rowColCount, _new_colData);

                        if (Pre_lvl != null)
                         PreviousLevel.Add(Pre_lvl);
                        if (Cur_lvl != null)
                            CurrentLevel.Add(Cur_lvl);

                        if (PreviousLevel.Count == 2 && CurrentLevel.Count == 1)
                        {
                            //if (_savedindex != 0)
                            //    count--;
                            //else
                            //    count--;
                            LevelInfo _lvl = _levels.Update(_savedindex, count, rowColCount);
                            //if(_lvl != null)
                            //{
                            UpdateHeaderHtml(_lvl, MaxLevel, rowColCount);
                            //}
                            PreviousLevel.RemoveAt(0);
                            CurrentLevel.RemoveAt(0);
                            _levels.Add(new LevelInfo()
                            {
                                Level = rowColCount,
                                RowIndex = i - 1,
                                GroupString = GetFooterHtml(IntIndex, _dv, _user_culture, rowColCount),
                                Type = "After",
                            });
                            count = 0;
                            _levels.Add(new LevelInfo()
                            {
                                Level = rowColCount,
                                RowIndex = i,
                                GroupString = GetHeaderHtml(_new_colData, Colcount, rowColCount, col, null),
                                Count = count,
                                LevelText = _new_colData,
                                Type = "Before",
                            });

                            CurrentLevel.Add(new LevelInfo()
                            {
                                Level = rowColCount,
                                RowIndex = i,
                                GroupString = GetHeaderHtml(_new_colData, Colcount, rowColCount, col, null),
                                Count = count,
                                LevelText = _new_colData
                            });

                            var _IntegerKeys = IntIndex.Keys.ToList<int>();

                            foreach (var Key in _IntegerKeys)
                            {
                                IntIndex[Key] = Convert.ToDouble(_table.Rows[i][Key]);
                            }
                        }
                        else
                        {
                            var IntegerKeys = IntIndex.Keys.ToList<int>();

                            foreach (var Key in IntegerKeys)
                            {
                                IntIndex[Key] += Convert.ToDouble(_table.Rows[i][Key]);
                            }
                        }
                    }

                    _last = _new_colData;
                    _lastrow = i;
                    if (count == 0)
                        _savedindex = i;
                }

                LevelInfo _lastlvl = _levels.Update(_savedindex, count, rowColCount);
                if (_lastlvl != null)
                {
                    UpdateHeaderHtml(_lastlvl, MaxLevel, rowColCount);
                    _levels.Add(new LevelInfo()
                    {
                        Level = rowColCount,
                        RowIndex = _lastrow,
                        GroupString = GetFooterHtml(IntIndex, _dv, _user_culture, rowColCount),
                        Type = "After"
                    });
                }
                if (PreviousLevel.Count == 1 && CurrentLevel.Count == 1)
                {
                    LevelInfo _final_lvl = _levels.Update(_savedindex, count+1, rowColCount);
                    UpdateHeaderHtml(_final_lvl, MaxLevel, rowColCount);
                    //_levels.Add(new LevelInfo()
                    //{
                    //    Level = rowColCount,
                    //    RowIndex = _lastrow,
                    //    GroupString = GetFooterHtml(IntIndex, _dv, _user_culture),
                    //    Type = "After"
                    //});
                }
            }

            //int MaxLevel = _currentGroup.RowGrouping.Count, _preLevel = 0;
            //Dictionary<int, Dictionary<int, int>> LevelCount = new Dictionary<int, Dictionary<int, int>>();//LevelCount[Level][RowIndex][Count]

            //foreach (LevelInfo level in _levels)
            //{
            //    if (level.Level == MaxLevel)
            //        continue;
            //    else
            //    {
            //        if(LevelCount.ContainsKey(level.Level))
            //        {
            //            foreach(int rowIndex in LevelCount[level.Level].Keys.ToList())
            //            {
            //                if(rowIndex == level.RowIndex)
            //                {
            //                    LevelCount[level.Level][level.RowIndex] += 1;
            //                }
            //                else
            //                {
            //                    if(!LevelCount[level.Level].Keys.ToList().Contains(level.RowIndex))
            //                    //    LevelCount[level.Level][level.RowIndex] += 1;
            //                    //else
            //                        LevelCount[level.Level].Add(level.RowIndex, 1);
            //                }
            //            }
            //           //if(LevelCount[level.Level].Keys.ToList().Contains(level.RowIndex))
            //           // {
            //           //     LevelCount[level.Level][level.RowIndex] += 1;
            //           // }
            //           //else
            //           // {
            //           //     LevelCount[level.Level].Add(level.RowIndex, 1);
            //           // }
            //        }
            //        else
            //        {
            //            LevelCount.Add(level.Level, new Dictionary<int, int>());

            //            LevelCount[level.Level].Add(level.RowIndex, 1);
            //        }
            //    }
            //    _preLevel = level.Level;
            //}

            for (int j = 0; j < _levels.Count; j += 2)
            {
                if (_levels[j].Level < _currentGroup.RowGrouping.Count)
                {
                    int r1 = _levels[j].RowIndex;
                    int r2 = _levels[j + 1].RowIndex;
                    _levels[j].Count = _levels.GetCount(r1, r2, (_levels[j].Level + 1));
                    UpdateHeaderHtml(_levels[j], 1, 1);
                }
            }

            return _levels;
        }

        public string GetHeaderHtml(string _htmlString, int _Colcount, int level=1, DVBaseColumn col = null, RowGroupParent currentGroup = null, List<string> groupList =null)
        {
            var str = "<tr class='group' group='"+ level + "'>";
            for (var i = 0; i <level; i++)
                str += "<td> &nbsp;</td>";
            string tempstr = string.Empty; 

            if (col != null)//multiple
                tempstr = GetStringFromColumn(col, _htmlString);
            else// single
            {
                int i = -1;
                foreach (DVBaseColumn CurCol in currentGroup.RowGrouping) {
                    i++;
                    tempstr += GetStringFromColumn(CurCol, _htmlString, groupList[i]);
                }
            }

            str += "<td><i class='fa fa-minus-square-o' style='cursor:pointer;'></i></td><td colspan=" + _Colcount + ">" + tempstr;
            return str;
        }

        public string GetStringFromColumn(DVBaseColumn col, string _htmlString, string currentString = null)
        {
            string ColumnString = string.Empty;
            if (col.LinkRefId != null)
                ColumnString = col.sTitle + ": <b data-rowgroup='true' data-colname='" + col.Name + "' data-coltype='" + col.Type + "+'' data-data='" + ((currentString != null) ? currentString : _htmlString) + "' ><a href = '#' oncontextmenu='return false' class='tablelink' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' tabindex='0'>" + _htmlString + "</a></b>";
            else
                ColumnString = col.sTitle + ": <b data-rowgroup='true' data-colname='" + col.Name + "' data-coltype='" + col.Type + "' data-data='" + ((currentString != null) ? currentString : _htmlString) + "'>" + ((currentString != null) ? currentString : _htmlString) + "</b>";

            return ColumnString;
        }


        public void UpdateHeaderHtml(LevelInfo _level, int maxlevel, int curlevel)
        {
            if (maxlevel == curlevel) 
                _level.GroupString += "(" + _level.Count + ")</td></tr>";
        }

        public string GetFooterHtml(Dictionary<int, double> _coll, EbDataVisualization _dv, CultureInfo _user_culture, int level = 1)
        {
            var str = "<tr class='group-sum' group="+level+">";

            foreach (DVBaseColumn col in (_dv as EbTableVisualization).CurrentRowGroup.RowGrouping)
                str += "<td>&nbsp;</td>";

            if ((_dv as EbTableVisualization).CurrentRowGroup.GetType().Name == "MultipleLevelRowGroup")
                str += "<td>&nbsp;</td>";

            str += "<td>&nbsp;</td>";//serial column

            foreach (DVBaseColumn col in (_dv as EbTableVisualization).Columns)
            {
                var cults = col.GetColumnCultureInfo(_user_culture);
                if (col.bVisible)
                {
                    if ((col is DVNumericColumn) && (col as DVNumericColumn).Aggregate)
                        str += "<td class='dt-body-right'>" + Convert.ToDecimal(_coll[col.Data]).ToString("N",cults.NumberFormat) + "</td>";
                    else
                        str += "<td>&nbsp;</td>";
                }
            }
            return str + "</tr>";
        }

        [CompressResponse]
        public DataSourceDataResponse Any(InlineTableDataRequest request)
        {
            DataSourceDataResponse dsresponse = null;

            var _ds = this.Redis.Get<EbDataSource>(request.RefId);
            string _sql = string.Empty;

            if (_ds == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                Redis.Set<EbDataSource>(request.RefId, _ds);
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

            dsresponse = new DataSourceDataResponse
            {
                Data = _dataset.Tables[0].Rows,
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

