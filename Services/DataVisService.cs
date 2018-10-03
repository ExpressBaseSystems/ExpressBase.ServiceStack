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
            //List<GroupingDetails> _levels = new List<GroupingDetails>();
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

        public EbDataTable PreProcessing(ref EbDataSet _dataset, EbDataVisualization _dv, User _user, ref List< GroupingDetails> _levels)
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
                    //_levels = GetGroupInfoSingleLevel(_dataset.Tables[0], _dv, _user_culture);
                    _levels = RowGroupingCommon(_dataset.Tables[0], _dv, _user_culture, false);
                else if ((_dv as EbTableVisualization).CurrentRowGroup.GetType().Name == "MultipleLevelRowGroup")
                    //_levels = GetGroupInfoMultiLevel(_dataset.Tables[0], _dv, _user_culture);//GetGroupInfoMultiLevel(_dataset.Tables[0], _dv, _user_culture);
                    _levels = RowGroupingCommon(_dataset.Tables[0], _dv, _user_culture, true);
            }


            return _formattedTable;
        }

        public List<GroupingDetails> RowGroupingCommon(EbDataTable Table, EbDataVisualization Visualization, CultureInfo Culture, bool IsMultiLevelRowGrouping=false)
        {
            Dictionary<string, GroupingDetails> RowGrouping = new Dictionary<string, GroupingDetails>();
            const string AfterText = "After", BeforeText = "Before", BlankText= "(Blank)";
            int finalHeaderIndex = 0;
            int TotalLevels = (IsMultiLevelRowGrouping) ? (Visualization as EbTableVisualization).CurrentRowGroup.RowGrouping.Count : 1,
            TotalColumnCount = (Visualization as EbTableVisualization).Columns.Count;
            Dictionary<int, int> LevelCount = new Dictionary<int, int>();

            List<int> AggregateColumnIndexes = new List<int>();
            foreach (DVBaseColumn _column in Visualization.Columns)
            {
                if (_column is DVNumericColumn && (_column as DVNumericColumn).Aggregate)
                    AggregateColumnIndexes.Add(_column.Data);
            }
            List<DVBaseColumn> RowGroupingColumns = new List<DVBaseColumn>((Visualization as EbTableVisualization).CurrentRowGroup.RowGrouping);

            string PreviousGroupingText = string.Empty;
            
            int CurrentLevel = 0;
            int GroupingCount = 1;
            int RowIndex = 0;
            List<string> GroupingTexts = new List<string>();
            EbDataRow previousRow = new EbDataRow();
            int previousLevel = 0;

            for (int i = 0; i < Table.Rows.Count; i++)
            {
                EbDataRow currentRow = Table.Rows[i];

                string TempGroupingText = string.Empty;
                GroupingTexts.Clear();
                int delimCount = 1;

                foreach (DVBaseColumn Column in RowGroupingColumns)
                {
                    string tempValue = Table.Rows[i][Column.Data].ToString().Trim();

                    TempGroupingText += (tempValue == string.Empty) ? BlankText : tempValue;
                    TempGroupingText += (delimCount == TotalLevels) ? string.Empty : ":-:";
                    delimCount++;
                    GroupingTexts.Add((tempValue == string.Empty) ? BlankText : tempValue);
                }

                if (TempGroupingText.Equals(PreviousGroupingText) == false)
                {
                    GroupingCount = 1;
                    RowIndex = i;
                    
                    if (i > 0)
                    {
                        List<string> TempRowGroupingKeys = CreateRowGroupingKeys(previousRow, RowGroupingColumns, IsMultiLevelRowGrouping);
                        if (IsMultiLevelRowGrouping)
                            CurrentLevel = GetCurrentLevel(TempGroupingText, PreviousGroupingText, false, i, TotalLevels, IsMultiLevelRowGrouping);
                        else
                            CurrentLevel = 1;
                        
                        if (CurrentLevel == 0)
                        {
                            ForcePushFirstLevelFooter(TotalLevels, PreviousGroupingText, AggregateColumnIndexes, ref RowGrouping, Visualization,
                                Culture);//(PreviousGroupingText, AggregateColumnIndexes, ref RowGrouping);
                        }
                        InitializeFooter(previousRow, AggregateColumnIndexes, RowGrouping, Visualization, Culture, 
                            BeforeText, TotalLevels, TotalColumnCount, CurrentLevel, i, IsMultiLevelRowGrouping, GroupingTexts);
                            finalHeaderIndex = RowGrouping.Count - 1;
                        if (IsMultiLevelRowGrouping)
                        {
                            CalculateLevelCount(RowGrouping, TempGroupingText, PreviousGroupingText);
                        }
                    }

                    if (i < Table.Rows.Count - 1)
                    {
                        CreateHeaderAndFooterPairs(currentRow, AggregateColumnIndexes,
                            RowGroupingColumns, GroupingTexts, RowGrouping, Visualization, CurrentLevel, TotalLevels, IsMultiLevelRowGrouping, Culture);
                        InitializeHeader(currentRow, GroupingTexts, GroupingCount, RowIndex,
                            CurrentLevel, BeforeText, RowGroupingColumns, IsMultiLevelRowGrouping,
                            RowGrouping, TotalColumnCount, TotalLevels);
                    }
                }
                else if (TempGroupingText.Equals(PreviousGroupingText) == true)
                {
                    List<string> TempRowGroupingKeys = CreateRowGroupingKeys(currentRow, RowGroupingColumns, IsMultiLevelRowGrouping);
                    foreach (var TempGroupingKey in TempRowGroupingKeys)
                    {
                        if (RowGrouping.ContainsKey("H_" + TempGroupingKey))
                        {
                            RowGrouping["H_" + TempGroupingKey].GroupingCount++;
                            foreach (var index in AggregateColumnIndexes)
                            {
                                if (RowGrouping.ContainsKey("F_" + TempGroupingKey))
                                    (RowGrouping["F_" + TempGroupingKey] as FooterGroupingDetails).Aggregations[index].SetValue(Convert.ToDecimal(currentRow[index]));
                            }

                            if (i == Table.Rows.Count - 1)
                            {
                                CurrentLevel = GetCurrentLevel(TempGroupingText, PreviousGroupingText, true, i, TotalLevels,
                                    IsMultiLevelRowGrouping);
                                
                                if (CurrentLevel == 0 && IsMultiLevelRowGrouping)
                                {
                                    ForcePushFirstLevelFooter(TotalLevels, PreviousGroupingText, AggregateColumnIndexes, ref RowGrouping, Visualization,
                                        Culture);
                                }
                                InitializeFooter(currentRow, AggregateColumnIndexes, RowGrouping, Visualization,
                                    Culture, AfterText, TotalLevels, TotalColumnCount, CurrentLevel, i, IsMultiLevelRowGrouping, GroupingTexts);
                            }
                        }
                    }
                }

                PreviousGroupingText = TempGroupingText;
                previousRow = currentRow;
                previousLevel = CurrentLevel;
            }
            return RowGrouping.Values.ToList();
        }
        
        private void CalculateLevelCount(Dictionary<string, GroupingDetails> RowGrouping, string CurrentGroupingText, 
            string PreviousGroupingText)
        {
            string ChangedLevelKey = GetKeyForChangedLevel(CurrentGroupingText, PreviousGroupingText);
            if(RowGrouping.ContainsKey(ChangedLevelKey))
            {
                RowGrouping[ChangedLevelKey].LevelCount++;
            }
        }

        private string GetKeyForChangedLevel(string CurrentGroupingText, string PreviousGroupingText)
        {
            string ChangedKey = "H_";
            int i = 0, StringIndex = 0;

            string[] CurrentStringTokens = CurrentGroupingText.Split(":-:");
            string[] PreviousStringTokens = PreviousGroupingText.Split(":-:");
            for(i = 0; i < CurrentStringTokens.Length; i++)
            {
                if(PreviousStringTokens[i].Equals(CurrentStringTokens[i]) == false)
                {
                    break;
                }
            }

            if (i > 0)
            {
                for (StringIndex = i; StringIndex > 0; StringIndex--)
                {
                    ChangedKey += PreviousStringTokens[i - 1];
                }
            }
            if(i==0)
            {
                for (StringIndex = i; StringIndex >= 0; StringIndex--)
                {
                    ChangedKey += PreviousStringTokens[i];
                }
            }

            return ChangedKey;
        }

        private void ForcePushFirstLevelFooter(int TotalLevels, string PreviousGroupingText, List<int> AggregateIndexes, ref Dictionary<string, GroupingDetails> RowGrouping, EbDataVisualization Visualization, CultureInfo culture)
        {
            string FooterKey = string.Empty;

            FooterKey = "F_" + PreviousGroupingText.Split(":-:")[0];
            if(!RowGrouping.ContainsKey(FooterKey))
                RowGrouping.Add(FooterKey, new FooterGroupingDetails(TotalLevels, AggregateIndexes, Visualization, culture));
        }

        private void CreateHeaderAndFooterPairs(EbDataRow CurrentRow, List<int> AggregateIndexes, 
            List<DVBaseColumn> RowGroupingColumns, List<string>GroupingTexts, Dictionary<string, GroupingDetails> RowGrouping, EbDataVisualization Visualization, 
            int CurrentLevel, int TotalLevels, bool IsMultiLevelGrouping, CultureInfo culture)
        {
            List<string> TempKey = CreateRowGroupingKeys(CurrentRow, RowGroupingColumns, (TotalLevels > 1) ? true : false);
            if (IsMultiLevelGrouping)
            {
                for (int j = 0; j < TotalLevels; j++)
                {
                    if (!RowGrouping.ContainsKey("H_" + TempKey[j]))
                    {
                        RowGrouping.Add("H_" + TempKey[j], new HeaderGroupingDetails());
                        RowGrouping["H_" + TempKey[j]].GroupingCount++;
                        RowGrouping["H_" + TempKey[j]].CurrentLevel = CurrentLevel;
                        (RowGrouping["H_" + TempKey[j]] as HeaderGroupingDetails).GroupingTexts = new List<string>(GroupingTexts);
                    }
                }

                if (CurrentLevel == 0)
                {
                    RowGrouping.Add("F_" + TempKey[CurrentLevel + 1], new FooterGroupingDetails(TotalLevels, AggregateIndexes, Visualization, culture));
                }
                else
                {
                    RowGrouping.Add("F_" + TempKey[CurrentLevel], new FooterGroupingDetails(TotalLevels, AggregateIndexes, Visualization, culture));
                }
            }
            else
            {
                RowGrouping.Add("H_" + TempKey[0], new HeaderGroupingDetails());
                RowGrouping["H_" + TempKey[0]].GroupingCount++;
                RowGrouping.Add("F_" + TempKey[0], new FooterGroupingDetails(TotalLevels, AggregateIndexes, Visualization, culture));
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
                    TempKey.Add(((TempKey.Count > 0) ? TempKey.Last() : string.Empty) + CurrentRow[column.Data]);
                }
                else
                {
                    TempStr += CurrentRow[column.Data];
                }
            }
            if (!IsMultiLevelRowGrouping)
            {
                TempKey.Add(TempStr);
            }

            return TempKey;
        }

        private void InitializeFooter(EbDataRow currentRow, List<int> AggregateColumnIndexes, 
            Dictionary<string, GroupingDetails> RowGrouping, EbDataVisualization Visualization,
            CultureInfo Culture, string Text, int TotalLevels, int TotalColumnCount, 
            int CurrentLevel, int TableRowIndex, bool IsMultiLevelGrouping, List<string> GroupingTexts)
        {
            List<DVBaseColumn> RowGroupingColumns = new List<DVBaseColumn>((Visualization as EbTableVisualization).CurrentRowGroup.RowGrouping);
            List<string> TempKey = new List<string>();
            string TempKeyBuilder = string.Empty;
            if (IsMultiLevelGrouping)
            {
                foreach (var column in RowGroupingColumns)
                {
                    TempKeyBuilder += currentRow[column.Data];
                    TempKey.Add("F_" + TempKeyBuilder);
                }
            }
            else
            {
                foreach (var column in RowGroupingColumns)
                {
                    TempKeyBuilder += currentRow[column.Data];
                }
                TempKey.Add("F_" + TempKeyBuilder);
            }
            if (IsMultiLevelGrouping)
            {
                for (int j = TotalLevels - 1; j >= CurrentLevel; j--)
                {
                    if (RowGrouping.Keys.Contains(TempKey[j]))
                    {
                        var FooterObject = (RowGrouping[TempKey[j]] as FooterGroupingDetails);
                        foreach (int columnKey in FooterObject.Aggregations.Keys)
                        {
                            FooterObject.Aggregations[columnKey].SetValue(Convert.ToDecimal(currentRow[columnKey]));
                        }

                        FooterObject.GroupingTexts = GroupingTexts;
                        FooterObject.InsertionType = Text;
                        FooterObject.RowIndex = TableRowIndex;
                    }
                }
            }
            else
            {
                if (RowGrouping.Keys.Contains(TempKey[0]))
                {
                    var FooterObject = (RowGrouping[TempKey[0]] as FooterGroupingDetails);
                    foreach (int columnKey in FooterObject.Aggregations.Keys)
                    {
                        FooterObject.Aggregations[columnKey].SetValue(Convert.ToDecimal(currentRow[columnKey]));
                    }

                    FooterObject.GroupingTexts = GroupingTexts;
                    FooterObject.InsertionType = Text;
                    FooterObject.RowIndex = TableRowIndex;
                }
            }
        }

        public int GetCurrentLevel(string CurrentString, string PreviousString, bool isEnd, int currentRowIndex, int totalLevels, bool IsMultiLevel)
        {
            if (IsMultiLevel)
            {
                if (!isEnd)
                {
                    string[] CurrentSplit = CurrentString.Split(":-:");
                    string[] PreviousSplit = PreviousString.Split(":-:");
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

        public void InitializeHeader(EbDataRow currentRow, List<string> GroupingTexts, int GroupingCount,
            int RowIndex, int CurrentLevel, string InsertionType, List<DVBaseColumn> RowGroupingColumns,
            bool IsMultiLevelRowGrouping, Dictionary<string, GroupingDetails> groupings, int ColumnCount,
            int TotalLevels)
        {
            List<string> TempKey = new List<string>();
            string TempKeyBuilder = string.Empty;

            if (IsMultiLevelRowGrouping)
            {
                foreach (var column in RowGroupingColumns)
                {
                    TempKeyBuilder += currentRow[column.Data];
                    TempKey.Add("H_" + TempKeyBuilder);
                }
            }
            else
            {
                foreach (var column in RowGroupingColumns)
                {
                    TempKeyBuilder += currentRow[column.Data];
                }
                TempKey.Add("H_" + TempKeyBuilder);
            }

            if (IsMultiLevelRowGrouping)
            {
                for (int j = CurrentLevel; j < TotalLevels; j++)
                {
                    groupings[TempKey[j]].InsertionType = "Before";
                    groupings[TempKey[j]].RowIndex = RowIndex;
                    groupings[TempKey[j]].CurrentLevel = j + 1;
                    groupings[TempKey[j]].ColumnCount = ColumnCount;
                    groupings[TempKey[j]].IsMultiLevel = true;
                    (groupings[TempKey[j]] as HeaderGroupingDetails).TotalLevels = TotalLevels;
                    (groupings[TempKey[j]] as HeaderGroupingDetails).GroupingTexts = new List<string>(GroupingTexts);
                }
            }
            else
            {
                for (int j = (CurrentLevel == 0) ? CurrentLevel : CurrentLevel - 1; j <= TotalLevels && TempKey.IndexOf(TempKey.Last()) >= j; j++)
                {
                    groupings[TempKey[j]].InsertionType = "Before";
                    groupings[TempKey[j]].RowIndex = RowIndex;
                    groupings[TempKey[j]].CurrentLevel = j + 1;
                    groupings[TempKey[j]].ColumnCount = ColumnCount;
                    groupings[TempKey[j]].IsMultiLevel = false;
                    (groupings[TempKey[j]] as HeaderGroupingDetails).TotalLevels = TotalLevels;
                    (groupings[TempKey[j]] as HeaderGroupingDetails).GroupingTexts = new List<string>(GroupingTexts);
                }
            }
        }

        //public string UpdateHeader(EbDataRow currentRow, GroupingDetails GroupingObject, int TotalLevels, bool IsMultiLevelGrouping, List<string> GroupingTexts, int GroupingCount,
        //    int RowIndex, int CurrentLevel, string InsertionType, List<DVBaseColumn> RowGroupingColumns,
        //    bool IsMultiLevelRowGrouping, Dictionary<string, GroupingDetails> groupings, int ColumnCount)
        //{
        //            string ret = (GroupingObject as HeaderGroupingDetails).Html + ": " + ((IsMultiLevelGrouping)? ((GroupingObject.CurrentLevel==TotalLevels-1)?GroupingObject.GroupingCount:GroupingObject.LevelCount+1): GroupingObject.GroupingCount).ToString() + "</td></tr>";
        //    return ret;
        //}

        //public string GetFooter(FooterGroupingDetails FooterObject, EbDataVisualization Visualization,
        //    CultureInfo Culture, int ColumnsCount, int TotalLevels, int CurrentLevel,
        //    string FooterText="")
        //{
        //    string RowFooter = "<tr class='group-sum' group=" + CurrentLevel + ">"; ;
        //    for (int i = 0; i < TotalLevels; i++)
        //    {
        //        RowFooter += "<td>&nbsp;</td>";
        //    }
        //    if (TotalLevels > 1)
        //    {
        //        RowFooter += "<td>&nbsp;</td>";
        //    }
        //    RowFooter += "<td>&nbsp;</td>";//serial column
        //    //
        //    foreach (DVBaseColumn col in (Visualization as EbTableVisualization).Columns)
        //    {
        //        var ColumnCulture = col.GetColumnCultureInfo(Culture);
        //        if (col.bVisible)
        //        {
        //            if ((col is DVNumericColumn) && (col as DVNumericColumn).Aggregate)
        //                RowFooter += "<td class='dt-body-right'>" + (FooterObject.Aggregations[col.Data].Sum).ToString("N", ColumnCulture.NumberFormat) + "</td>";
        //            else
        //                RowFooter += "<td>&nbsp;</td>";
        //        }
        //    }
        //    return RowFooter + "</tr>";
        //}

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

