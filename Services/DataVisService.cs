using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects.DVRelated;
using ExpressBase.Objects.Objects.ReportRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using ServiceStack;
using ServiceStack.Logging;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
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
            _Tags = _Tags.Remove(0,1);

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
                            _cond = _cond.Substring(0,place);
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
                    _sql = sql1 + tempsql ;
                }
            }
            bool _isPaged = false;

            string __order = string.Empty;
            if (request.OrderBy != null && request.OrderBy.Count >0)
            {
                foreach(OrderBy order in request.OrderBy)
                {
                    __order += string.Format("{0} {1},", order.Column, (order.Direction == 2) ? "DESC" : "ASC");
                }
                int indx = __order.LastIndexOf(",");
                __order = __order.Substring(0, indx);
            }
            _sql = _sql.Replace(":orderby",(string.IsNullOrEmpty(__order)) ? "1" : __order);

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
                Int32.TryParse(_dataset.Tables[_dataset.Tables.Count-1].Rows[0][0].ToString(), out _recordsTotal);
                Int32.TryParse(_dataset.Tables[_dataset.Tables.Count - 1].Rows[0][0].ToString(), out _recordsFiltered);
            }
            _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[_dataset.Tables.Count - 1].Rows.Count;
            _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[_dataset.Tables.Count - 1].Rows.Count;
            //-- 
            if (_dataset.Tables.Count > 0)
            {
                _dataset = PreProcessing(_dataset, _dV);
            }
            dsresponse = new DataSourceDataResponse
            {
                Draw = request.Draw,
                Data =  _dataset.Tables[0].Rows,
                RecordsTotal = _recordsTotal,
                RecordsFiltered = _recordsFiltered,
                Ispaged = _isPaged
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

        public EbDataSet PreProcessing(EbDataSet _dataset, EbDataVisualization _dv)
        {
            var colCount =  _dataset.Tables[0].Columns.Count;
            Dictionary<string, int> dict = new Dictionary<string, int>();
            foreach (DVBaseColumn col in _dv.Columns)
            {
                if (col.Formula != null && col.Formula != "")
                {
                    string[] _dataFieldsUsed;
                    Script valscript = CSharpScript.Create<dynamic>(col.Formula, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic"), globalsType: typeof(Globals));
                    valscript.Compile();
                    _dataset.Tables[0].Columns.RemoveAt(colCount - 1);// rownum deleted for oracle
                    _dataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = col.Data, ColumnName = col.Name, Type = col.Type });
                    for (int i = 0; i < _dataset.Tables[0].Rows.Count; i++)
                    {
                        Globals globals = new Globals();
                        var matches = Regex.Matches(col.Formula, @"T[0-9]{1}.\w+").OfType<Match>().Select(m => m.Groups[0].Value).Distinct();
                        _dataFieldsUsed = new string[matches.Count()];
                        int j = 0;
                        foreach (var match in matches)
                            _dataFieldsUsed[j++] = match;
                        foreach (string calcfd in _dataFieldsUsed)
                        {
                            string TName = calcfd.Split('.')[0];
                            string fName = calcfd.Split('.')[1];
                            globals[TName].Add(fName, new NTV { Name = fName, Type = _dataset.Tables[0].Columns[fName].Type, Value = _dataset.Tables[0].Rows[i][fName] });
                        }
                    _dataset.Tables[0].Rows[i][col.Data] = valscript.RunAsync(globals).Result.ReturnValue.ToString();
                    }
                }
            }

            return _dataset;
        }
    }
}

