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
using System.Dynamic;
using ExpressBase.ServiceStack.Services;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using System.Drawing;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects.Helpers;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Drawing;
using DocumentFormat.OpenXml.Spreadsheet;
using A = DocumentFormat.OpenXml.Drawing;
using Xdr = DocumentFormat.OpenXml.Drawing.Spreadsheet;
using A14 = DocumentFormat.OpenXml.Office2010.Drawing;
using System.Drawing.Imaging;
using Font = DocumentFormat.OpenXml.Spreadsheet.Font;
using Fonts = DocumentFormat.OpenXml.Spreadsheet.Fonts;
using Fill = DocumentFormat.OpenXml.Spreadsheet.Fill;
using Color = DocumentFormat.OpenXml.Spreadsheet.Color;
using ExpressBase.Objects.WebFormRelated;

namespace ExpressBase.ServiceStack
{
    [Authenticate]
    public class DataVisService : EbBaseService
    {
        private const string HeaderPrefix = "H_", FooterPrefix = "F_", GroupDelimiter = ":-:", AfterText = "After", BeforeText = "Before", BlankText = "(Blank)";

        private Eb_Solution _ebSolution = null;

        private bool _replaceEbColumns = true;

        private ResponseStatus _Responsestatus = new ResponseStatus();

        private Dictionary<int, object> IntermediateDic = new Dictionary<int, object>();

        public DataVisService(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc) : base(_dbf, _sfc) { }

        private string TableId = null;

        private bool Modifydv = true;

        private List<DVBaseColumn> dependencyTable = null;

        private EbDataSet dataset = null;

        private int CurLocId = 0;

        List<FileMetaInfo> _ImageList = new List<FileMetaInfo>();

        EbDataSet _approvaldata = null;

        EbDataVisualization _dV = null;

        int ExcelRowcount = 1;

        List<Param> Inpuparams = new List<Param>();

        List<TFilters> TableFilters = new List<TFilters>();

        List<DVBaseColumn> ExcelColumns = new List<DVBaseColumn>();

        bool showCheckboxColumn = false;

        SheetData partSheetData = new SheetData();

        WorksheetPart worksheetPart1 = null;

        WorkbookPart workbookPart1 = null;

        MergeCells mergeCells = null;

        PivotConfig _pivotConfig = null;

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
                this.showCheckboxColumn = request.showCheckboxColumn;
                this.FileClient.BearerToken = request.Token;
                this.FileClient.RefreshToken = request.rToken;
                _ebSolution = request.eb_Solution;
                this.TableId = request.TableId;
                Modifydv = request.Modifydv;
                this.Log.Info("data request");
                CurLocId = request.LocId;
                if (request.TFilters != null)
                    TableFilters = request.TFilters;

                //_dV = request.EbDataVisualization;
                if (!string.IsNullOrWhiteSpace(request.dvRefId))
                {
                    _dV = EbFormHelper.GetEbObject<EbDataVisualization>(request.dvRefId, null, this.Redis, this);
                }
                else if (request.DataVizObjString != null)
                {
                    _dV = EbSerializers.Json_Deserialize<EbDataVisualization>(request.DataVizObjString);
                    request.DataVizObjString = null;
                }
                else if (request.EbDataVisualization != null)
                {
                    _dV = request.EbDataVisualization;
                }
                else if (_dV is null)
                {
                    throw new Exception("Data Visualization object is null.");
                }
                if (request.CurrentRowGroup != null && _dV is EbTableVisualization _tV)
                    _tV.CurrentRowGroup = EbSerializers.Json_Deserialize<RowGroupParent>(request.CurrentRowGroup);

                request.UserInfo = GetUserObject(request.UserAuthId);

                DataSourceDataResponse dsresponse = null;
                //this._replaceEbColumns = request.ReplaceEbColumns;
                EbDataReader _ds = null;
                EbDataSet _dataset = null;
                EbApi _api = null;
                bool _isPaged = false;
                if (_dV != null && _dV.IsDataFromApi)
                    _dataset = GetDatafromUrl();
                else if (request.RefId != string.Empty && request.RefId != null)
                    _ds = this.Redis.Get<EbDataReader>(request.RefId);
                else if (_dV.Sql != null && _dV.Sql != string.Empty)
                {
                    _ds = new EbDataReader { Sql = _dV.Sql };
                    request.Params.AddRange(_dV.ParamsList);
                }
                else if (_dV.ApiRefId != null && _dV.ApiRefId != string.Empty)
                {
                    _api = this.Redis.Get<EbApi>(_dV.ApiRefId);
                    var dsrefid = _api.Resources.First(res => res is EbSqlReader).Reference;
                    _pivotConfig = (_api.Resources.First(res => res is EbPivotTable) as EbPivotTable).Pivotconfig;
                    _ds = this.Redis.Get<EbDataReader>(dsrefid);
                }

                if (!_dV.IsDataFromApi)
                {
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
                        if (!(_dV is EbCalendarView) && (request.Params == null || request.Params.Count == 2))
                        {
                            if (request.Params.Count == 2)
                            {
                                request.Params = request.Params.Concat(_dsf.GetDefaultParams()).ToList();
                            }
                            else
                            {
                                request.Params = _dsf.GetDefaultParams();
                            }
                        }
                    }
                    string _sql = string.Empty;
                    string tempsql = string.Empty;
                    if (request.Source == "Calendar")
                    {
                        _sql = _ds.Sql.Split(';')[1];
                    }
                    else if (_ds != null && !(_dV is EbCalendarView))
                    {
                        string _c = string.Empty;
                        DVBaseColumn Treecol = null;
                        DVBaseColumn AutoResolve = null;
                        if (_dV is EbTableVisualization)
                        {
                            Treecol = this.Check4Tree((_dV as EbTableVisualization));
                        }

                        if (request.TFilters != null && request.TFilters.Count > 0)
                        {
                            foreach (TFilters _dic in request.TFilters)
                            {
                                string _cond = string.Empty;
                                var op = _dic.Operator.Trim(); var col = _dic.Column; var val = _dic.Value; var type = _dic.Type;
                                var array = _dic.Value.Split("|");
                                AutoResolve = _dV.Columns.Find(x => x.Name == col && x.AutoResolve);
                                if (AutoResolve != null)
                                {
                                    string _auto = string.Empty;
                                    for (int i = 0; i < array.Length; i++)
                                    {
                                        if (array[i].Trim() != "")
                                        {
                                            col = AutoResolve.ColumnQueryMapping.DisplayMember[0].Name;
                                            if (op == "x*")
                                                _auto += string.Format(" LOWER({0}) LIKE LOWER('{1}%') OR", col, val);
                                            else if (op == "*x")
                                                _auto += string.Format(" LOWER({0}) LIKE LOWER('%{1}') OR", col, val);
                                            else if (op == "*x*")
                                                _auto += string.Format(" LOWER({0}) LIKE LOWER('%{1}%') OR ", col, val);
                                            else if (op == "=")
                                                _auto += string.Format(" LOWER({0}) = LOWER('{1}') OR", col, val);
                                            else
                                                _auto += string.Format(" {0} {1} '{2}' OR", col, op, val);
                                        }
                                    }
                                    int _place = _auto.LastIndexOf("OR");
                                    string ccc = _auto.Substring(0, _place);
                                    array = GetValue4Columns(_dV as EbTableVisualization, ccc).Split(",");
                                    if (array[0].Trim() == "")
                                        _cond += string.Format(" {0} = '{1}' OR", _dic.Column, 0);
                                    else
                                        op = "=";
                                }
                                else if (col == "eb_created_by" || col == "eb_lastmodified_by" || col == "eb_loc_id")
                                {
                                    List<string> templist = new List<string>();
                                    if (col == "eb_created_by" || col == "eb_lastmodified_by")
                                    {
                                        if (this._ebSolution.Users != null)
                                        {
                                            for (int i = 0; i < array.Length; i++)
                                            {
                                                if (array[i].Trim() != "")
                                                {
                                                    if (op == "x*")
                                                        templist.AddRange(this._ebSolution.Users.Where(pair => pair.Value.ToLower().StartsWith(array[i].Trim().ToLower())).Select(pair => pair.Key.ToString()).ToList());
                                                    else if (op == "*x")
                                                        templist.AddRange(this._ebSolution.Users.Where(pair => pair.Value.ToLower().EndsWith(array[i].Trim().ToLower())).Select(pair => pair.Key.ToString()).ToList());
                                                    else if (op == "*x*")
                                                        templist.AddRange(this._ebSolution.Users.Where(pair => pair.Value.ToLower().Contains(array[i].Trim().ToLower())).Select(pair => pair.Key.ToString()).ToList());
                                                    else if (op == "=")
                                                        templist.AddRange(this._ebSolution.Users.Where(pair => pair.Value.ToLower() == array[i].Trim().ToLower()).Select(pair => pair.Key.ToString()).ToList());

                                                }
                                            }
                                        }

                                    }
                                    else
                                    {
                                        for (int i = 0; i < array.Length; i++)
                                        {
                                            if (array[i].Trim() != "")
                                            {
                                                if (op == "x*")
                                                    templist.AddRange(this._ebSolution.Locations.Where(pair => pair.Value.ShortName.ToLower().StartsWith(array[i].Trim().ToLower())).Select(pair => pair.Key.ToString()).ToList());
                                                else if (op == "*x")
                                                    templist.AddRange(this._ebSolution.Locations.Where(pair => pair.Value.ShortName.ToLower().EndsWith(array[i].Trim().ToLower())).Select(pair => pair.Key.ToString()).ToList());
                                                else if (op == "*x*")
                                                    templist.AddRange(this._ebSolution.Locations.Where(pair => pair.Value.ShortName.ToLower().Contains(array[i].Trim().ToLower())).Select(pair => pair.Key.ToString()).ToList());
                                                else if (op == "=")
                                                    templist.AddRange(this._ebSolution.Locations.Where(pair => pair.Value.ShortName.ToLower() == array[i].Trim().ToLower()).Select(pair => pair.Key.ToString()).ToList());
                                            }
                                        }
                                    }
                                    array = templist.ToArray();
                                    if (array.Length == 0)
                                        _cond += string.Format(" {0} = '{1}' OR", _dic.Column, 0);
                                    op = "=";
                                }
                                col = _dic.Column;
                                for (int i = 0; i < array.Length; i++)
                                {
                                    if (array[i].Trim() != "")
                                    {
                                        if (type == EbDbTypes.String)
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
                                                if (type == EbDbTypes.Date || type == EbDbTypes.DateTime)
                                                    _cond += string.Format(" {0} {1} date '{2}' OR", col, op, array[i].Trim());
                                                else
                                                    _cond += string.Format(" {0} {1} '{2}' OR", col, op, array[i].Trim());
                                            }
                                            else if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.MYSQL)
                                            {
                                                if (type == EbDbTypes.Date || type == EbDbTypes.DateTime)
                                                    _cond += string.Format(" CAST({0} AS date) {1} '{2}' OR", col, op, array[i].Trim());
                                                else
                                                    _cond += string.Format(" {0} {1} '{2}' OR", col, op, array[i].Trim());
                                            }
                                            else
                                            {
                                                if (type == EbDbTypes.Date || type == EbDbTypes.DateTime)
                                                    _cond += string.Format(" {0}::date {1} '{2}' OR", col, op, array[i].Trim());
                                                else
                                                    _cond += string.Format(" {0} {1} '{2}' OR", col, op, array[i].Trim());
                                            }
                                        }
                                    }
                                }
                                int place = _cond.LastIndexOf("OR");
                                _cond = _cond.Substring(0, place);
                                _c += "AND (" + _cond + ")";
                            }
                        }
                        _sql = _ds.Sql;
                        if (Treecol == null)
                        {
                            if (!_ds.Sql.ToLower().Contains("@and_search") || !_ds.Sql.ToLower().Contains(":and_search"))
                            {
                                _ds.Sql = "SELECT * FROM (" + _ds.Sql + "\n ) data WHERE 1=1 :and_search order by :orderby";
                            }
                            _ds.Sql = _ds.Sql.Replace(";", string.Empty);
                            _sql = _ds.Sql.Replace(":and_search", _c).Replace("@and_search", _c) + ";";
                            //}
                            if (request.Ispaging || request.Length > 0)
                            {
                                var matches = Regex.Matches(_sql, @"\;\s*SELECT\s*COUNT\(\*\)\s*FROM");
                                if (matches.Count == 0)
                                {
                                    tempsql = _sql.Replace(";", string.Empty);
                                    tempsql = "SELECT COUNT(*) FROM (" + tempsql + ") data1;";
                                }

                                var sql1 = _sql.Replace(";", string.Empty);
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
                            if (string.IsNullOrEmpty(__order))
                                _sql = _sql.Replace("order by :orderby", string.Empty);
                            else
                                _sql = _sql.Replace(":orderby", __order);

                            _isPaged = (_sql.ToLower().Contains(":offset") && _sql.ToLower().Contains(":limit"));
                        }
                        else
                        {
                            string pattern = $"(?i)(order by {Treecol.ParentColumn[0].Name})";
                            var matches = Regex.Matches(_sql, pattern);
                            if (matches.Count == 0)
                            {
                                string _columnorder = string.Empty;
                                if (Treecol.NeedAlphabeticOrder)
                                    _columnorder = ", " + Treecol.Name;
                                _sql = $"SELECT * FROM ({_sql.Replace(";", string.Empty)}) data ORDER BY {Treecol.ParentColumn[0].Name} {_columnorder}";
                            }

                        }
                    }
                    else
                    {
                        _sql = _ds.Sql;
                    }

                    if (request.Params == null)
                        _sql = _sql.Replace(":id", "0");
                    //}
                    var parameters = DataHelper.GetParams(this.EbConnectionFactory.ObjectsDB, _isPaged, request.Params, request.Length, request.Start);
                    Console.WriteLine("Before :  " + DateTime.Now);
                    var dtStart = DateTime.Now;
                    Console.WriteLine("................................................dataviz datarequest start " + DateTime.Now);
                    try
                    {
                        Inpuparams = SqlHelper.GetSqlParams(_sql, 2);
                        _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());
                    }
                    catch (Exception e)
                    {
                        Log.Info("Datviz Qurey Exception........." + e.StackTrace);
                        Log.Info("Datviz Qurey Exception........." + e.Message);
                        this._Responsestatus.Message = e.Message;
                        return new DataSourceDataResponse { error = "Qurey Exception : " + e.Message };
                    }
                    Console.WriteLine("................................................dataviz datarequest end " + DateTime.Now);
                    var dtstop = DateTime.Now;
                    Console.WriteLine("..................................totaltimeinSeconds" + dtstop.Subtract(dtStart).Seconds);
                    if (request.RefId != null && GetLogEnabled(request.RefId))
                    {
                        TimeSpan T = _dataset.EndTime - _dataset.StartTime;
                        InsertExecutionLog(_dataset.RowNumbers, T, _dataset.StartTime, request.UserId, request.Params, request.RefId);
                    }
                    Console.WriteLine(DateTime.Now);
                    var dtEnd = DateTime.Now;
                    var ts = (dtEnd - dtStart).TotalMilliseconds;
                    Console.WriteLine("final:::" + ts);
                }

                if (_dataset != null && _dataset.Tables.Count > 0)
                {
                    int _recordsTotal = 0, _recordsFiltered = 0;
                    if (_isPaged)
                    {
                        Int32.TryParse(_dataset.Tables[_dataset.Tables.Count - 1].Rows[0][0].ToString(), out _recordsTotal);
                        Int32.TryParse(_dataset.Tables[_dataset.Tables.Count - 1].Rows[0][0].ToString(), out _recordsFiltered);
                    }
                    _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[_dataset.Tables.Count - 1].Rows.Count;
                    _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[_dataset.Tables.Count - 1].Rows.Count;
                    //-- 

                    PrePrcessorReturn ReturnObj = null;
                    List<GroupingDetails> _levels = new List<GroupingDetails>();
                    if (_dataset.Tables.Count > 0 && _dV != null)
                    {
                        try
                        {
                            if (_dV is EbCalendarView)
                            {
                                DateTime x = DateTime.Now;
                                ReturnObj = PreProcessingCalendarViewNew(ref _dataset, request.Params, ref _dV, request.UserInfo);
                                DateTime y = DateTime.Now;
                                var diff = y - x;
                                Console.WriteLine("new: " + diff);
                                // ReturnObj = PreProcessingCalendarView(ref _dataset, request.Params, ref _dV, request.UserInfo);
                            }
                            else if (_dV.ApiRefId != null && _dV.ApiRefId != string.Empty)
                                ReturnObj = PreProcessingPivot(ref _dataset, request.Params, ref _dV, request.UserInfo);
                            else
                                ReturnObj = PreProcessing(ref _dataset, request.Params, _dV, request.UserInfo, ref _levels, request.IsExcel);
                        }
                        catch (Exception e)
                        {
                            Log.Info("Call to PreProcessing ----" + e.StackTrace);
                            Log.Info("Call to PreProcessing ----" + e.Message);
                            this._Responsestatus.Message = e.Message;
                        }
                    }

                    List<string> _permission = new List<string>();
                    if (request.dvRefId != null)
                        _permission = PermissionCheck(request.UserInfo, request.dvRefId);
                    dsresponse = new DataSourceDataResponse
                    {
                        Draw = request.Draw,
                        Data = (ReturnObj?.rows != null) ? ReturnObj.rows : _dataset.Tables[0].Rows,
                        FormattedData = (ReturnObj?.FormattedTable != null) ? ReturnObj.FormattedTable.Rows : null,
                        RecordsTotal = _recordsTotal,
                        RecordsFiltered = _recordsFiltered,
                        Ispaged = _isPaged,
                        Levels = _levels,
                        Permission = _permission,
                        Summary = ReturnObj?.Summary,
                        excel_file = ReturnObj?.excel_file,
                        TableName = _dataset.Tables[0].TableName,
                        Tree = ReturnObj?.tree,
                        ResponseStatus = this._Responsestatus,
                        ReturnObjString = EbSerializers.Json_Serialize(_dV),
                        ImageList = JsonConvert.SerializeObject(_ImageList)
                    };
                    this.Log.Info(" dataviz dataresponse*****" + dsresponse.Data);
                    EbSerializers.Json_Serialize(dsresponse);
                    return dsresponse;
                }
                else
                {
                    Log.Info("Datviz Dataset Empty .........");
                    return new DataSourceDataResponse { error = "Datviz Dataset Empty........." };
                }
            }
            catch (Exception e)
            {
                Log.Info("Datviz service Exception........." + e.StackTrace);
                Log.Info("Datviz service Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
                return new DataSourceDataResponse { error = e.Message };
            }
        }

        private EbDataSet GetDatafromUrl()
        {
            var _service = base.ResolveService<ApiConversionService>();
            var result = (ApiConversionResponse)_service.Any(new ApiConversionRequest() { Url = _dV.Url, Method = _dV.Method, Parameters = _dV.ParamsList, Headers = _dV.Headers });
            return result.dataset;
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
                    var parameters = DataHelper.GetParams(this.EbConnectionFactory.ObjectsDB, _isPaged, request.Params, 0, 0);

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
                if (col is DVStringColumn && _dv.AutoGen && col.Name == "eb_action")
                    _dataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = col.Data, ColumnName = col.Name, Type = col.Type });
                else if (col.IsCustomColumn)
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
                    try
                    {
                        object __value = null;
                        var __partType = _datarow.Table.Columns[formulaPart.FieldName].Type;
                        if (__partType == EbDbTypes.Decimal || __partType == EbDbTypes.Int32)
                            __value = (_datarow[formulaPart.FieldName] != DBNull.Value) ? _datarow[formulaPart.FieldName] : 0;
                        else
                            __value = _datarow[formulaPart.FieldName];

                        globals[formulaPart.TableName].Add(formulaPart.FieldName, new NTV { Name = formulaPart.FieldName, Type = __partType, Value = __value });
                    }
                    catch (Exception e)
                    {
                        Log.Info("customCol.FormulaParts........." + e.StackTrace + "Column Name...." + customCol.Name);
                        Log.Info("customCol.FormulaParts........." + e.Message + "Column Name...." + customCol.Name);
                        this._Responsestatus.Message = e.Message;
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info("CustomColumDoCalc4Row........." + e.StackTrace + "Column Name...." + customCol.Name);
                Log.Info("CustomColumDoCalc4Row........." + e.Message + "Column Name...." + customCol.Name);
                this._Responsestatus.Message = e.Message;
            }

            try
            {
                if (customCol is DVNumericColumn)
                    result = Convert.ToDecimal(customCol.GetCodeAnalysisScript().RunAsync(globals).Result.ReturnValue);
                else if (customCol is DVBooleanColumn)
                    result = Convert.ToBoolean(customCol.GetCodeAnalysisScript().RunAsync(globals).Result.ReturnValue);
                else if (customCol is DVDateTimeColumn)
                    result = Convert.ToDateTime(customCol.GetCodeAnalysisScript().RunAsync(globals).Result.ReturnValue);
                else
                    result = customCol.GetCodeAnalysisScript().RunAsync(globals).Result.ReturnValue.ToString();
            }
            catch (Exception e)
            {
                Log.Info("CustomColumDoCalc4Row Script Exception........." + e.StackTrace + "Column Name...." + customCol.Name);
                Log.Info("CustomColumDoCalc4Row Script Exception........." + e.Message + "Column Name...." + customCol.Name);
                this._Responsestatus.Message = e.Message;
            }

            _datarow[customCol.Name] = result;
        }

        public void GetDictonaries4Columns(EbDataVisualization _dv)
        {
            foreach (DVBaseColumn col in _dv.Columns)
            {
                if (col.ColumnQueryMapping != null)
                {
                    GetDictonaries4AutoResolveColumn(col);

                }
            }
        }

        public void GetDictonaries4AutoResolveColumn(DVBaseColumn col)
        {
            try
            {
                if (col.AutoResolve && !col.ColumnQueryMapping.DataSourceId.IsNullOrEmpty())
                {
                    EbDataReader dr = this.Redis.Get<EbDataReader>(col.ColumnQueryMapping.DataSourceId);
                    if (dr == null || dr.Sql == null || dr.Sql == string.Empty)
                    {
                        var myService = base.ResolveService<EbObjectService>();
                        EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest { RefId = col.ColumnQueryMapping.DataSourceId });
                        dr = EbSerializers.Json_Deserialize(result.Data[0].Json);
                        Redis.Set<EbDataReader>(col.ColumnQueryMapping.DataSourceId, dr);
                    }
                    //string _name = string.Join(",", col.ColumnQueryMapping.DisplayMember.Select(obj => obj.Name));
                    string _name = col.ColumnQueryMapping.DisplayMember[0].Name;
                    col.ColumnQueryMapping.Values = this.EbConnectionFactory.ObjectsDB.GetDictionary(dr.Sql, _name, col.ColumnQueryMapping.ValueMember.Name);
                }
            }
            catch (Exception e)
            {
                Log.Info("GetDictonaries4AutoResolveColumn in datatable Exception........." + e.StackTrace + "Column Name....." + col.Name);
                Log.Info("GetDictonaries4AutoResolveColumn in datatable Exception........." + e.Message + "Column Name....." + col.Name);
                this._Responsestatus.Message = e.Message;
            }
        }

        public string GetValue4Columns(EbDataVisualization _dv, string cond)
        {
            foreach (DVBaseColumn col in _dv.Columns)
            {
                if (col.ColumnQueryMapping != null)
                {
                    return GetValues4AutoResolveColumn(col, cond);

                }
            }
            return string.Empty;
        }

        public string GetValues4AutoResolveColumn(DVBaseColumn col, string cond)
        {
            try
            {
                if (col.AutoResolve && !col.ColumnQueryMapping.DataSourceId.IsNullOrEmpty())
                {
                    EbDataReader dr = this.Redis.Get<EbDataReader>(col.ColumnQueryMapping.DataSourceId);
                    if (dr == null || dr.Sql == null || dr.Sql == string.Empty)
                    {
                        var myService = base.ResolveService<EbObjectService>();
                        EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest { RefId = col.ColumnQueryMapping.DataSourceId });
                        dr = EbSerializers.Json_Deserialize(result.Data[0].Json);
                        Redis.Set<EbDataReader>(col.ColumnQueryMapping.DataSourceId, dr);
                    }
                    return string.Join(",", this.EbConnectionFactory.ObjectsDB.GetAutoResolveValues(dr.Sql, col.ColumnQueryMapping.ValueMember.Name, cond).ToArray());
                }
            }
            catch (Exception e)
            {
                Log.Info("GetValues4AutoResolveColumn in datatable Exception........." + e.StackTrace + "Column Name....." + col.Name);
                Log.Info("GetValues4AutoResolveColumn in datatable Exception........." + e.Message + "Column Name....." + col.Name);
                this._Responsestatus.Message = e.Message;
            }
            return string.Empty;
        }

        public PrePrcessorReturn PreProcessing(ref EbDataSet _dataset, List<Param> Parameters, EbDataVisualization _dv, User _user, ref List<GroupingDetails> _levels, Boolean _isexcel, bool isSQljob = false)
        {
            try
            {
                var _array = Inpuparams.Select(para => para.Name).ToArray();
                if (Parameters != null)
                    _dv.ParamsList = Parameters.FindAll(para => Array.IndexOf(_array, para.Name) > -1);
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
                this.GetDictonaries4Columns(_dv);
                EbDataTable _formattedTable = _dataset.Tables[0].GetEmptyTable();
                if (isSQljob)
                    _formattedTable.Columns.Add(_formattedTable.NewDataColumn(_dv.Columns.Count - 1, "action", EbDbTypes.String));
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
                int SerialCount = 0;
                bool isTree = false;
                FileInfo file = null;
                SpreadsheetDocument package = null;
                //ExcelWorksheet worksheet = null;
                MemoryStream ms = new MemoryStream();
                byte[] bytes = null;

                TreeData<EbDataRow> tree = new TreeData<EbDataRow>();
                dependencyTable = this.CreateDependencyTable(_dv);

                RowColletion rows = _dataset.Tables[0].Rows;
                if ((_dv as EbTableVisualization) != null)
                {
                    List<DVBaseColumn> ApprovalColumns = GetApprovalColumn(_dv as EbTableVisualization);
                    if (ApprovalColumns.Count > 0)
                    {
                        GetApprovalData(_user, ApprovalColumns, rows);
                    }
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
                        else// type change in old forms (rowgroup parent)
                            TotalLevels = 1;
                    }
                    string sFileName = _dv.DisplayName + ".xlsx";

                    if (_isexcel)
                    {
                        //file = PreExcelCalculation(sFileName);
                        package = SpreadsheetDocument.Create(ms, DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook);
                        CreatePartsForExcel(package);
                        //worksheet = package.Workbook.Worksheets.Add("Report");
                        //PreExcelAddHeader(ref worksheet, _dv);
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
                            DataTable2FormatedTable(Nodedr.Item, _dv, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, i, rows.Count, Nodedr.IsGroup, Nodedr.Level, isTree);
                            if (Nodedr.Children.Count > 0)
                            {
                                RecursiveGetTreeChilds(Nodedr, _dv, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, ref i, rows.Count, isTree);
                            }
                            i++;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < rows.Count; i++)
                        {
                            DataTable2FormatedTable(rows[i], _dv, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, i, rows.Count);
                            if (isRowgrouping)
                                DoRowGroupingCommon(rows[i], _dv, _user_culture, _user, ref _formattedTable, IsMultiLevelRowGrouping, ref RowGrouping, ref PreviousGroupingText, ref CurSortIndex, ref SerialCount, i, dvColCount, TotalLevels, ref AggregateColumnIndexes, ref RowGroupingColumns, rows.Count);
                        }
                    }

                    List<GroupingDetails> SortedGroupings = RowGrouping.Values.ToList();
                    SortedGroupings.Sort();
                    _levels = SortedGroupings;
                    if (_isexcel)
                    {
                        worksheetPart1.Worksheet.InsertAfter(mergeCells, worksheetPart1.Worksheet.Elements<SheetData>().First());
                        worksheetPart1.Worksheet.Save();
                        workbookPart1.Workbook.Save();
                        package.Close();
                        bytes = ms.ToArray();
                    }
                }
                else
                {
                    for (int i = 0; i < rows.Count; i++)
                    {
                        DataTable2FormatedTable(rows[i], _dv, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, i, rows.Count);

                    }
                }
                return new PrePrcessorReturn { FormattedTable = _formattedTable, Summary = Summary, excel_file = bytes, rows = rows, tree = tree.Tree };
            }
            catch (Exception e)
            {
                Log.Info("Before PreProcessing in datatable  Exception........." + e.StackTrace);
                Log.Info("Before PreProcessing in datatable  Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
            }
            return null;
        }

        private void CreatePartsForExcel(SpreadsheetDocument document)
        {
            //partSheetData = GenerateSheetdataForDetails();
            workbookPart1 = document.AddWorkbookPart();
            workbookPart1.Workbook = new Workbook();
            AddStyleSheet();
            worksheetPart1 = workbookPart1.AddNewPart<WorksheetPart>("rId1");
            worksheetPart1.Worksheet = new Worksheet();
            //ff
            CreateHeaderRowForExcel();
            createcolumns();
            worksheetPart1.Worksheet.Append(partSheetData);
            //GenerateWorksheetPartContent();
            GenerateWorkbookPartContent(document);
        }

        private void GenerateWorkbookPartContent(SpreadsheetDocument document)
        {
            Sheets sheets1 = new Sheets();
            Sheet sheet1 = new Sheet() { Name = "Sheet1", SheetId = (UInt32Value)1U, Id = document.WorkbookPart.GetIdOfPart(worksheetPart1) };
            sheets1.Append(sheet1);
            workbookPart1.Workbook.Append(sheets1);
        }

        //private void GenerateWorksheetPartContent()
        //{
        //    Worksheet worksheet1 = worksheetPart1.Worksheet;
        //    worksheet1.Append(partSheetData);
        //    worksheetPart1.Worksheet = worksheet1;
        //}

        public void createcolumns()
        {
            Columns columns = new Columns();
            for (var i = 1; i <= ExcelColumns.Count; i++)
                columns.Append(new Column() { Min = Convert.ToUInt32(i), Max = Convert.ToUInt32(i), Width = 25, CustomWidth = true });
            worksheetPart1.Worksheet.Append(columns);
        }

        public void CreateHeaderRowForExcel()
        {
            ExcelRowcount = 1;
            ExcelColumns = _dV.Columns.FindAll(col => col.bVisible && !(col is DVApprovalColumn) && !(col is DVActionColumn)).ToList();
            Row workRow = new Row();
            workRow.Append(CreateCell(_dV.DisplayName, 1U));
            partSheetData.Append(workRow);
            var char1 = GetExcelColumnName(ExcelColumns.Count);
            mergeCells = new MergeCells();
            mergeCells.Append(new MergeCell() { Reference = new StringValue($"A1:{char1}1") });
            for (var i = 1; i <= _dV.ParamsList.Count; i++)
            {
                workRow = new Row();
                workRow.Append(CreateCell(_dV.ParamsList[i - 1].Name + " = " + _dV.ParamsList[i - 1].Value, 3U));
                mergeCells.Append(new MergeCell() { Reference = new StringValue($"A{i + 1}:B{i + 1}") });
                ExcelRowcount++;
                partSheetData.Append(workRow);
            }
            for (var i = 1; i <= TableFilters.Count; i++)
            {
                ExcelRowcount++;
                var col = ExcelColumns.Find(_col => _col.Name == TableFilters[i - 1].Column);
                workRow = new Row();
                workRow.Append(CreateCell(col.sTitle + " " + TableFilters[i - 1].Operator + " " + TableFilters[i - 1].Value, 3U));
                mergeCells.Append(new MergeCell() { Reference = new StringValue($"A{ExcelRowcount}:B{ExcelRowcount}") });
                partSheetData.Append(workRow);
            }
            ExcelRowcount++;
            workRow = new Row();
            for (var i = 0; i < ExcelColumns.Count; i++)
            {
                workRow.Append(CreateCell(ExcelColumns[i].sTitle, 2U));
            }
            partSheetData.Append(workRow);
            ExcelRowcount++;
        }

        private string GetExcelColumnName(int columnNumber)
        {
            int dividend = columnNumber;
            string columnName = String.Empty;
            int modulo;

            while (dividend > 0)
            {
                modulo = (dividend - 1) % 26;
                columnName = Convert.ToChar(65 + modulo).ToString() + columnName;
                dividend = (int)((dividend - modulo) / 26);
            }

            return columnName;
        }

        private Cell CreateCell(string cellreference, string text)
        {
            Cell cell = new Cell { CellReference = cellreference };
            cell.StyleIndex = 3U;
            //cell.DataType = ResolveCellDataTypeOnValue(text);
            //cell.CellValue = new CellValue(text);
            return cell;
        }

        private Cell CreateCell(string text, uint styleIndex)
        {
            Cell cell = new Cell();
            cell.StyleIndex = styleIndex;
            cell.DataType = ResolveCellDataTypeOnValue(text);
            cell.CellValue = new CellValue(text);
            return cell;
        }

        private Cell CreateCellWithFormula(string cellreference, string _formula)
        {
            Cell cell = new Cell { CellReference = cellreference };
            CellFormula cellformula = new CellFormula();
            cellformula.Text = _formula;
            cell.StyleIndex = 3U;
            cell.Append(cellformula);
            return cell;
        }

        private EnumValue<CellValues> ResolveCellDataTypeOnValue(string text)
        {
            int intVal;
            double doubleVal;
            if (int.TryParse(text, out intVal) || double.TryParse(text, out doubleVal))
            {
                return CellValues.Number;
            }
            else
            {
                return CellValues.String;
            }
        }

        private WorkbookStylesPart AddStyleSheet()
        {
            WorkbookStylesPart stylesheet = workbookPart1.AddNewPart<WorkbookStylesPart>();

            Stylesheet workbookstylesheet = new Stylesheet();

            Font font0 = new Font();         // Default font

            Font font1 = new Font();
            font1.Append(new Bold());
            font1.Append(new FontSize() { Val = 15D });
            font1.Append(new FontName() { Val = "Calibri" });

            Font font2 = new Font();
            font2.Append(new Bold());
            font2.Append(new FontSize() { Val = 12D });
            font2.Append(new FontName() { Val = "Calibri" });

            Font font3 = new Font();
            font3.Append(new FontSize() { Val = 11D });
            font3.Append(new FontName() { Val = "Calibri" });



            Fonts fonts = new Fonts();      // <APENDING Fonts>
            fonts.Append(font0);
            fonts.Append(font1);
            fonts.Append(font2);
            fonts.Append(font3);

            Fills fills = new Fills();
            fills.Append(new Fill());

            Borders borders = new Borders();
            borders.Append(new Border());

            Alignment alignment = new Alignment()
            {
                Horizontal = HorizontalAlignmentValues.Center,
                Vertical = VerticalAlignmentValues.Center
            };

            Alignment alignment1 = new Alignment()
            {
                Horizontal = HorizontalAlignmentValues.Center,
                Vertical = VerticalAlignmentValues.Center
            };

            // <CellFormats>
            CellFormat cellformat0 = new CellFormat() { FontId = 0, FillId = 0, BorderId = 0 }; // Default style : Mandatory | Style ID =0
            CellFormat cellformat1 = new CellFormat() { FontId = 1, Alignment = alignment };  // Style with Bold text ; Style ID = 1
            CellFormat cellformat2 = new CellFormat() { FontId = 2, Alignment = alignment1 };  // Style with Bold text ; Style ID = 1
            CellFormat cellformat3 = new CellFormat() { FontId = 3 };  // Style with Bold text ; Style ID = 1

            // <APENDING CellFormats>
            CellFormats cellformats = new CellFormats();
            cellformats.Append(cellformat0);
            cellformats.Append(cellformat1);
            cellformats.Append(cellformat2);
            cellformats.Append(cellformat3);


            // Append FONTS, FILLS , BORDERS & CellFormats to stylesheet <Preserve the ORDER>
            workbookstylesheet.Append(fonts);
            workbookstylesheet.Append(fills);
            workbookstylesheet.Append(borders);
            workbookstylesheet.Append(cellformats);

            // Finalize
            stylesheet.Stylesheet = workbookstylesheet;
            stylesheet.Stylesheet.Save();

            return stylesheet;
        }

        public PrePrcessorReturn PreProcessingCalendarViewNew(ref EbDataSet _dataset, List<Param> Parameters, ref EbDataVisualization _dv, User _user)
        {
            try
            {
                DataStruct4CalView CalendarData = new DataStruct4CalView(_dv as EbCalendarView);
                EbDataSet tempdataset = new EbDataSet();
                Dictionary<string, DynamicObj> _hourCount = new Dictionary<string, DynamicObj>();
                Dictionary<int, List<object>> summary = new Dictionary<int, List<object>>();
                CalendarData.InitialColumnsCount = _dataset.Tables.Sum(x => x.Columns.Count);
                this.CreateCustomcolumn4Calendar(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount, ref summary);
                int _count = (_dv as EbCalendarView).DataColumns.FindAll(col => col.bVisible).Count;
                List<object> _list = new List<object>();
                for (int i = 0; i < _count; i++)
                {
                    _list.Add(0L);
                }
                foreach (var key in summary.Keys.ToList())
                {
                    summary[key] = new List<object>(_list);
                }
                EbDataTable _formattedTable = tempdataset.Tables[0].GetEmptyTable();
                _formattedTable.Columns.Add(_formattedTable.NewDataColumn(_dv.Columns.Count, "Total", EbDbTypes.Int32));
                summary.Add(summary.Keys.Last() + 1, new List<object>(_list));
                _dv.Columns.Add(new DVBaseColumn { Data = _dv.Columns.Count, Name = "Total", sTitle = "Total", Type = EbDbTypes.Int32, RenderType = EbDbTypes.Int32, bVisible = true, AggregateFun = AggregateFun.Sum });
                _formattedTable.Columns.Add(_formattedTable.NewDataColumn(_dv.Columns.Count, "serial", EbDbTypes.Int32));
                for (int i = CalendarData.InitialColumnsCount; i < _dv.Columns.Count; i++)
                {
                    CalendarData.Columns.Add(_dv.Columns[i] as CalendarDynamicColumn);
                }

                RowColletion MasterRows = _dataset.Tables[0].Rows;
                RowColletion LinesRows = _dataset.Tables[1].Rows;
                for (int i = 0; i < LinesRows.Count; i++)
                {
                    CalendarData.Add(LinesRows[i]);
                }
                CalendarData.GetFormatedTable(ref _formattedTable, MasterRows, ref summary, _user);
                return new PrePrcessorReturn { FormattedTable = _formattedTable, rows = MasterRows, Summary = summary };
            }
            catch (Exception e)
            {
                Console.WriteLine("PreProcessing calView New - Exception: " + e.Message + e.StackTrace);
            }
            return null;
        }
        public PrePrcessorReturn PreProcessingCalendarView(ref EbDataSet _dataset, List<Param> Parameters, ref EbDataVisualization _dv, User _user)
        {
            try
            {
                var _user_culture = CultureHelper.GetSerializedCultureInfo(_user.Preference.Locale).GetCultureInfo();

                var colCount = _dataset.Tables[0].Columns.Count;

                dataset = _dataset;
                EbDataSet tempdataset = new EbDataSet();
                Globals globals = new Globals();
                Dictionary<string, DynamicObj> _hourCount = new Dictionary<string, DynamicObj>();
                Dictionary<int, List<object>> summary = new Dictionary<int, List<object>>();
                this.CreateCustomcolumn4Calendar(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount, ref summary);
                int _count = (_dv as EbCalendarView).DataColumns.FindAll(col => col.bVisible).Count;
                List<object> _list = new List<object>();
                for (int i = 0; i < _count; i++)
                {
                    _list.Add(0);
                    _list.Add(0);
                }
                foreach (var key in summary.Keys.ToList())
                {
                    summary[key] = new List<object>(_list);
                }
                EbDataTable _formattedTable = tempdataset.Tables[0].GetEmptyTable();
                _formattedTable.Columns.Add(_formattedTable.NewDataColumn(_dv.Columns.Count, "Total", EbDbTypes.Int32));
                summary.Add(_dv.Columns.Count, new List<object>(_list));
                _dv.Columns.Add(new DVBaseColumn { Data = _dv.Columns.Count, Name = "Total", sTitle = "Total", Type = EbDbTypes.Int32, RenderType = EbDbTypes.Int32, bVisible = true, AggregateFun = AggregateFun.Sum });
                _formattedTable.Columns.Add(_formattedTable.NewDataColumn(_dv.Columns.Count, "serial", EbDbTypes.Int32));
                RowColletion MasterRows = _dataset.Tables[0].Rows;
                RowColletion LinesRows = _dataset.Tables[1].Rows;
                DVBaseColumn PrimaryColumn = (_dv as EbCalendarView).PrimaryKey;
                DVBaseColumn ForeignColumn = (_dv as EbCalendarView).ForeignKey;
                string PreviousGroupingText = string.Empty;

                DVBaseColumn DateColumn = (_dv as EbCalendarView).LinesColumns.FirstOrDefault(col => !col.IsCustomColumn && (col.Type == EbDbTypes.Date || col.Type == EbDbTypes.DateTime));

                for (int i = 0; i < MasterRows.Count; i++)
                {
                    object keydata = MasterRows[i][PrimaryColumn.OIndex];
                    List<EbDataRow> customRows = LinesRows.FindAll(row => Convert.ToInt32(row[ForeignColumn.OIndex]).Equals(keydata));//not complete(int)
                    _formattedTable.Rows.Add(_formattedTable.NewDataRow2());
                    _formattedTable.Rows[i][_formattedTable.Columns.Count - 1] = i + 1;//serial

                    DataTable2FormatedTable4Calendar(MasterRows[i], customRows, _dv, _user_culture, _user, ref _formattedTable, ref globals, i, _hourCount, DateColumn, ref summary);
                }
                return new PrePrcessorReturn { FormattedTable = _formattedTable, rows = MasterRows, Summary = summary };
            }
            catch (Exception e)
            {
                Log.Info("Before PreProcessing in PreProcessingCalendarView  Exception........." + e.StackTrace);
                Log.Info("Before PreProcessing in PreProcessingCalendarView  Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
            }
            return null;
        }

        public void CreateCustomcolumn4Calendar(EbDataSet _dataset, ref EbDataSet tempdataset, List<Param> Parameters, ref EbDataVisualization _dv, ref Dictionary<string, DynamicObj> _hourCount, ref Dictionary<int, List<object>> summary)
        {
            int i = 0;
            foreach (EbDataTable _table in _dataset.Tables)
            {
                if (i == 0)
                    tempdataset.Tables.Add(_table.GetEmptyTable());
                else
                {
                    foreach (EbDataColumn col in _table.Columns)
                    {
                        tempdataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = tempdataset.Tables[0].Columns.Count, ColumnName = col.ColumnName, Type = col.Type });
                    }
                }
                i++;
            }
            int index = tempdataset.Tables[0].Columns.Count;
            foreach (DVBaseColumn col in (_dv as EbCalendarView).DataColumns)
            {
                if (col.IsCustomColumn)
                    tempdataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index++, ColumnName = col.Name, Type = col.Type });
            }

            if ((_dv as EbCalendarView).CalendarType == AttendanceType.DayWise)
            {
                DayWiseDateColumns(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount, ref summary);
            }
            //else if ((_dv as EbCalendarView).CalendarType == AttendanceType.Hourly)
            //{
            //    HourlyWiseDateColumns(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount, ref summary);
            //}
            else if ((_dv as EbCalendarView).CalendarType == AttendanceType.Weekely)
            {
                WeekelyDateColumns(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount, ref summary);
            }
            else if ((_dv as EbCalendarView).CalendarType == AttendanceType.Monthly)
            {
                MonthlyDateColumns(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount, ref summary);
            }

            else if ((_dv as EbCalendarView).CalendarType == AttendanceType.Quarterly)
            {
                QuarterlyDateColumns(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount, ref summary);
            }
            else if ((_dv as EbCalendarView).CalendarType == AttendanceType.HalfYearly)
            {
                HalfYearlyDateColumns(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount, ref summary);
            }
            else if ((_dv as EbCalendarView).CalendarType == AttendanceType.Yearly)
            {
                YearlyDateColumns(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount, ref summary);
            }
        }

        public void DayWiseDateColumns(EbDataSet _dataset, ref EbDataSet tempdataset, List<Param> Parameters, ref EbDataVisualization _dv, ref Dictionary<string, DynamicObj> _hourCount, ref Dictionary<int, List<object>> summary)
        {
            int index = tempdataset.Tables[0].Columns.Count;
            int i = -1;
            string sql = $"SELECT eb_loc_id, holiday_date, holiday_name FROM eb_public_holidays WHERE eb_loc_id={CurLocId}";
            var _datatable = this.EbConnectionFactory.ObjectsDB.DoQuery(sql);
            DateTime paramdate = Parameters[0].ValueTo;
            for (var date = paramdate; paramdate.Month == date.Month; date = date.AddDays(1))
            {
                var startDate = new DateTime(date.Year, date.Month, date.Day, 0, 0, 0);
                var endDate = new DateTime(date.Year, date.Month, date.Day, 23, 59, 59);
                var key = GetKey(startDate, endDate);
                string _tooltip = date.ToString("dd-MM-yyyy");
                string _title = date.ToString("ddd")[0] + "</br>" + date.ToString("dd");
                tempdataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index, ColumnName = key, Type = EbDbTypes.String });
                i++;
                if (Modifydv)
                {
                    string cls = string.Empty;
                    var _hoidayRow = _datatable.Rows.FirstOrDefault(row => row["holiday_date"].Equals(startDate));
                    if (_hoidayRow != null)
                    {
                        cls = "holiday_class public-holiday";
                        _tooltip += "</br>" + _hoidayRow["holiday_name"].ToString();
                    }
                    else if (this._ebSolution.Locations.ContainsKey(CurLocId))
                    {
                        if (this._ebSolution.Locations[CurLocId].WeekHoliday1.ToLower() == date.ToString("dddd").ToLower())
                        {
                            _tooltip += "</br>" + this._ebSolution.Locations[CurLocId].WeekHoliday1;
                            cls = "holiday_class week-holiday";
                        }
                        else if (this._ebSolution.Locations[CurLocId].WeekHoliday2.ToLower() == date.ToString("dddd").ToLower())
                        {
                            _tooltip += "</br>" + this._ebSolution.Locations[CurLocId].WeekHoliday2;
                            cls = "holiday_class week-holiday";
                        }
                    }
                    if (DateTime.Now.Date.Equals(date))
                    {
                        cls += "current_date_class";
                    }
                    CalendarDynamicColumn col = new CalendarDynamicColumn
                    {
                        Data = index,
                        OIndex = i,
                        Name = key,
                        sTitle = _title,
                        Type = EbDbTypes.String,
                        RenderType = EbDbTypes.Int32,
                        IsCustomColumn = true,
                        bVisible = true,
                        ClassName = cls,
                        HeaderTooltipText = _tooltip,
                        StartDT = startDate,
                        EndDT = endDate,
                        Align = Align.Right
                    };
                    _dv.Columns.Add(col);
                    (_dv as EbCalendarView).DateColumns.Add(col);
                    summary.Add(index, new List<object> { 0, 0, 0, 0 });
                }
                index++;
                if (!_hourCount.ContainsKey(key))
                    _hourCount.Add(key, new DynamicObj());
            }
        }

        public void HourlyWiseDateColumns(EbDataSet _dataset, ref EbDataSet tempdataset, List<Param> Parameters, ref EbDataVisualization _dv, ref Dictionary<string, DynamicObj> _hourCount, ref Dictionary<int, List<object>> summary)
        {
            int index = tempdataset.Tables[0].Columns.Count;
            int i = -1;

            TimeSpan End = new TimeSpan((_dv as EbCalendarView).EndTime + 12, 0, 0);
            DateTime paramdate = Parameters[0].ValueTo;
            string _tooltip = paramdate.ToString("dd-MM-yyyy");

            for (TimeSpan start = new TimeSpan((_dv as EbCalendarView).StartTime, 0, 0); TimeSpan.Compare(start, End) <= 0; start = start.Add(TimeSpan.FromHours((_dv as EbCalendarView).Interval)))
            {
                var startDate = new DateTime(paramdate.Year, paramdate.Month, paramdate.Day, start.Hours, 0, 0);
                var endDate = new DateTime(paramdate.Year, paramdate.Month, paramdate.Day, start.Hours, 59, 59);
                var key = GetKey(startDate, endDate);
                string _title = startDate.ToString("hh:mm tt").ToString() + " - " + endDate.ToString("hh:mm tt").ToString();
                tempdataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index, ColumnName = key, Type = EbDbTypes.String });
                i++;
                if (Modifydv)
                {
                    CalendarDynamicColumn col = new CalendarDynamicColumn
                    {
                        Data = index,
                        OIndex = i,
                        Name = key,
                        sTitle = _title,
                        Type = EbDbTypes.String,
                        RenderType = EbDbTypes.Int32,
                        IsCustomColumn = true,
                        bVisible = true,
                        StartDT = startDate,
                        EndDT = endDate,
                        Align = Align.Right,
                        HeaderTooltipText = _tooltip
                    };
                    _dv.Columns.Add(col);
                    (_dv as EbCalendarView).DateColumns.Add(col);
                    summary.Add(index, new List<object> { 0, 0, 0, 0 });
                }
                index++;
                if (!_hourCount.ContainsKey(key))
                    _hourCount.Add(key, new DynamicObj());
            }
        }

        public void WeekelyDateColumns(EbDataSet _dataset, ref EbDataSet tempdataset, List<Param> Parameters, ref EbDataVisualization _dv, ref Dictionary<string, DynamicObj> _hourCount, ref Dictionary<int, List<object>> summary)
        {
            int index = tempdataset.Tables[0].Columns.Count;
            int i = -1;
            DateTime paramdate = Parameters[0].ValueTo;
            Calendar calendar = CultureInfo.CurrentCulture.Calendar;

            IEnumerable<int> daysInMonth = Enumerable.Range(1, calendar.GetDaysInMonth(paramdate.Year, paramdate.Month));

            List<Tuple<DateTime, DateTime>> weeks = daysInMonth.Select(day => new DateTime(paramdate.Year, paramdate.Month, day))
                .GroupBy(d => calendar.GetWeekOfYear(d, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Sunday))
                .Select(g => new Tuple<DateTime, DateTime>(g.First(), g.Last()))
                .ToList();

            foreach (Tuple<DateTime, DateTime> x in weeks)
            {
                i++;
                var date = new DateTime(x.Item1.Year, x.Item1.Month, x.Item1.Day, 0, 0, 0);
                var startDate = date;
                date = new DateTime(x.Item2.Year, x.Item2.Month, x.Item2.Day, 23, 59, 59);
                var endDate = DateTimeHelper.EndOfDay(x.Item2);
                var key = GetKey(startDate, endDate);
                var _title = "week " + (i + 1);
                string _tooltip = x.Item1.ToString("dd-MM-yyyy") + " to " + x.Item2.ToString("dd-MM-yyyy");
                tempdataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index, ColumnName = key, Type = EbDbTypes.String });
                if (Modifydv)
                {
                    CalendarDynamicColumn col = new CalendarDynamicColumn
                    {
                        Data = index,
                        OIndex = i,
                        Name = key,
                        sTitle = _title,
                        Type = EbDbTypes.String,
                        RenderType = EbDbTypes.Int32,
                        IsCustomColumn = true,
                        bVisible = true,
                        StartDT = startDate,
                        EndDT = endDate,
                        Align = Align.Right,
                        HeaderTooltipText = _tooltip
                    };
                    _dv.Columns.Add(col);
                    (_dv as EbCalendarView).DateColumns.Add(col);
                    summary.Add(index, new List<object> { 0, 0, 0, 0 });
                }
                index++;
                if (!_hourCount.ContainsKey(key))
                    _hourCount.Add(key, new DynamicObj());
            }
        }

        public void MonthlyDateColumns(EbDataSet _dataset, ref EbDataSet tempdataset, List<Param> Parameters, ref EbDataVisualization _dv, ref Dictionary<string, DynamicObj> _hourCount, ref Dictionary<int, List<object>> summary)
        {
            int index = tempdataset.Tables[0].Columns.Count;
            int i = -1;
            DateTime datefrom = Parameters[0].ValueTo;
            DateTime dateto = Parameters[1].ValueTo;
            for (int m = 1; m <= 12; m++)
            {
                for (int y = datefrom.Year; y <= dateto.Year; y++)
                {
                    DateTime date = new DateTime(y, m, 1, 0, 0, 0);
                    DateTime startDate = date;
                    date = startDate.AddMonths(1).AddDays(-1);
                    DateTime endDate = new DateTime(y, m, date.Day, 23, 59, 59);
                    string key = GetKey(startDate, endDate);
                    string title = date.ToString("MMM-yy");
                    string _tooltip = startDate.ToString("dd-MM-yyyy") + " to " + endDate.ToString("dd-MM-yyyy");
                    tempdataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index, ColumnName = key, Type = EbDbTypes.String });
                    i++;
                    if (Modifydv)
                    {
                        CalendarDynamicColumn col = new CalendarDynamicColumn
                        {
                            Data = index,
                            OIndex = i,
                            Name = key,
                            sTitle = title,
                            Type = EbDbTypes.String,
                            RenderType = EbDbTypes.Int32,
                            IsCustomColumn = true,
                            bVisible = true,
                            StartDT = startDate,
                            EndDT = endDate,
                            Align = Align.Right,
                            HeaderTooltipText = _tooltip,
                            //sWidth = "75px"
                        };
                        _dv.Columns.Add(col);
                        (_dv as EbCalendarView).DateColumns.Add(col);
                        summary.Add(index, new List<object> { 0, 0, 0, 0 });
                    }
                    index++;
                    if (!_hourCount.ContainsKey(key))
                        _hourCount.Add(key, new DynamicObj());
                }
            }
        }

        public void QuarterlyDateColumns(EbDataSet _dataset, ref EbDataSet tempdataset, List<Param> Parameters, ref EbDataVisualization _dv, ref Dictionary<string, DynamicObj> _hourCount, ref Dictionary<int, List<object>> summary)
        {
            int index = tempdataset.Tables[0].Columns.Count;
            int i = 0;
            DateTime datefrom = Parameters[0].ValueTo;
            DateTime dateto = Parameters[1].ValueTo;
            for (int m = 1, j = 1; m <= 12; m += 3, j++)
            {
                for (int y = datefrom.Year; y <= dateto.Year; y++)
                {
                    string month = DateTimeFormatInfo.CurrentInfo.GetMonthName(m);
                    DateTime date = new DateTime(y/*paramdate.Year*/, m, 1, 0, 0, 0);
                    DateTime startDate = date;
                    date = startDate.AddMonths(3).AddDays(-1);
                    DateTime endDate = new DateTime(y/*date.Year*/, date.Month, date.Day, 23, 59, 59);
                    string key = GetKey(startDate, endDate);
                    string endmonth = DateTimeFormatInfo.CurrentInfo.GetMonthName(date.Month);
                    string title = "Q" + j + " - '" + y.ToString().Substring(2, 2);
                    string _tooltip = month + "-" + endmonth + "</br>" + startDate.ToString("dd-MM-yyyy") + " to " + endDate.ToString("dd-MM-yyyy");
                    tempdataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index, ColumnName = key, Type = EbDbTypes.String });
                    i++;
                    if (Modifydv)
                    {
                        CalendarDynamicColumn col = new CalendarDynamicColumn
                        {
                            Data = index,
                            OIndex = i,
                            Name = key,
                            sTitle = title,
                            Type = EbDbTypes.String,
                            RenderType = EbDbTypes.Int32,
                            IsCustomColumn = true,
                            bVisible = true,
                            StartDT = startDate,
                            EndDT = endDate,
                            Align = Align.Right,
                            HeaderTooltipText = _tooltip
                        };
                        _dv.Columns.Add(col);
                        (_dv as EbCalendarView).DateColumns.Add(col);
                        summary.Add(index, new List<object> { 0, 0, 0, 0 });
                    }
                    index++;
                    if (!_hourCount.ContainsKey(key))
                        _hourCount.Add(key, new DynamicObj());
                }
            }
        }

        public void HalfYearlyDateColumns(EbDataSet _dataset, ref EbDataSet tempdataset, List<Param> Parameters, ref EbDataVisualization _dv, ref Dictionary<string, DynamicObj> _hourCount, ref Dictionary<int, List<object>> summary)
        {
            int index = tempdataset.Tables[0].Columns.Count;
            int i = 0;
            DateTime datefrom = Parameters[0].ValueTo;
            DateTime dateto = Parameters[1].ValueTo;
            for (int m = 1, j = 1; m <= 12; m += 6, j++)
            {
                for (int y = datefrom.Year; y <= dateto.Year; y++)
                {
                    string startmonth = DateTimeFormatInfo.CurrentInfo.GetMonthName(m);
                    DateTime date = new DateTime(y, m, 1, 0, 0, 0);
                    DateTime startDate = date;
                    date = startDate.AddMonths(6).AddDays(-1);
                    DateTime endDate = new DateTime(y, date.Month, date.Day, 23, 59, 59);
                    string endmonth = DateTimeFormatInfo.CurrentInfo.GetMonthName(date.Month);
                    string key = GetKey(startDate, endDate);
                    string title = "HF " + j + " - '" + y.ToString().Substring(2, 2);
                    string _tooltip = startmonth + "-" + endmonth + "</br>" + startDate.ToString("dd-MM-yyyy") + " to " + endDate.ToString("dd-MM-yyyy");
                    tempdataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index, ColumnName = key, Type = EbDbTypes.String });
                    i++;
                    if (Modifydv)
                    {
                        CalendarDynamicColumn col = new CalendarDynamicColumn
                        {
                            Data = index,
                            OIndex = i,
                            Name = key,
                            sTitle = title,
                            Type = EbDbTypes.String,
                            RenderType = EbDbTypes.Int32,
                            IsCustomColumn = true,
                            bVisible = true,
                            StartDT = startDate,
                            EndDT = endDate,
                            Align = Align.Right,
                            HeaderTooltipText = _tooltip
                        };
                        _dv.Columns.Add(col);
                        (_dv as EbCalendarView).DateColumns.Add(col);
                        summary.Add(index, new List<object> { 0, 0, 0, 0 });
                    }
                    index++;
                    if (!_hourCount.ContainsKey(key))
                        _hourCount.Add(key, new DynamicObj());
                }
            }
        }

        public void YearlyDateColumns(EbDataSet _dataset, ref EbDataSet tempdataset, List<Param> Parameters, ref EbDataVisualization _dv, ref Dictionary<string, DynamicObj> _hourCount, ref Dictionary<int, List<object>> summary)
        {
            int index = tempdataset.Tables[0].Columns.Count;
            int i = 0;
            DateTime datefrom = Parameters[0].ValueTo;
            DateTime dateto = Parameters[1].ValueTo;

            for (int y = datefrom.Year; y <= dateto.Year; y++)
            {
                string startmonth = DateTimeFormatInfo.CurrentInfo.GetMonthName(1);
                DateTime startDate = new DateTime(y, 1, 1, 0, 0, 0);
                DateTime endDate = new DateTime(y, 12, 31, 23, 59, 59);
                string endmonth = DateTimeFormatInfo.CurrentInfo.GetMonthName(12);
                string key = GetKey(startDate, endDate);
                string title = y.ToString();
                string _tooltip = startmonth + "-" + endmonth + "</br>" + startDate.ToString("dd-MM-yyyy") + " to " + endDate.ToString("dd-MM-yyyy");
                tempdataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index, ColumnName = key, Type = EbDbTypes.String });
                i++;
                if (Modifydv)
                {
                    CalendarDynamicColumn col = new CalendarDynamicColumn
                    {
                        Data = index,
                        OIndex = i,
                        Name = key,
                        sTitle = title,
                        Type = EbDbTypes.String,
                        RenderType = EbDbTypes.Int32,
                        IsCustomColumn = true,
                        bVisible = true,
                        StartDT = startDate,
                        EndDT = endDate,
                        Align = Align.Right,
                        HeaderTooltipText = _tooltip
                    };
                    _dv.Columns.Add(col);
                    (_dv as EbCalendarView).DateColumns.Add(col);
                    summary.Add(index, new List<object> { 0, 0, 0, 0 });
                }
                index++;
                if (!_hourCount.ContainsKey(key))
                    _hourCount.Add(key, new DynamicObj());
            }
        }

        public List<DVBaseColumn> CreateDependencyTable(EbDataVisualization _dv)
        {
            List<DVBaseColumn> noncustom = _dv.Columns.Where(item => !item.IsCustomColumn).ToList<DVBaseColumn>();
            List<DVBaseColumn> Columns = new List<DVBaseColumn>();
            RecursiveCallforNonCustom(ref Columns, noncustom);
            List<DVBaseColumn> custom = _dv.Columns.Where(item => item.IsCustomColumn).ToList<DVBaseColumn>();
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

        public void RecursiveCallforNonCustom(ref List<DVBaseColumn> Columns, List<DVBaseColumn> noncustom)
        {
            foreach (DVBaseColumn _column in noncustom)
                RecursiveNonCustomColumn(ref Columns, _column);
        }

        public void RecursiveNonCustomColumn(ref List<DVBaseColumn> Columns, DVBaseColumn _column)
        {
            foreach (DVBaseColumn infocol in _column.InfoWindow)
            {
                RecursiveNonCustomColumn(ref Columns, infocol);
            }
            if (!Columns.Exists(x => x.Name == _column.Name))
                Columns.Add(_column);
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

        public void RecursiveGetTreeChilds(Node<EbDataRow> Nodedr, EbDataVisualization _dv, CultureInfo _user_culture, User _user, ref EbDataTable _formattedTable, ref Globals globals, bool bObfuscute, bool _isexcel, ref Dictionary<int, List<object>> Summary, ref int i, int count, bool isTree)
        {
            foreach (Node<EbDataRow> dr in Nodedr.Children)
            {
                i++;
                DataTable2FormatedTable(dr.Item, _dv, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, i, count, dr.IsGroup, dr.Level, isTree);
                if (dr.Children.Count > 0)
                {
                    RecursiveGetTreeChilds(dr, _dv, _user_culture, _user, ref _formattedTable, ref globals, bObfuscute, _isexcel, ref Summary, ref i, count, isTree);
                }

            }

        }

        public void DataTable2FormatedTable(EbDataRow row, EbDataVisualization _dv, CultureInfo _user_culture, User _user, ref EbDataTable _formattedTable, ref Globals globals, bool bObfuscute, bool _isexcel, ref Dictionary<int, List<object>> Summary, int i, int count, bool isgroup = false, int level = 0, bool isTree = false)
        {
            bool isnotAdded = true;
            try
            {
                Row workRow = new Row();
                if (_isexcel)
                {
                    for (int k = 1; k <= ExcelColumns.Count; k++)
                    {
                        string cellReference = GetExcelColumnName(k) + (i + ExcelRowcount);
                        workRow.Append(CreateCell(cellReference, string.Empty));
                    }
                }
                IntermediateDic = new Dictionary<int, object>();
                _formattedTable.Rows.Add(_formattedTable.NewDataRow2());
                _formattedTable.Rows[i][_formattedTable.Columns.Count - 1] = i + 1;//serial
                CreateIntermediateDict(row, _dv, _user_culture, _user, ref _formattedTable, ref globals, _isexcel, i);
                if ((_dv as EbTableVisualization) != null)
                {
                    foreach (DVBaseColumn col in dependencyTable)
                    {
                        isnotAdded = true;
                        int ExcelColIndex = ExcelColumns.FindIndex(_col => _col.Name == col.Name) + 1;
                        try
                        {
                            bool AllowLinkifNoData = true;
                            var cults = col.GetColumnCultureInfo(_user_culture);
                            object _unformattedData = row[col.Data] == null ? "" : row[col.Data];//(_dv.AutoGen && col.Name == "eb_action") ? "<i class='fa fa-edit'></i>" :
                            object _formattedData = IntermediateDic[col.Data] == null ? "" : IntermediateDic[col.Data];
                            object ActualFormatteddata = IntermediateDic[col.Data] == null || col is DVActionColumn ? "" : Convert.ToString(IntermediateDic[col.Data]).Replace("<", "").Replace(">", "").Replace("'", "").Replace("\"", "");
                            object ExcelData = _formattedData;

                            if (col.RenderType == EbDbTypes.Decimal || col.RenderType == EbDbTypes.Int32 || col.RenderType == EbDbTypes.Int64)
                            {
                                if (((col as DVNumericColumn).RenderAs == NumericRenderType.ProgressBar) && (_isexcel == false))
                                    _formattedData = "<div class='progress'><div class='progress-bar' role='progressbar' aria-valuenow='" + _formattedData + "' aria-valuemin='0' aria-valuemax='100' style='width:" + _unformattedData.ToString() + "%'>" + _formattedData + "</div></div>";
                                else if (((col as DVNumericColumn).RenderAs == NumericRenderType.Rating) && (_isexcel == false))
                                    _formattedData = GetDataforRating((col as DVNumericColumn), _formattedData);

                                SummaryCalc(ref Summary, col, _unformattedData, cults);
                                ExcelData = _unformattedData;
                            }
                            else if (col.RenderType == EbDbTypes.String && (_isexcel == false))
                            {
                                if (col is DVStringColumn)
                                {
                                    if ((col as DVStringColumn).RenderAs == StringRenderType.Marker)
                                        _formattedData = "<a href = '#' class ='columnMarker" + this.TableId + "' data-latlong='" + _unformattedData + "'><i class='fa fa-map-marker fa-2x' style='color:red;'></i></a>";

                                    else if ((col as DVStringColumn).RenderAs == StringRenderType.Image)
                                    {
                                        var _height = (col as DVStringColumn).ImageHeight == 0 ? "auto" : (col as DVStringColumn).ImageHeight + "px";
                                        var _width = (col as DVStringColumn).ImageWidth == 0 ? "auto" : (col as DVStringColumn).ImageWidth + "px";
                                        var _quality = (col as DVStringColumn).ImageQuality.ToString().ToLower();
                                        if (_unformattedData.ToString().Trim() != string.Empty)
                                        {
                                            _formattedData = $"<img class='img-thumbnail columnimage' src='/images/{_quality}/{_unformattedData}.jpg' style='height:{_height};width:{_width};'/>";
                                            _ImageList.Add(new FileMetaInfo
                                            {
                                                FileName = string.Empty,
                                                FileRefId = Convert.ToInt32(_unformattedData),
                                                FileCategory = Common.Enums.EbFileCategory.Images
                                            });
                                        }
                                        else
                                            _formattedData = $"<img class='img-thumbnail' src='/images/image.png' style='height:{_height};width:{_width};'/>";

                                    }

                                    else if ((col as DVStringColumn).RenderAs == StringRenderType.Tag)
                                        _formattedData = GetTaggedData((col as DVStringColumn), _formattedData);


                                    if ((col as DVStringColumn).AllowMultilineText)
                                    {
                                        if ((col as DVStringColumn).NoOfCharactersPerLine > 0 && (col as DVStringColumn).NoOfLines > 0)
                                        {
                                            //var _formattedData = HtmlUtilities.ConvertToPlainText(_formattedData);
                                            _formattedData = Regex.Replace(_formattedData.ToString(), "<.*?>", string.Empty);
                                            if (_formattedData.ToString().Length > (col as DVStringColumn).NoOfCharactersPerLine)
                                                _formattedData = GetMultilineText(_formattedData.ToString(), (col as DVStringColumn).NoOfCharactersPerLine, (col as DVStringColumn).NoOfLines);
                                        }
                                    }
                                }
                            }
                            else if (col.RenderType == EbDbTypes.String && _isexcel && col.bVisible)
                            {
                                if (col is DVStringColumn && (col as DVStringColumn).RenderAs == StringRenderType.Image)
                                {
                                    Log.Info("Rendar As Image-------");
                                    isnotAdded = false;
                                    var _height = (col as DVStringColumn).ImageHeight == 0 ? 40 : (col as DVStringColumn).ImageHeight;
                                    var _width = (col as DVStringColumn).ImageWidth == 0 ? 40 : (col as DVStringColumn).ImageWidth;
                                    var _quality = (col as DVStringColumn).ImageQuality.ToString().ToLower();
                                    //var src = "/images/{_quality}/{_unformattedData}.jpg"; 
                                    int rowIndex = i + ExcelRowcount;
                                    int imgid = Convert.ToInt32(_unformattedData);
                                    workRow.Height = _height;
                                    workRow.CustomHeight = true;
                                    if (imgid > 0)
                                    {
                                        Log.Info("dprefid----" + imgid);
                                        //byte[] bytea = GetImage(imgid);
                                        Stream imageStream = GetImageStream(imgid);
                                        if (imageStream != null)
                                            InsertImage(worksheetPart1, rowIndex - 1, ExcelColIndex - 1, imageStream);
                                        //if (bytea.Length > 0)
                                        //{
                                        //    MemoryStream ms = new MemoryStream(bytea);
                                        //    Log.Info("MemoryStream ok-----" + imgid);
                                        //    // Image img = Image.FromStream(ms);
                                        //    Log.Info("Drawings.Image ok-----" + imgid);
                                        //    //string sFilePath = string.Format("../StaticFiles/{0}/{1}", this._ebSolution.SolutionID, imgid);
                                        //    //using (FileStream file = new FileStream(sFilePath, FileMode.Create, System.IO.FileAccess.Write))
                                        //    //{
                                        //    //    file.Write(bytea, 0, bytea.Length);
                                        //    //}
                                        //    //FileInfo fileInfo = new FileInfo(sFilePath);
                                        //    Bitmap img = new Bitmap(ms);
                                        //    if (img.HorizontalResolution == 0 || img.VerticalResolution == 0)
                                        //        img.SetResolution(96, 96);

                                        //    //ExcelPicture pic = worksheet.Drawings.AddPicture(_unformattedData + ".jpg", img);
                                        //    Log.Info("ExcelPicture ok-----" + imgid);
                                        //    //pic.SetPosition(rowIndex - 1, 0, ExcelColIndex - 1, 0);
                                        //    //pic.SetSize(_height, _width);
                                        //    //pic.From.Column = colIndex;
                                        //    //pic.From.Row = rowIndex;
                                        //    //pic.SetSize(100, 100);
                                        //    // 2x2 px space for better alignment
                                        //    Log.Info("Image added in excel-----");
                                        //}


                                    }
                                    //worksheet.Column(ExcelColIndex).Width = _width;
                                    //worksheet.Row(rowIndex).Height = _height;
                                }
                            }

                            if (!_isexcel)
                            {
                                string info = string.Empty;
                                if (col.AllowedCharacterLength > 0 || col.InfoWindow.Count > 0)
                                {
                                    info = "<table>";
                                    if (col.AllowedCharacterLength > 0)
                                        info += "<tr><td><span class='headspan'>" + col.sTitle + " &nbsp; : &nbsp;</span></br><span class='bodyspan'>" + _formattedData + "</span></td></tr>";
                                    if (col.InfoWindow.Count > 0)
                                    {
                                        foreach (DVBaseColumn _column in col.InfoWindow)
                                        {
                                            if (_column.Name != col.Name)
                                            {
                                                info += "<tr><td><span class='headspan'>" + _column.sTitle + " &nbsp; : &nbsp;</span></br><span class='bodyspan'>" + IntermediateDic[_column.Data] + "</span></td></tr>";
                                            }
                                        }
                                    }
                                    info += "</table>";

                                    _formattedData = _formattedData.ToString().Truncate(col.AllowedCharacterLength);
                                    if (!string.IsNullOrEmpty(col.LinkRefId) && _formattedData.ToString() == string.Empty)
                                        _formattedData = "...";
                                    _formattedData = "<span class='columntooltip' data-toggle='popover' data-contents='" + info.ToBase64() + "'>" + _formattedData + "</span>";
                                }

                                if (_formattedData.ToString() == string.Empty)
                                    AllowLinkifNoData = false;
                                if (col.ShowLinkifNoData)
                                    AllowLinkifNoData = true;

                                if (!string.IsNullOrEmpty(col.LinkRefId) && (_isexcel == false))
                                {
                                    if (AllowLinkifNoData)
                                    {
                                        string _link = col.LinkRefId;
                                        if (_formattedData.ToString() == string.Empty)
                                            _formattedData = "...";
                                        if (col.LinkType == LinkTypeEnum.Popout)
                                            _formattedData = "<a href='#' oncontextmenu='return false' class ='tablelink" + this.TableId + "' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-column='" + col.Name + "'>" + _formattedData + "</a>";
                                        else if (col.LinkType == LinkTypeEnum.Inline)
                                            _formattedData = _formattedData + "&nbsp; <a  href= '#' oncontextmenu= 'return false' class ='tablelink" + this.TableId + "' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-column='" + col.Name + "' data-inline='true' data-data='" + ActualFormatteddata + "'><i class='fa fa-caret-down'></i></a>";
                                        else if (col.LinkType == LinkTypeEnum.Both)
                                            _formattedData = "<a href='#' oncontextmenu='return false' class ='tablelink" + this.TableId + "' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-column='" + col.Name + "' >" + _formattedData + "</a>" + "&nbsp; <a  href ='#' oncontextmenu='return false' class='tablelink" + this.TableId + "' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-column='" + col.Name + "' data-inline='true' data-data='" + ActualFormatteddata + "'> <i class='fa fa-caret-down'></i></a>";
                                        else if (col.LinkType == LinkTypeEnum.Popup)
                                            _formattedData = "<a  href= '#' oncontextmenu= 'return false' class ='tablelink" + this.TableId + "' data-colindex='" + col.Data + "' data-link='" + col.LinkRefId + "' data-column='" + col.Name + "' data-popup='true' data-data='" + ActualFormatteddata + "'>" + _formattedData + "</a>";
                                    }
                                }

                                if (col is DVStringColumn && col.RenderType == EbDbTypes.String && (col as DVStringColumn).RenderAs == StringRenderType.LinkFromColumn && (_isexcel == false))
                                {
                                    if (_formattedData.ToString() == string.Empty)
                                        _formattedData = "...";
                                    _formattedData = "<a href='#' class ='tablelinkfromcolumn" + this.TableId + "' data-link='" + row[col.RefidColumn.Data] + "' data-colindex='" + col.Data + "' data-column='" + col.Name + "' data-linkfromcolumn='true'>" + _formattedData + "</a>";
                                }

                                if (col is DVStringColumn && col.RenderType == EbDbTypes.String && (col as DVStringColumn).RenderAs == StringRenderType.Link && col.LinkType == LinkTypeEnum.Tab && (_isexcel == false))/////////////////
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

                                this.conditinallyformatColumn(col, ref _formattedData, _unformattedData, row, ref globals);
                                if (col is DVPhoneColumn)
                                    this.ModifyPhonecolumn(col, ref _formattedData);
                            }

                            _formattedTable.Rows[i][col.Data] = _formattedData;
                            if (i + 1 == count)
                            {
                                SummaryCalcAverage(ref Summary, col, cults, count);
                            }
                            if (_isexcel && isnotAdded && ExcelColIndex > 0)
                            {
                                string cellReference = GetExcelColumnName(ExcelColIndex) + (i + ExcelRowcount);
                                Cell cell = workRow.Elements<Cell>().Where(c => c.CellReference.Value == cellReference).First();
                                cell.CellValue = new CellValue(ExcelData.ToString());
                                cell.DataType = ResolveCellDataTypeOnValue(ExcelData.ToString());
                                //workRow.Append(CreateCell(cellReference, ExcelData.ToString()));
                            }

                        }
                        catch (Exception e)
                        {
                            Log.Info("PreProcessing  data from IntermediateDictionay datatable Exception........." + e.StackTrace + "Column Name  ......" + col.Name);
                            Log.Info("PreProcessing data from IntermediateDictionay datatable Exception........." + e.Message + "Column Name  ......" + col.Name);
                            this._Responsestatus.Message = e.Message;
                        }
                    }
                    if (isTree)
                    {
                        var treecol = _dv.Columns.FirstOrDefault(e => e.IsTree == true);
                        _formattedTable.Rows[i][treecol.Data] = GetTreeHtml(_formattedTable.Rows[i][treecol.Data], isgroup, level);
                    }
                    if (_isexcel)
                    {
                        partSheetData.Append(workRow);
                        if (i + 1 == count)
                        {
                            Row workRow1 = new Row();
                            Row workRow2 = new Row();
                            foreach (var _key in Summary.Keys)
                            {
                                int ExcelColIndex = ExcelColumns.FindIndex(_col => _col.Data == _key) + 1;
                                var cellReference = GetExcelColumnName(ExcelColIndex) + (i + ExcelRowcount + 1);
                                var _formula = $"SUM({GetExcelColumnName(ExcelColIndex)}{ExcelRowcount}:{GetExcelColumnName(ExcelColIndex)}{i + ExcelRowcount})";
                                workRow1.Append(CreateCellWithFormula(cellReference, _formula));
                                cellReference = GetExcelColumnName(ExcelColIndex) + (i + ExcelRowcount + 2);
                                _formula = $"AVERAGE({GetExcelColumnName(ExcelColIndex)}{ExcelRowcount}:{GetExcelColumnName(ExcelColIndex)}{i + ExcelRowcount})";
                                workRow2.Append(CreateCellWithFormula(cellReference, _formula));
                            }
                            partSheetData.Append(workRow1);
                            partSheetData.Append(workRow2);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info("PreProcessing in datatable Exception........." + e.StackTrace);
                Log.Info("PreProcessing in datatable Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
            }

        }

        /// <summary>
        /// Inserts the image at the specified location
        /// </summary>
        /// <param name="sheet1">The WorksheetPart where image to be inserted</param>
        /// <param name="startRowIndex">The starting Row Index</param>
        /// <param name="startColumnIndex">The starting column index</param>
        /// <param name="endRowIndex">The ending row index</param>
        /// <param name="endColumnIndex">The ending column index</param>
        /// <param name="imageStream">Stream which contains the image data</param>
        private void InsertImage(WorksheetPart sheet1, int startRowIndex, int startColumnIndex, Stream imageStream)
        {
            ImagePartType ipt = ImagePartType.Jpeg;
            DrawingsPart drawingsPart1;
            ImagePart imagePart1;
            Xdr.WorksheetDrawing worksheetDrawing1;
            if (sheet1.DrawingsPart == null)
            {
                drawingsPart1 = sheet1.AddNewPart<DrawingsPart>();
                imagePart1 = drawingsPart1.AddImagePart(ipt, sheet1.GetIdOfPart(drawingsPart1));
                worksheetDrawing1 = new Xdr.WorksheetDrawing();
            }
            else
            {
                drawingsPart1 = sheet1.DrawingsPart;
                imagePart1 = drawingsPart1.AddImagePart(ipt);
                drawingsPart1.CreateRelationshipToPart(imagePart1);
                worksheetDrawing1 = drawingsPart1.WorksheetDrawing;
            }

            int imageNumber = drawingsPart1.ImageParts.Count<ImagePart>();
            if (imageNumber == 1)
            {
                Drawing drawing = new Drawing();
                drawing.Id = drawingsPart1.GetIdOfPart(imagePart1);
                sheet1.Worksheet.Append(drawing);
            }
            imagePart1.FeedData(imageStream);

            Xdr.TwoCellAnchor twoCellAnchor1 = new Xdr.TwoCellAnchor() { EditAs = Xdr.EditAsValues.OneCell };

            Xdr.FromMarker fromMarker1 = new Xdr.FromMarker();
            Xdr.ColumnId columnId1 = new Xdr.ColumnId();
            columnId1.Text = startColumnIndex.ToString();
            Xdr.ColumnOffset columnOffset1 = new Xdr.ColumnOffset();
            columnOffset1.Text = "0";
            Xdr.RowId rowId1 = new Xdr.RowId();
            rowId1.Text = startRowIndex.ToString();
            Xdr.RowOffset rowOffset1 = new Xdr.RowOffset();
            rowOffset1.Text = "0";

            fromMarker1.Append(columnId1);
            fromMarker1.Append(columnOffset1);
            fromMarker1.Append(rowId1);
            fromMarker1.Append(rowOffset1);

            Xdr.ToMarker toMarker1 = new Xdr.ToMarker();
            Xdr.ColumnId columnId2 = new Xdr.ColumnId();
            columnId2.Text = startColumnIndex.ToString();
            Xdr.ColumnOffset columnOffset2 = new Xdr.ColumnOffset();
            columnOffset2.Text = "700000";
            Xdr.RowId rowId2 = new Xdr.RowId();
            rowId2.Text = startRowIndex.ToString();
            Xdr.RowOffset rowOffset2 = new Xdr.RowOffset();
            rowOffset2.Text = "501925";

            toMarker1.Append(columnId2);
            toMarker1.Append(columnOffset2);
            toMarker1.Append(rowId2);
            toMarker1.Append(rowOffset2);

            Xdr.Picture picture1 = new Xdr.Picture();

            Xdr.NonVisualPictureProperties nonVisualPictureProperties1 = new Xdr.NonVisualPictureProperties();
            Xdr.NonVisualDrawingProperties nonVisualDrawingProperties1 = new Xdr.NonVisualDrawingProperties() { Id = new UInt32Value((uint)(1024 + imageNumber)), Name = "Picture " + imageNumber.ToString() };

            Xdr.NonVisualPictureDrawingProperties nonVisualPictureDrawingProperties1 = new Xdr.NonVisualPictureDrawingProperties();
            A.PictureLocks pictureLocks1 = new A.PictureLocks() { NoChangeAspect = true };

            nonVisualPictureDrawingProperties1.Append(pictureLocks1);

            nonVisualPictureProperties1.Append(nonVisualDrawingProperties1);
            nonVisualPictureProperties1.Append(nonVisualPictureDrawingProperties1);

            Xdr.BlipFill blipFill1 = new Xdr.BlipFill();

            A.Blip blip1 = new A.Blip() { Embed = drawingsPart1.GetIdOfPart(imagePart1), CompressionState = A.BlipCompressionValues.Print };
            blip1.AddNamespaceDeclaration("r", "http://schemas.openxmlformats.org/officeDocument/2006/relationships");

            A.BlipExtensionList blipExtensionList1 = new A.BlipExtensionList();

            A.BlipExtension blipExtension1 = new A.BlipExtension() { Uri = "{28A0092B-C50C-407E-A947-70E740481C1C}" };

            A14.UseLocalDpi useLocalDpi1 = new A14.UseLocalDpi() { Val = false };
            useLocalDpi1.AddNamespaceDeclaration("a14", "http://schemas.microsoft.com/office/drawing/2010/main");

            blipExtension1.Append(useLocalDpi1);

            blipExtensionList1.Append(blipExtension1);

            blip1.Append(blipExtensionList1);

            A.Stretch stretch1 = new A.Stretch();
            A.FillRectangle fillRectangle1 = new A.FillRectangle();

            stretch1.Append(fillRectangle1);

            blipFill1.Append(blip1);
            blipFill1.Append(stretch1);

            Xdr.ShapeProperties shapeProperties1 = new Xdr.ShapeProperties();

            A.Transform2D transform2D1 = new A.Transform2D();
            A.Offset offset1 = new A.Offset() { X = 1257300L, Y = 762000L };
            A.Extents extents1 = new A.Extents() { Cx = 2943225L, Cy = 2257425L };

            transform2D1.Append(offset1);
            transform2D1.Append(extents1);

            A.PresetGeometry presetGeometry1 = new A.PresetGeometry() { Preset = A.ShapeTypeValues.Rectangle };
            A.AdjustValueList adjustValueList1 = new A.AdjustValueList();

            presetGeometry1.Append(adjustValueList1);

            shapeProperties1.Append(transform2D1);
            shapeProperties1.Append(presetGeometry1);

            picture1.Append(nonVisualPictureProperties1);
            picture1.Append(blipFill1);
            picture1.Append(shapeProperties1);
            Xdr.ClientData clientData1 = new Xdr.ClientData();

            twoCellAnchor1.Append(fromMarker1);
            twoCellAnchor1.Append(toMarker1);
            twoCellAnchor1.Append(picture1);
            twoCellAnchor1.Append(clientData1);

            worksheetDrawing1.Append(twoCellAnchor1);

            if (imageNumber == 1)
                drawingsPart1.WorksheetDrawing = worksheetDrawing1;
        }

        //private void InsertImage(Stream imageStream, Row row, DVBaseColumn col, int rowIndex, int colIndex)
        //{
        //    //Inserting a drawing element in worksheet
        //    //Make sure that the relationship id is same for drawing element in worksheet and its relationship part
        //    int drawingPartId = GetNextRelationShipID();
        //    Drawing drawing1 = new Drawing() { Id = "rId" + rowIndex.ToString() };

        //    //Check whether the WorksheetPart contains VmlDrawingParts (LegacyDrawing element)
        //    if (worksheetPart1.VmlDrawingParts == null)
        //    {
        //        //if there is no VMLDrawing part (LegacyDrawing element) exists, just append the drawing part to the sheet
        //        worksheetPart1.Worksheet.Append(drawing1);
        //    }
        //    else
        //    {
        //        //if VmlDrawingPart (LegacyDrawing element) exists, then find the index of legacy drawing in the sheet and inserts the new drawing element before VMLDrawing part
        //        int legacyDrawingIndex = GetIndexofLegacyDrawing();
        //        if (legacyDrawingIndex != -1)
        //            worksheetPart1.Worksheet.InsertAt<OpenXmlElement>(drawing1, legacyDrawingIndex);
        //        else
        //            worksheetPart1.Worksheet.Append(drawing1);
        //    }
        //    //Adding the drawings.xml part
        //    DrawingsPart drawingsPart1 = null;
        //    if (worksheetPart1.DrawingsPart == null)
        //        drawingsPart1 = worksheetPart1.AddNewPart<DrawingsPart>("rId" + rowIndex.ToString());
        //    else
        //        drawingsPart1 = worksheetPart1.DrawingsPart;
        //    GenerateDrawingsPart1Content(drawingsPart1, rowIndex, colIndex, rowIndex+1, colIndex+1);
        //    //Adding the image
        //    ImagePart imagePart1 = drawingsPart1.AddNewPart<ImagePart>("image/jpeg", "rId" + rowIndex.ToString());
        //    imagePart1.FeedData(imageStream);
        //}

        ///// <summary>
        ///// Get the index of legacy drawing element in the specified WorksheetPart
        ///// </summary>
        ///// <param name="sheet1">The worksheetPart</param>
        ///// <returns>Index of legacy drawing</returns>
        //private int GetIndexofLegacyDrawing()
        //{
        //    for (int i = 0; i < worksheetPart1.Worksheet.ChildElements.Count; i++)
        //    {
        //        OpenXmlElement element = worksheetPart1.Worksheet.ChildElements[i];
        //        if (element is LegacyDrawing)
        //            return i;
        //    }
        //    return -1;
        //}

        //private static void GenerateDrawingsPart1Content(DrawingsPart drawingsPart1, int startRowIndex, int startColumnIndex, int endRowIndex, int endColumnIndex)
        //{
        //    Xdr.WorksheetDrawing worksheetDrawing1 = new Xdr.WorksheetDrawing();
        //    worksheetDrawing1.AddNamespaceDeclaration("xdr", "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing");
        //    worksheetDrawing1.AddNamespaceDeclaration("a", "http://schemas.openxmlformats.org/drawingml/2006/main");

        //    Xdr.TwoCellAnchor twoCellAnchor1 = new Xdr.TwoCellAnchor() { EditAs = Xdr.EditAsValues.OneCell };

        //    Xdr.FromMarker fromMarker1 = new Xdr.FromMarker();
        //    Xdr.ColumnId columnId1 = new Xdr.ColumnId();
        //    columnId1.Text = startColumnIndex.ToString();
        //    Xdr.ColumnOffset columnOffset1 = new Xdr.ColumnOffset();
        //    columnOffset1.Text = "38100";
        //    Xdr.RowId rowId1 = new Xdr.RowId();
        //    rowId1.Text = startRowIndex.ToString();
        //    Xdr.RowOffset rowOffset1 = new Xdr.RowOffset();
        //    rowOffset1.Text = "0";

        //    fromMarker1.Append(columnId1);
        //    fromMarker1.Append(columnOffset1);
        //    fromMarker1.Append(rowId1);
        //    fromMarker1.Append(rowOffset1);

        //    Xdr.ToMarker toMarker1 = new Xdr.ToMarker();
        //    Xdr.ColumnId columnId2 = new Xdr.ColumnId();
        //    columnId2.Text = endColumnIndex.ToString();
        //    Xdr.ColumnOffset columnOffset2 = new Xdr.ColumnOffset();
        //    columnOffset2.Text = "542925";
        //    Xdr.RowId rowId2 = new Xdr.RowId();
        //    rowId2.Text = endRowIndex.ToString();
        //    Xdr.RowOffset rowOffset2 = new Xdr.RowOffset();
        //    rowOffset2.Text = "161925";

        //    toMarker1.Append(columnId2);
        //    toMarker1.Append(columnOffset2);
        //    toMarker1.Append(rowId2);
        //    toMarker1.Append(rowOffset2);

        //    Xdr.Picture picture1 = new Xdr.Picture();

        //    Xdr.NonVisualPictureProperties nonVisualPictureProperties1 = new Xdr.NonVisualPictureProperties();
        //    Xdr.NonVisualDrawingProperties nonVisualDrawingProperties1 = new Xdr.NonVisualDrawingProperties() { Id = (UInt32Value)2U, Name = "Picture 1" };

        //    Xdr.NonVisualPictureDrawingProperties nonVisualPictureDrawingProperties1 = new Xdr.NonVisualPictureDrawingProperties();
        //    A.PictureLocks pictureLocks1 = new A.PictureLocks() { NoChangeAspect = true };

        //    nonVisualPictureDrawingProperties1.Append(pictureLocks1);

        //    nonVisualPictureProperties1.Append(nonVisualDrawingProperties1);
        //    nonVisualPictureProperties1.Append(nonVisualPictureDrawingProperties1);

        //    Xdr.BlipFill blipFill1 = new Xdr.BlipFill();

        //    A.Blip blip1 = new A.Blip() { Embed = "rId"+ startRowIndex, CompressionState = A.BlipCompressionValues.Print };
        //    blip1.AddNamespaceDeclaration("r", "http://schemas.openxmlformats.org/officeDocument/2006/relationships");

        //    A.BlipExtensionList blipExtensionList1 = new A.BlipExtensionList();

        //    A.BlipExtension blipExtension1 = new A.BlipExtension() { Uri = "{28A0092B-C50C-407E-A947-70E740481C1C}" };

        //    A14.UseLocalDpi useLocalDpi1 = new A14.UseLocalDpi() { Val = false };
        //    useLocalDpi1.AddNamespaceDeclaration("a14", "http://schemas.microsoft.com/office/drawing/2010/main");

        //    blipExtension1.Append(useLocalDpi1);

        //    blipExtensionList1.Append(blipExtension1);

        //    blip1.Append(blipExtensionList1);

        //    A.Stretch stretch1 = new A.Stretch();
        //    A.FillRectangle fillRectangle1 = new A.FillRectangle();

        //    stretch1.Append(fillRectangle1);

        //    blipFill1.Append(blip1);
        //    blipFill1.Append(stretch1);

        //    Xdr.ShapeProperties shapeProperties1 = new Xdr.ShapeProperties();

        //    A.Transform2D transform2D1 = new A.Transform2D();
        //    A.Offset offset1 = new A.Offset() { X = 1257300L, Y = 762000L };
        //    A.Extents extents1 = new A.Extents() { Cx = 2943225L, Cy = 2257425L };

        //    transform2D1.Append(offset1);
        //    transform2D1.Append(extents1);

        //    A.PresetGeometry presetGeometry1 = new A.PresetGeometry() { Preset = A.ShapeTypeValues.Rectangle };
        //    A.AdjustValueList adjustValueList1 = new A.AdjustValueList();

        //    presetGeometry1.Append(adjustValueList1);

        //    shapeProperties1.Append(transform2D1);
        //    shapeProperties1.Append(presetGeometry1);

        //    picture1.Append(nonVisualPictureProperties1);
        //    picture1.Append(blipFill1);
        //    picture1.Append(shapeProperties1);
        //    Xdr.ClientData clientData1 = new Xdr.ClientData();

        //    twoCellAnchor1.Append(fromMarker1);
        //    twoCellAnchor1.Append(toMarker1);
        //    twoCellAnchor1.Append(picture1);
        //    twoCellAnchor1.Append(clientData1);

        //    worksheetDrawing1.Append(twoCellAnchor1);

        //    drawingsPart1.WorksheetDrawing = worksheetDrawing1;
        //}

        //private void InsertImage(Stream imageStream, Row row,DVBaseColumn col, int rowIndex, int colIndex)
        //{
        //    //Calculate & sets the column width & Row height based on the image size
        //    var data = GetImageFromFile(imageId);
        //    Size imageSize = (data as Image).Size;
        //    row.Height = imageSize.Height;
        //    row.CustomHeight = true;
        //    //Column column = (columns.ChildElements[colIndex] as Column);
        //    //DoubleValue currentImageWidth = GetExcelCellWidth(imageSize.Width);
        //    //if (column.Width != null)
        //    //    column.Width = column.Width >
        //    //    currentImageWidth ? column.Width : currentImageWidth;
        //    //else
        //    //    column.Width = currentImageWidth;
        //    //column.Min = UInt32Value.FromUInt32((uint)colIndex + 1);
        //    //column.Max = UInt32Value.FromUInt32((uint)colIndex + 2);

        //    //if the data is Image, we need to serailize 
        //    //its characteristics information in the drawing part
        //    //and then raw image need to be added as Image part within file or package
        //    int drawingrID = GetNextRelationShipID();
        //    DrawingsPart drawingsPart = null;
        //    Xdr.WorksheetDrawing worksheetDrawing = null;

        //    if (worksheetPart1.DrawingsPart == null)
        //    {
        //        drawingsPart = worksheetPart1.AddNewPart<DrawingsPart>(drawingrID.ToString());
        //        worksheetDrawing = new Xdr.WorksheetDrawing();
        //        drawingsPart.WorksheetDrawing = worksheetDrawing;
        //    }
        //    else if (worksheetPart1.DrawingsPart != null && worksheetPart1.DrawingsPart.WorksheetDrawing != null)
        //    {
        //        drawingsPart = worksheetPart1.DrawingsPart;
        //        worksheetDrawing = worksheetPart1.DrawingsPart.WorksheetDrawing;
        //    }
        //    int imagerId = GetNextRelationShipID();
        //    Xdr.TwoCellAnchor cellAnchor = AddTwoCellAnchor(rowIndex, colIndex, rowIndex + 1, colIndex + 1, imagerId.ToString());
        //    worksheetDrawing.Append(cellAnchor);
        //    ImagePart imagePart =
        //    drawingsPart.AddNewPart<ImagePart>("image/png", imagerId.ToString());
        //    GenerateImagePartContent(imagePart, data as Image);
        //}

        /// <summary>
        /// Represents the bounds of the image, 
        /// reference to image part and other characteristics using TwoCellAnchor class
        /// </summary>
        /// <param name="startRow">Starting row of the image</param>
        /// <param name="startColumn">starting column of the image</param>
        /// <param name="endRow">Ending row of the image</param>
        /// <param name="endColumn">ending column of the image</param>
        /// <param name="imagerId">Image's relationship id</param>
        /// <returns></returns>
        //private Xdr.TwoCellAnchor AddTwoCellAnchor(int startRow, int startColumn, int endRow, int endColumn, string imagerId)
        //{
        //    Xdr.TwoCellAnchor twoCellAnchor1 = new Xdr.TwoCellAnchor() { EditAs = Xdr.EditAsValues.OneCell };

        //    Xdr.FromMarker fromMarker1 = new Xdr.FromMarker();
        //    Xdr.ColumnId columnId1 = new Xdr.ColumnId();
        //    columnId1.Text = startColumn.ToString();
        //    Xdr.ColumnOffset columnOffset1 = new Xdr.ColumnOffset();
        //    columnOffset1.Text = "0";
        //    Xdr.RowId rowId1 = new Xdr.RowId();
        //    rowId1.Text = startRow.ToString();
        //    Xdr.RowOffset rowOffset1 = new Xdr.RowOffset();
        //    rowOffset1.Text = "0";

        //    fromMarker1.Append(columnId1);
        //    fromMarker1.Append(columnOffset1);
        //    fromMarker1.Append(rowId1);
        //    fromMarker1.Append(rowOffset1);

        //    Xdr.ToMarker toMarker1 = new Xdr.ToMarker();
        //    Xdr.ColumnId columnId2 = new Xdr.ColumnId();
        //    columnId2.Text = endColumn.ToString();
        //    Xdr.ColumnOffset columnOffset2 = new Xdr.ColumnOffset();
        //    columnOffset2.Text = "0";// "152381";
        //    Xdr.RowId rowId2 = new Xdr.RowId();
        //    rowId2.Text = endRow.ToString();
        //    Xdr.RowOffset rowOffset2 = new Xdr.RowOffset();
        //    rowOffset2.Text = "0";//"152381";

        //    toMarker1.Append(columnId2);
        //    toMarker1.Append(columnOffset2);
        //    toMarker1.Append(rowId2);
        //    toMarker1.Append(rowOffset2);

        //    Xdr.Picture picture1 = new Xdr.Picture();

        //    Xdr.NonVisualPictureProperties nonVisualPictureProperties1 = new Xdr.NonVisualPictureProperties();
        //    Xdr.NonVisualDrawingProperties nonVisualDrawingProperties1 =
        //        new Xdr.NonVisualDrawingProperties() { Id = (UInt32Value)2U, Name = "Picture 1" };

        //    Xdr.NonVisualPictureDrawingProperties nonVisualPictureDrawingProperties1 =
        //        new Xdr.NonVisualPictureDrawingProperties();
        //    A.PictureLocks pictureLocks1 = new A.PictureLocks() { NoChangeAspect = true };

        //    nonVisualPictureDrawingProperties1.Append(pictureLocks1);

        //    nonVisualPictureProperties1.Append(nonVisualDrawingProperties1);
        //    nonVisualPictureProperties1.Append(nonVisualPictureDrawingProperties1);

        //    Xdr.BlipFill blipFill1 = new Xdr.BlipFill();

        //    A.Blip blip1 = new A.Blip() { Embed = imagerId };
        //    blip1.AddNamespaceDeclaration("r",
        //        "http://schemas.openxmlformats.org/officeDocument/2006/relationships");

        //    A.BlipExtensionList blipExtensionList1 = new A.BlipExtensionList();

        //    A.BlipExtension blipExtension1 = new A.BlipExtension()
        //    { Uri = "{28A0092B-C50C-407E-A947-70E740481C1C}" };

        //    A14.UseLocalDpi useLocalDpi1 = new A14.UseLocalDpi() { Val = false };
        //    useLocalDpi1.AddNamespaceDeclaration("a14",
        //        "http://schemas.microsoft.com/office/drawing/2010/main");

        //    blipExtension1.Append(useLocalDpi1);

        //    blipExtensionList1.Append(blipExtension1);

        //    blip1.Append(blipExtensionList1);

        //    A.Stretch stretch1 = new A.Stretch();
        //    A.FillRectangle fillRectangle1 = new A.FillRectangle();

        //    stretch1.Append(fillRectangle1);

        //    blipFill1.Append(blip1);
        //    blipFill1.Append(stretch1);

        //    Xdr.ShapeProperties shapeProperties1 = new Xdr.ShapeProperties();

        //    A.Transform2D transform2D1 = new A.Transform2D();
        //    A.Offset offset1 = new A.Offset() { X = 0L, Y = 0L };
        //    A.Extents extents1 = new A.Extents() { Cx = 152381L, Cy = 152381L };

        //    transform2D1.Append(offset1);
        //    transform2D1.Append(extents1);

        //    A.PresetGeometry presetGeometry1 = new A.PresetGeometry() { Preset = A.ShapeTypeValues.Rectangle };
        //    A.AdjustValueList adjustValueList1 = new A.AdjustValueList();

        //    presetGeometry1.Append(adjustValueList1);

        //    shapeProperties1.Append(transform2D1);
        //    shapeProperties1.Append(presetGeometry1);

        //    picture1.Append(nonVisualPictureProperties1);
        //    picture1.Append(blipFill1);
        //    picture1.Append(shapeProperties1);
        //    Xdr.ClientData clientData1 = new Xdr.ClientData();

        //    twoCellAnchor1.Append(fromMarker1);
        //    twoCellAnchor1.Append(toMarker1);
        //    twoCellAnchor1.Append(picture1);
        //    twoCellAnchor1.Append(clientData1);

        //    return twoCellAnchor1;
        //}

        /// <summary>
        /// Generates the image part
        /// </summary>
        /// <param name="imagePart">Instance of the image part</param>
        /// <param name="image">Instance of the 
        /// image which need to be added into the package
        /// </param>
        //private void GenerateImagePartContent(ImagePart imagePart, Image image)
        //{
        //    MemoryStream memStream = new MemoryStream();
        //    image.Save(memStream, ImageFormat.Png);
        //    memStream.Position = 0;
        //    imagePart.FeedData(memStream);
        //    memStream.Close();
        //}

        /// <summary>
        /// Returns the next relationship id for the specified WorksheetPart
        /// </summary>
        /// <param name="sheet1">The worksheetPart</param>
        /// <returns>Returns the next relationship id </returns>
        private int GetNextRelationShipID()
        {
            int nextId = 0;
            List<int> ids = new List<int>();
            foreach (IdPartPair part in worksheetPart1.Parts)
            {
                ids.Add(int.Parse(part.RelationshipId.Replace("rId", string.Empty)));
            }
            if (ids.Count > 0)
                nextId = ids.Max() + 1;
            else
                nextId = 1;
            return nextId;
        }


        //private Image GetImageFromFile(int fileName)
        //{
        //    string path = $"../images/medium/{fileName}.jpg";
        //    //check the existence of the file in disc
        //    if (File.Exists(path))
        //    {
        //        Image image = Image.FromFile(path);
        //        return image;
        //    }
        //    else
        //        return null;
        //}

        private byte[] GetImage(int refId)
        {
            DownloadFileResponse dfs = null;

            byte[] fileByte = new byte[0];
            dfs = FileClient.Get
                 (new DownloadImageByIdRequest
                 {
                     ImageInfo = new ImageMeta
                     {
                         FileRefId = refId,
                         FileCategory = Common.Enums.EbFileCategory.Images
                     }
                 });
            if (dfs.StreamWrapper != null)
            {
                dfs.StreamWrapper.Memorystream.Position = 0;
                fileByte = dfs.StreamWrapper.Memorystream.ToBytes();
            }

            return fileByte;
        }

        private Stream GetImageStream(int refId)
        {
            DownloadFileResponse dfs = null;

            byte[] fileByte = new byte[0];
            dfs = FileClient.Get
                 (new DownloadImageByIdRequest
                 {
                     ImageInfo = new ImageMeta
                     {
                         FileRefId = refId,
                         FileCategory = Common.Enums.EbFileCategory.Images
                     }
                 });
            if (dfs.StreamWrapper != null)
            {
                dfs.StreamWrapper.Memorystream.Position = 0;
                return dfs.StreamWrapper.Memorystream;
            }

            return null;
        }

        private object GetTaggedData(DVStringColumn dVStringColumn, object _formattedData)
        {
            string _html = string.Empty;
            if (_formattedData.ToString() != string.Empty)
            {
                _html = "<div class='dvtaginput'>";
                foreach (string str in _formattedData.ToString().Split(","))
                {
                    _html += $"<span class='dvtagspan'>{str}</span>";
                }
                _html += "</div>";
            }
            return _html;
        }

        private object GetDataforRating(DVNumericColumn dVNumericColumn, object _formattedData)
        {
            if (_formattedData != null && Convert.ToDecimal(_formattedData) != -1)
            {
                var deci = Convert.ToDecimal(_formattedData);
                decimal dPart = Convert.ToDecimal(_formattedData) % 1.0m;
                if (deci > dVNumericColumn.MaxLimit)
                    deci = dVNumericColumn.MaxLimit;
                return $"<div class='rating' data-rateyo-num-stars='{dVNumericColumn.MaxLimit}' data-rateyo-rating='{deci}' data-rateyo-half-star='true'> </div>";
            }
            else
            {
                return "<div class='ratingg'> NA </div>";
            }
            throw new NotImplementedException();
        }

        private object GetMultilineText(string data, int _length, int _lines)
        {
            string info = $"<table><tr><td>{data}</td></tr></table>";
            string formatted = string.Empty;
            int count = 0;
            while (data.Length > _length && count < _lines)
            {
                //trim the string to the maximum length
                var trimmedString = data.Substring(0, _length);

                //re-trim if we are in the middle of a word and 
                trimmedString = trimmedString.Substring(0, Math.Min(trimmedString.Length, trimmedString.LastIndexOf(" ")));
                formatted += trimmedString + "</br> ";
                data = data.Remove(0, trimmedString.Length);
                count++;
            };
            if (data.Length > _length)
                formatted = formatted.Substring(0, formatted.LastIndexOf("</br>")) + " ...";
            else
                formatted = formatted + data + " ...";
            return "<span class='columntooltip' data-toggle='popover' data-contents='" + info.ToBase64() + "'>" + formatted + "</span>"; ;
        }

        public void CreateIntermediateDict(EbDataRow row, EbDataVisualization _dv, CultureInfo _user_culture, User _user, ref EbDataTable _formattedTable, ref Globals globals, bool _isexcel, int i)
        {
            foreach (DVBaseColumn col in dependencyTable)
            {
                try
                {
                    if (col.IsCustomColumn)
                    {
                        if (col is DVButtonColumn)
                            ProcessButtoncolumn(row, globals, col);
                        else if (col is DVApprovalColumn)
                            ProcessApprovalcolumn(col, row, _user);
                        else if (col is DVActionColumn)
                            row[col.Data] = "<i class='fa fa-edit'></i>";
                        else if (col is DVPhoneColumn)
                            ProcessPhonecolumn(col, row, _user);
                        else
                            CustomColumDoCalc4Row(row, _dv, globals, col);
                    }
                    else if (col is DVStringColumn && _dv.AutoGen && col.Name == "eb_action")
                        row[col.Data] = "<i class='fa fa-edit'></i>";
                    var cults = col.GetColumnCultureInfo(_user_culture);
                    object _unformattedData = row[col.Data];
                    object _formattedData = _unformattedData;

                    if (col.RenderType == EbDbTypes.Date || col.RenderType == EbDbTypes.DateTime || col.RenderType == EbDbTypes.Time)
                    {
                        DateTimeformat(_unformattedData, ref _formattedData, ref row, col, cults, _user);
                    }

                    else if (col.RenderType == EbDbTypes.Decimal || col.RenderType == EbDbTypes.Int32 || col.RenderType == EbDbTypes.Int64)
                    {
                        if (col.Name != "id")
                        {
                            if ((col as DVNumericColumn).SuppresIfZero && (_isexcel == false))
                            {
                                _formattedData = (Convert.ToDecimal(_unformattedData) == 0) ? string.Empty : Convert.ToDecimal(_unformattedData).ToString("N", cults.NumberFormat);

                            }
                            else
                                _formattedData = Convert.ToDecimal(_unformattedData).ToString("N", cults.NumberFormat);
                        }
                    }

                    else if (col.RenderType == EbDbTypes.Boolean || col.RenderType == EbDbTypes.BooleanOriginal)
                    {
                        if (col.Type == EbDbTypes.Decimal || col.Type == EbDbTypes.Int32 || col.Type == EbDbTypes.Int64)
                            _formattedData = Convert.ToDecimal(_unformattedData);
                        else if (col.Type == EbDbTypes.String)
                            _formattedData = _unformattedData.ToString();
                    }

                    if (col.Name == "eb_created_by" || col.Name == "eb_lastmodified_by" || col.Name == "eb_loc_id" || col.Name == "eb_createdby")
                    {
                        ModifyEbColumns(col, ref _formattedData, _unformattedData);
                    }
                    if (col.ColumnQueryMapping != null && col.ColumnQueryMapping.Values.Count > 0)
                    {
                        _formattedData = GetDataforPowerSelect(col, _formattedData);
                    }
                    IntermediateDic.Add(col.Data, _formattedData);
                    if ((_dv as EbTableVisualization) == null)
                    {
                        _formattedTable.Rows[i][col.Data] = _formattedData;
                    }
                }
                catch (Exception e)
                {
                    Log.Info("PreProcessing in Create IntermediateDictionay........." + e.StackTrace + "Column Name  ......" + col.Name);
                    Log.Info("PreProcessing in Create IntermediateDictionay........." + e.Message + "Column Name  ......" + col.Name);
                    this._Responsestatus.Message = e.Message;
                }
            }
        }

        private List<DVBaseColumn> GetApprovalColumn(EbTableVisualization ebTableVisualization)
        {
            return ebTableVisualization.Columns.FindAll(e => e is DVApprovalColumn);
        }

        private void GetApprovalData(User _user, List<DVBaseColumn> cols, RowColletion rows)
        {
            try
            {
                foreach (DVBaseColumn _col in cols)
                {
                    DVApprovalColumn col = _col as DVApprovalColumn;

                    EbWebForm Form = EbFormHelper.GetEbObject<EbWebForm>(col.FormRefid, null, this.Redis, this);
                    EbControl[] Allctrls = Form.Controls.FlattenAllEbControls();
                    col.ReviewCtrl = (Allctrls.FirstOrDefault(e => e is EbReview)) as EbReview;

                    int[] eb_src_ids = rows.Select(e => Convert.ToInt32(e[col.FormDataId[0].Data])).ToArray();

                    var _roles = string.Join(",", _user.RoleIds.ToArray());
                    var verid = col.FormRefid.Split("-")[4];
                    string str = string.Empty;
                    if (_user.Roles.Contains(SystemRoles.SolutionOwner.ToString()) || _user.Roles.Contains(SystemRoles.SolutionAdmin.ToString()))
                    {
                        str = string.Format(@"
                    SELECT Q1.*,act.action_name,act.action_unique_id
                    FROM(
	                    SELECT my.id, st.stage_name,st.id as stage_id,my.form_ref_id,my.form_data_id,st.stage_unique_id, my.from_datetime
	                    FROM eb_my_actions my, eb_stages st
	                    WHERE  my.form_ref_id ='{0}'
			                    AND my.is_completed='F' AND my.eb_del='F'
			                    AND st.id=my.eb_stages_id AND st.eb_del='F' 
                                AND my.form_data_id = ANY (ARRAY[{1}]::INT[])
	                    ) Q1
                    LEFT JOIN
	                    eb_stage_actions act
                    ON
	                    Q1.stage_id=act.eb_stages_id AND act.eb_del='F';", col.FormRefid, eb_src_ids.Join(","));
                    }
                    else
                    {
                        str = string.Format(@"
                    SELECT Q1.*,act.action_name,act.action_unique_id
                    FROM(
	                    SELECT my.id, st.stage_name,st.id as stage_id,my.form_ref_id,my.form_data_id,st.stage_unique_id, my.from_datetime
	                    FROM eb_my_actions my, eb_stages st
	                    WHERE ('{0}' = any(string_to_array(user_ids, ',')) OR
	 		                    (string_to_array(role_ids,',')) && (string_to_array('{1}',',')))
                                AND my.form_ref_id ='{2}'
			                    AND my.is_completed='F' AND my.eb_del='F'
			                    AND st.id=my.eb_stages_id AND st.eb_del='F' 
                                AND my.form_data_id = ANY (ARRAY[{3}]::INT[])
	                    ) Q1
                    LEFT JOIN
	                    eb_stage_actions act
                    ON
	                    Q1.stage_id=act.eb_stages_id AND act.eb_del='F' ;", _user.UserId, _roles, col.FormRefid, eb_src_ids.Join(","));
                    }
                    str += string.Format(@"
	                    SELECT app.review_status,app.eb_src_id,my.id,st.stage_name,app.eb_lastmodified_at, app.eb_created_at
	                    FROM eb_approval app,eb_my_actions my, eb_stages st
	                    WHERE  app.eb_ver_id ='{0}' AND app.eb_del='F'
			                    AND my.id=app.eb_my_actions_id
			                    AND st.id = my.eb_stages_id
                                AND my.form_data_id = ANY (ARRAY[{1}]::INT[]);", verid, eb_src_ids.Join(","));
                    str += $@"
SELECT * FROM
(   
    SELECT 
        AL.id, USR.fullname, AL.comments, AL.eb_created_by, AL.eb_created_at, AL.eb_src_id,
        S.stage_name, SA.action_name
    FROM 
        eb_approval_lines AL,
        eb_users USR,
        eb_stages S,
        eb_stage_actions SA
    WHERE  
        AL.eb_ver_id ='{verid}' AND 
        AL.eb_created_by = USR.id AND 
        AL.stage_unique_id = S.stage_unique_id AND 
        S.form_ref_id='{col.FormRefid}' AND 
        AL.action_unique_id = SA.action_unique_id AND 
        SA.eb_stages_id = S.id AND 
        COALESCE(SA.eb_del, 'F') = 'F' AND 
        AL.eb_src_id = ANY (ARRAY[{eb_src_ids.Join(",")}]::INT[]) AND
        COALESCE(AL.eb_del, 'F') = 'F'
    UNION
    SELECT 
        AL.id, USR.fullname, AL.comments, AL.eb_created_by, AL.eb_created_at, AL.eb_src_id,
        'System' AS stage_name, 'Reset' AS action_name
    FROM 
        eb_approval_lines AL,
        eb_users USR
    WHERE  
        AL.eb_ver_id ='{verid}' AND 
        AL.eb_created_by = USR.id AND 
        AL.stage_unique_id = '{FormConstants.__system_stage}' AND 
        AL.action_unique_id = '{FormConstants.__review_reset}' AND         
        AL.eb_src_id = ANY (ARRAY[{eb_src_ids.Join(",")}]::INT[]) AND
        COALESCE(AL.eb_del, 'F') = 'F'
) AS xx
ORDER BY 
    xx.eb_created_at DESC; ";
                    col.ApprovalData = this.EbConnectionFactory.DataDB.DoQueries(str);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

        }

        public ParticularApprovalColumnResponse Any(ParticularApprovalColumnRequest request)
        {
            ParticularApprovalColumnResponse resp = new ParticularApprovalColumnResponse();
            try
            {
                var _roles = string.Join(",", request.UserObj.RoleIds.ToArray());
                var verid = request.RefId.Split("-")[4];
                string str = string.Empty;
                if (request.UserObj.Roles.Contains(SystemRoles.SolutionOwner.ToString()) || request.UserObj.Roles.Contains(SystemRoles.SolutionAdmin.ToString()))
                {
                    str = string.Format(@"
                    SELECT Q1.*,act.action_name,act.action_unique_id
                    FROM(
	                    SELECT my.id, st.stage_name,st.id as stage_id,my.form_ref_id,my.form_data_id,st.stage_unique_id,my.from_datetime
	                    FROM eb_my_actions my, eb_stages st
	                    WHERE 
                                 my.form_ref_id ='{0}' AND my.form_data_id ={1}
			                    AND my.is_completed='F' AND my.eb_del='F'
			                    AND st.id=my.eb_stages_id AND st.eb_del='F' 
	                    ) Q1
                    LEFT JOIN
	                    eb_stage_actions act
                    ON
	                    Q1.stage_id=act.eb_stages_id AND act.eb_del='F' ;", request.RefId, request.RowId);
                }
                else
                {
                    str = string.Format(@"
                    SELECT Q1.*,act.action_name,act.action_unique_id
                    FROM(
	                    SELECT my.id, st.stage_name,st.id as stage_id,my.form_ref_id,my.form_data_id,st.stage_unique_id,my.from_datetime
	                    FROM eb_my_actions my, eb_stages st
	                    WHERE ('{0}' = any(string_to_array(user_ids, ',')) OR
	 		                    (string_to_array(role_ids,',')) && (string_to_array('{1}',',')))
                                AND my.form_ref_id ='{2}' AND my.form_data_id ={3}
			                    AND my.is_completed='F' AND my.eb_del='F'
			                    AND st.id=my.eb_stages_id AND st.eb_del='F' 
	                    ) Q1
                    LEFT JOIN
	                    eb_stage_actions act
                    ON
	                    Q1.stage_id=act.eb_stages_id AND act.eb_del='F' ;", request.UserObj.UserId, _roles, request.RefId, request.RowId);
                }
                str += string.Format(@"
	                    SELECT app.review_status,app.eb_src_id,my.id,st.stage_name,app.eb_lastmodified_at, app.eb_created_at
	                    FROM eb_approval app,eb_my_actions my, eb_stages st
	                    WHERE  app.eb_ver_id ='{0}' AND app.eb_del='F' AND app.eb_src_id={1}
			                    AND my.id=app.eb_my_actions_id
			                    AND st.id = my.eb_stages_id;", verid, request.RowId);
                str += $@"
SELECT * FROM
(   
    SELECT 
        AL.id, USR.fullname, AL.comments, AL.eb_created_by, AL.eb_created_at, AL.eb_src_id,
        S.stage_name, SA.action_name
    FROM 
        eb_approval_lines AL,
        eb_users USR,
        eb_stages S,
        eb_stage_actions SA
    WHERE  
        AL.eb_ver_id ='{verid}' AND 
        AL.eb_created_by = USR.id AND 
        AL.stage_unique_id = S.stage_unique_id AND 
        S.form_ref_id='{request.RefId}' AND 
        AL.action_unique_id = SA.action_unique_id AND 
        SA.eb_stages_id = S.id AND 
        SA.eb_del='F' AND 
        AL.eb_src_id = '{request.RowId}' AND
        COALESCE(AL.eb_del, 'F') = 'F'
    UNION
    SELECT 
        AL.id, USR.fullname, AL.comments, AL.eb_created_by, AL.eb_created_at, AL.eb_src_id,
        'System' AS stage_name, 'Reset' AS action_name
    FROM 
        eb_approval_lines AL,
        eb_users USR
    WHERE  
        AL.eb_ver_id ='{verid}' AND 
        AL.eb_created_by = USR.id AND 
        AL.stage_unique_id = '{FormConstants.__system_stage}' AND 
        AL.action_unique_id = '{FormConstants.__review_reset}' AND         
        AL.eb_src_id = '{request.RowId}' AND
        COALESCE(AL.eb_del, 'F') = 'F'
) AS xx
ORDER BY 
    xx.eb_created_at DESC; ";
                _approvaldata = this.EbConnectionFactory.DataDB.DoQueries(str);
                resp._data = ProcessParticularApprovalcolumn(request.UserObj, request.RowId, request.RefId);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            return resp;
        }

        private string ProcessParticularApprovalcolumn(User _user, int DataId, string FormRefId)
        {
            EbWebForm Form = EbFormHelper.GetEbObject<EbWebForm>(FormRefId, null, this.Redis, this);
            EbControl[] Allctrls = Form.Controls.FlattenAllEbControls();
            EbReview ReviewCtrl = Allctrls.FirstOrDefault(e => e is EbReview) as EbReview;

            string _formattedData = string.Empty;
            var _rows = _approvaldata.Tables[0].Rows;
            if (_rows.Count > 0)
                _formattedData = GetDataforPermissedApprovalColumn(_rows, _user, _approvaldata.Tables[2].Rows, null, null, ReviewCtrl);
            else
            {
                _rows = _approvaldata.Tables[1].Rows;
                if (_rows.Count > 0)
                {
                    _formattedData = GetDataforNotPermissedApprovalColumn(_rows, _user, _approvaldata.Tables[2].Rows, null, null, DataId, FormRefId);
                }
                else
                    _formattedData = string.Empty;
            }
            return _formattedData;
        }

        private void ProcessApprovalcolumn(DVBaseColumn col, EbDataRow row, User _user)
        {
            string _formattedData = string.Empty;
            if (col.ApprovalData != null)
            {
                var _rows = col.ApprovalData.Tables[0].Rows.FindAll(_row => Convert.ToInt32(_row["form_data_id"]) == Convert.ToInt32(row[(col as DVApprovalColumn).FormDataId[0].Data]));
                var linesRows = col.ApprovalData.Tables[2].Rows.FindAll(_row => Convert.ToInt32(_row["eb_src_id"]) == Convert.ToInt32(row[(col as DVApprovalColumn).FormDataId[0].Data]));
                if (_rows.Count > 0)
                    _formattedData = GetDataforPermissedApprovalColumn(_rows, _user, linesRows, row, col as DVApprovalColumn, null);
                else
                {
                    _rows = col.ApprovalData.Tables[1].Rows.FindAll(_row => Convert.ToInt32(_row["eb_src_id"]) == Convert.ToInt32(row[(col as DVApprovalColumn).FormDataId[0].Data]));
                    if (_rows.Count > 0)
                        _formattedData = GetDataforNotPermissedApprovalColumn(_rows, _user, linesRows, row, col as DVApprovalColumn, 0, null);
                    else
                    {
                        _formattedData = string.Empty;
                        var indx = -1;
                        if (_dV.Columns.Get("eb_review_status") != null)
                        {
                            indx = _dV.Columns.Get("eb_review_status").Data;
                            row[indx] = _formattedData;
                        }
                        if (_dV.Columns.Get("eb_review_stage") != null)
                        {
                            indx = _dV.Columns.Get("eb_review_stage").Data;
                            row[indx] = _formattedData;
                        }
                    }
                }
            }
            row[col.Data] = _formattedData;
        }

        private bool CheckReviewResetPermission(User _user, DVApprovalColumn col)
        {
            bool hasRoleMatch = _user.RoleIds.Contains((int)SystemRoles.SolutionOwner) || _user.RoleIds.Contains((int)SystemRoles.SolutionAdmin);
            if (col?.ResetterRoles != null)
                hasRoleMatch = hasRoleMatch || _user.RoleIds.Select(x => x).Intersect(col.ResetterRoles).Any();
            return hasRoleMatch;
        }

        private string IsCommentsRequired(EbReview ReviewCtrl, string stage, string action)
        {
            if (ReviewCtrl.FormStages.Find(e => e.EbSid == stage) is EbReviewStage currentStage)
            {
                if (currentStage.StageActions.Find(e => e.EbSid == action) is EbReviewAction currentAction && currentAction.CommentsRequired)
                    return "req='y'";
            }
            return string.Empty;
        }

        private string GetDataforPermissedApprovalColumn(List<EbDataRow> rows, User _user, List<EbDataRow> linesRows, EbDataRow row, DVApprovalColumn col, EbReview ReviewCtrl)
        {
            if (col != null)
                ReviewCtrl = col.ReviewCtrl;
            bool enableReset = CheckReviewResetPermission(_user, col);
            string _data = $@"<div class='nav-container'>
                          <ul class='nav nav-tabs'>
                            <li class='active'><a data-toggle='tab' href='#action'>Action</a></li>
                            <li><a data-toggle='tab' href='#history'>History</a></li>
                            {(enableReset ? "<li><a data-toggle='tab' href='#resetstage'>Reset</a></li>" : "")}
                            </ul>";
            _data += @"<div class='tab-content'>
                                  <div class='tab-pane active' id='action'>";
            _data += "<table class='action-table'><tr><td class='action-td stage-label' colspan='2'><label>" + rows[0]["stage_name"].ToString() + "</label></tr>";
            _data += "<tr><td class='action-td'>Actions</td><td class='action-td'><select class='selectpicker stage_actions'>";
            ApprovalData _obj = new ApprovalData();
            _obj.Stage_unique_id = rows[0]["stage_unique_id"].ToString();
            _obj.My_action_id = rows[0]["id"].ToString();
            _obj.Form_ref_id = rows[0]["form_ref_id"].ToString();
            _obj.Form_data_id = rows[0]["form_data_id"].ToString();
            var _date = Convert.ToDateTime(rows[0]["from_datetime"]);
            var _time = _date.TimeAgoShort();
            var _tooltipdate = _date.ConvertFromUtc(_user.Preference.TimeZone).ToString(_user.Preference.GetShortDatePattern()) + " " + _date.ConvertFromUtc(_user.Preference.TimeZone).ToString(_user.Preference.GetShortTimePattern());
            foreach (EbDataRow _ebdatarow in rows)
            {
                _obj.Action_unique_id = _ebdatarow["action_unique_id"].ToString();
                _data += "<option " + IsCommentsRequired(ReviewCtrl, _obj.Stage_unique_id, _obj.Action_unique_id) + " value='" + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(_obj))) + "'>" + _ebdatarow["action_name"].ToString() + "</option>";
            }
            _data += "</select></td></tr>";
            _data += "<tr><td class='action-td'>Comments</td><td class='action-td'><textarea class='comment-text'></textarea></td></tr>";
            _data += "<tr><td class='action-td'></td><td class='action-td'><button class='btn stage-btn btn-action_execute' data-toggle='tooltip' title='Execute Review'>Execute</button></td></tr>";//<i class='fa fa-play' aria-hidden='true'></i>
            _data += "</table></div>";
            _data += "<div class='tab-pane' id='history'>";
            _data += GetApprovalHistoryString(linesRows, _user);
            _data += "</div>";
            if (enableReset)
            {
                _obj.Stage_unique_id = FormConstants.__system_stage;
                _obj.Action_unique_id = FormConstants.__review_reset;
                _obj.My_action_id = "0";

                _data += $@"
<div class='tab-pane' id='resetstage'>
    <table class='action-table'>
        <tr><td class='action-td'>Comments</td><td class='action-td'><textarea class='comment-text'></textarea></td></tr>
        <tr><td class='action-td'></td><td class='action-td'>
            <button class='btn stage-btn btn-action_reset' data-toggle='tooltip' title='Reset' data-json = '{Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(_obj)))}'>Reset</button>
        </td></tr>
    </table>
</div>";
            }
            _data += "</div></div>";
            string _stage = "<div class='stage-div'>";
            var _latestHistory = GetLatestHistory(linesRows);
            if (string.IsNullOrWhiteSpace(_latestHistory))
                _latestHistory = rows[0]["stage_name"].ToString();
            _stage += "<div class='stage-div-inner stage-status-cont'><span class='stage-status'>" + _latestHistory + "</span></div>";
            _stage += $"<div class='stage-div-inner'><div class='icon-status-cont'>" +
                $"<span class='status-icon'><i class='fa fa-commenting color-warning' aria-hidden='true'></i></span>" +
                $"<span class='status-label label label-warning'>Review Required</span>" +
                $"<span class='status-time' title='{_tooltipdate}'>{_time}</span>" +
                $"</div></div></div>";
            string _button = "<div class='stage-div'><div class='stage-div-inner'><button class='btn stage-btn btn-approval_popover' data-contents='" + _data.ToBase64() + "' data-toggle='popover'><i class='fa fa-pencil' aria-hidden='true'></i></button></div></div>";//
            if (row != null)
            {
                var indx = -1;
                if (_dV.Columns.Get("eb_review_status") != null)
                {
                    indx = _dV.Columns.Get("eb_review_status").Data;
                    row[indx] = "Review Required";
                }
                if (_dV.Columns.Get("eb_review_stage") != null)
                {
                    indx = _dV.Columns.Get("eb_review_stage").Data;
                    row[indx] = rows[0]["stage_name"].ToString();
                }
            }

            return "<div class='stage_actions_cont'>" + _stage + _button + "</div>";
        }

        private string GetDataforNotPermissedApprovalColumn(List<EbDataRow> rows, User _user, List<EbDataRow> linesRows, EbDataRow row, DVApprovalColumn col, int dataId, string formRefId)
        {
            bool enableReset = CheckReviewResetPermission(_user, col);
            string _stage = "<div class='stage_actions_cont'>";
            string _stage_name = "<div class='stage-div'>";
            string _history = $@"<div class='nav-container'>
                          <ul class='nav nav-tabs'>
                            <li class='active'><a data-toggle='tab' href='#history'>History</a></li>
                            {(enableReset ? "<li><a data-toggle='tab' href='#resetstage'>Reset</a></li>" : "")}
                            </ul>";
            _history += @"<div class='tab-content'>
                                  <div class='tab-pane active' id='history'>";
            _history += GetApprovalHistoryString(linesRows, _user);
            _history += "</div>";

            if (enableReset)
            {
                ApprovalData _obj = new ApprovalData
                {
                    Form_ref_id = col != null ? col.FormRefid : formRefId,
                    Form_data_id = row != null ? Convert.ToString(row[col.FormDataId[0].Data]) : dataId.ToString(),
                    Stage_unique_id = FormConstants.__system_stage,
                    Action_unique_id = FormConstants.__review_reset,
                    My_action_id = "0"
                };

                _history += $@"
<div class='tab-pane' id='resetstage'>
    <table class='action-table'>
        <tr><td class='action-td'>Comments</td><td class='action-td'><textarea class='comment-text'></textarea></td></tr>
        <tr><td class='action-td'></td><td class='action-td'>
            <button class='btn stage-btn btn-action_reset' data-toggle='tooltip' title='Reset' data-json = '{Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(_obj)))}'>Reset</button>
        </td></tr>
    </table>
</div>";
            }

            _history += "</div></div> ";
            var _time = string.Empty; var _tooltipdate = string.Empty;
            DateTime _date = DateTime.Now;
            var _latestHistory = string.Empty;
            if (linesRows != null && linesRows.Count > 0)
            {
                _date = Convert.ToDateTime(linesRows[0]["eb_created_at"]);
                _latestHistory = GetLatestHistory(linesRows);
            }
            else
            {
                _date = Convert.ToDateTime(rows[0]["eb_lastmodified_at"]);
                if (_date == DateTime.MinValue)
                    _date = Convert.ToDateTime(rows[0]["eb_created_at"]);
                _latestHistory = rows[0]["stage_name"].ToString();
            }
            _time = _date.TimeAgoShort();
            _tooltipdate = _date.ConvertFromUtc(_user.Preference.TimeZone).ToString(_user.Preference.GetShortDatePattern()) + " " + _date.ConvertFromUtc(_user.Preference.TimeZone).ToString(_user.Preference.GetShortTimePattern());
            if (rows != null && rows.Count == 1)
            {
                var _status = GetSynonymsforReviewStatus(rows[0]["review_status"].ToString());
                var _icon = GetIconforReviewStatus(rows[0]["review_status"].ToString());
                var _label = GetLabelStyleforReviewStatus(rows[0]["review_status"].ToString());

                _stage_name += "<div class='stage-div-inner stage-status-cont'><span class='stage-status'>" + _latestHistory + "</span></div>";
                _stage_name += "<div class='stage-div-inner'><div class='icon-status-cont'>" +
                    "<span class='status-icon'><i class='" + _icon + "' aria-hidden='true'></i></span>" +
                    "<span class='status-label label " + _label + "'>" + _status + "</span>" +
                    $"<span class='status-time' title='{_tooltipdate}'>{_time}</span>" +
                    "</div></div>";
            }
            if (row != null)
            {
                var indx = -1;
                if (_dV.Columns.Get("eb_review_status") != null)
                {
                    indx = _dV.Columns.Get("eb_review_status").Data;
                    row[indx] = rows[0]["review_status"].ToString();
                }
                if (_dV.Columns.Get("eb_review_stage") != null)
                {
                    indx = _dV.Columns.Get("eb_review_stage").Data;
                    row[indx] = rows[0]["stage_name"].ToString();
                }
            }
            string _button = "<div class='stage-div'><div class='stage-div-inner'><button class='btn stage-btn btn-approval_popover' data-contents='" + _history.ToBase64() + "' data-toggle='popover'><i class='fa fa-history' aria-hidden='true'></i></button></div></div>";
            _stage_name += "</div>";
            return _stage + _stage_name + _button + "</div>";
        }

        private string GetApprovalHistoryString(List<EbDataRow> ebDataRows, User _user)
        {
            string _history = string.Empty;
            if (ebDataRows != null && ebDataRows.Count > 0)
            {
                _history += "<table class='table'><thead class='history-head'><tr><th>Date</th><th>Stage</th><th>Action</th><th>User</th><th>Comments</th></tr></thead><tbody class='history-body'>";

                foreach (EbDataRow _ebdatarow in ebDataRows)
                {
                    var _zone = Convert.ToDateTime(_ebdatarow["eb_created_at"]).ConvertFromUtc(_user.Preference.TimeZone);
                    var __date = _zone.ToString(_user.Preference.GetShortDatePattern()) + "<br>" + _zone.ToString(_user.Preference.GetShortTimePattern());
                    _history += "<tr><td class='datetime-td'>" + __date.ToString() + "</td>";
                    _history += "<td>" + _ebdatarow["stage_name"].ToString() + "</td>";
                    _history += "<td>" + _ebdatarow["action_name"].ToString() + "</td>";
                    _history += "<td class='image-td'><span><img src='/images/dp/" + _ebdatarow["eb_created_by"].ToString() + ".png' class='history-image Eb_Image' onerror='imgError(this);'></span>" +
                                "<span>" + _ebdatarow["fullname"].ToString() + "</span></td>";
                    _history += "<td class='comment-td'>" + _ebdatarow["comments"].ToString() + "</td></tr>";
                }

                _history += "</tbody></table>";
            }
            else
                _history = "<div>No Action Performed yet.</div>";
            return _history;
        }

        private string GetSynonymsforReviewStatus(string status)
        {
            if (status == "Abandoned")
                return "Review Abandoned";
            else if (status == "Completed")
                return "Review Completed";
            else if (status == "In Process")
                return "Review Pending";
            return string.Empty;
        }

        private string GetIconforReviewStatus(string status)
        {
            if (status == "Abandoned")
                return "fa fa-ban color-red";
            else if (status == "Completed")
                return "fa fa-check color-green";
            else if (status == "In Process")
                return "fa fa-spinner color-blue";
            return string.Empty;
        }

        private string GetLabelStyleforReviewStatus(string status)
        {
            if (status == "Abandoned")
                return "label-danger";
            else if (status == "Completed")
                return "label-success";
            else if (status == "In Process")
                return "label-info";
            return string.Empty;
        }

        private string GetLatestHistory(List<EbDataRow> rows)
        {
            string _history = string.Empty;
            if (rows != null && rows.Count > 0)
            {
                _history += rows[0]["stage_name"].ToString();
                _history += " (" + rows[0]["action_name"].ToString() + ") ";
            }
            return _history;
        }

        private object GetDataforPowerSelect(DVBaseColumn col, object _formattedData)
        {
            string[] vmArray = _formattedData.ToString().Split(",");
            string data = string.Empty;
            foreach (string vm in vmArray)
                data += (vm != "" && col.ColumnQueryMapping.Values.ContainsKey(Convert.ToInt32(vm))) ? col.ColumnQueryMapping.Values[Convert.ToInt32(vm)] + " ," : string.Empty + " ,";
            return data.Substring(0, data.Length - 1);
        }

        public void ProcessButtoncolumn(EbDataRow row, Globals globals, DVBaseColumn customCol)
        {
            if (customCol is DVButtonColumn)
            {
                bool result = (customCol as DVButtonColumn).RenderCondition.EvaluateExpression(row, ref globals);
                if ((customCol as DVButtonColumn).RenderCondition.RenderAS == AdvancedRenderType.Default)
                {
                    if (result == (customCol as DVButtonColumn).RenderCondition.GetBoolValue())
                        row[customCol.Data] = $"<button class='{(customCol as DVButtonColumn).ButtonClassName}'>{(customCol as DVButtonColumn).ButtonText}</button>";
                    else
                        row[customCol.Data] = string.Empty;
                }
            }
        }

        public void ProcessPhonecolumn(DVBaseColumn customCol, EbDataRow row, User _user)
        {
            if (customCol is DVPhoneColumn)
            {
                DVPhoneColumn Phonecolumn = customCol as DVPhoneColumn;
                DVBaseColumn MapColumn = Phonecolumn.MappingColumn;
                if (MapColumn.Name != null)
                    row[customCol.Data] = IntermediateDic[MapColumn.Data];
                else
                    row[customCol.Data] = string.Empty;
            }
        }

        public void ModifyPhonecolumn(DVBaseColumn phonecol, ref object _formattedData)
        {
            string _disabled = (this.EbConnectionFactory.SMSConnection != null) ? string.Empty : "disabled";
            _formattedData = "<div class='smsdiv'><button class='smsbutton btn' data-colname='" + phonecol.Name + "' " + _disabled + "><i class='fa fa-phone smsicon' aria-hidden='true'></i></button><span class='smstext'>" + _formattedData + "</span></div>";

        }

        public void DataTable2FormatedTable4Calendar(EbDataRow row, List<EbDataRow> Customrows, EbDataVisualization _dv, CultureInfo _user_culture, User _user,
            ref EbDataTable _formattedTable, ref Globals globals, int i, Dictionary<string, DynamicObj> _hourCount, DVBaseColumn DateColumn, ref Dictionary<int, List<object>> summary)
        {
            try
            {
                bool _islink = ((_dv as EbCalendarView).ObjectLinks.Count == 1) ? true : false;
                foreach (DVBaseColumn col in (_dv as EbCalendarView).KeyColumns)
                {
                    _formattedTable.Rows[i][col.Data] = formatColumn(col as CalendarDynamicColumn, row[col.OIndex], _user_culture, _user, ref globals);
                }
                //foreach (DVBaseColumn col in (_dv as EbCalendarView).LinesColumns)
                //{
                //    //_tempdatatable.Rows[i][col.Data] = Customrows[0][col.OIndex];
                //}

                this.CalendarProcessing(_hourCount, ref _formattedTable, Customrows, DateColumn, _dv, _islink, i, _user_culture, _user, ref globals, ref summary);

            }
            catch (Exception e)
            {
                Log.Info("PreProcessing in DataTable2FormatedTable4Calendar Exception........." + e.StackTrace);
                Log.Info("PreProcessing in DataTable2FormatedTable4Calendar Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
            }

        }

        public void CalendarProcessing(Dictionary<string, DynamicObj> _hourCount, ref EbDataTable _formattedTable, List<EbDataRow> Customrows, DVBaseColumn DateColumn,
            EbDataVisualization _dv, bool _islink, int i, CultureInfo _user_culture, User _user, ref Globals globals, ref Dictionary<int, List<object>> summary)
        {
            try
            {
                var _array = _hourCount.Keys.ToList();
                Dictionary<string, List<object>> Rowsummaryval = new Dictionary<string, List<object>>();
                foreach (DVBaseColumn col in (_dv as EbCalendarView).DateColumns)
                {
                    var CalendarCol = (col as CalendarDynamicColumn);
                    string _formatteddata = string.Empty;
                    int summaryval = 0;
                    string _tooltip = "<table>";
                    int DataColumnsincrementer = 0;
                    foreach (DVBaseColumn datacol in (_dv as EbCalendarView).DataColumns)
                    {
                        if (datacol.bVisible)
                        {
                            _hourCount.Clear();

                            foreach (var key in _array)
                            {
                                _hourCount.Add(key, new DynamicObj());
                            }

                            var _Customrows = Customrows.FindAll(row => DateTimeHelper.IsBewteenTwoDates(Convert.ToDateTime(row[DateColumn.OIndex]), CalendarCol.StartDT, CalendarCol.EndDT));

                            if (datacol.ConditionalFormating.Count > 0)
                            {
                                if (_Customrows.Count > 0)
                                {
                                    EbDataRow dr = _Customrows[0];
                                    if (_hourCount[CalendarCol.Name].Row == null)
                                        _hourCount[CalendarCol.Name].Row = System.Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(string.Join(",", dr.ToArray())));

                                    var _data = dr[datacol.OIndex];
                                    summaryval = Convert.ToInt32(_data);

                                    this.conditinallyformatColumn(datacol, ref _data, _data, dr, ref globals);

                                    _hourCount[CalendarCol.Name].Value = _data;
                                }
                                else
                                {
                                    if (CalendarCol.StartDT.Date <= DateTime.Now)
                                    {
                                        _hourCount[CalendarCol.Name].Value = "<i class='fa fa-times' aria-hidden='true' style='color:red'></i>";
                                    }
                                }
                            }

                            else if (datacol.AggregateFun == AggregateFun.Count)
                            {
                                foreach (EbDataRow dr in _Customrows)
                                {
                                    if (_hourCount[CalendarCol.Name].Row == null)
                                        _hourCount[CalendarCol.Name].Row = System.Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(string.Join(",", dr.ToArray())));
                                    var data = Convert.ToInt32(_hourCount[CalendarCol.Name].Value);
                                    _hourCount[CalendarCol.Name].Value = ++data;

                                }
                                summaryval = Convert.ToInt32(_hourCount[CalendarCol.Name].Value);
                            }

                            else if (datacol.AggregateFun == AggregateFun.Sum)
                            {
                                if (datacol.Type == EbDbTypes.Int32 || datacol.Type == EbDbTypes.Int64 || datacol.Type == EbDbTypes.Decimal)
                                {
                                    foreach (EbDataRow dr in _Customrows)
                                    {
                                        var _data = dr[datacol.OIndex];
                                        if (_hourCount[CalendarCol.Name].Row == null)
                                            _hourCount[CalendarCol.Name].Row = System.Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(string.Join(",", dr.ToArray())));
                                        var data = Convert.ToInt32(_hourCount[CalendarCol.Name].Value);
                                        _hourCount[CalendarCol.Name].Value = data + Convert.ToInt32(_data);
                                    }
                                    summaryval = Convert.ToInt32(_hourCount[CalendarCol.Name].Value);
                                }
                            }

                            var ValueTo = this.formatColumn(datacol as CalendarDynamicColumn, _hourCount[col.Name].Value, _user_culture, _user, ref globals);
                            ValueTo = (ValueTo == null) ? "" : ValueTo;

                            _tooltip += $"<tr><td> {datacol.Name} &nbsp; : &nbsp; {ValueTo}</td></tr>";

                            object _val = (_islink) ? "<a href = '#' oncontextmenu = 'return false' class ='tablelink4calendar' data-popup='true' data-link='" + (_dv as EbCalendarView).ObjectLinks[0].ObjRefId + "' data-colindex='" + CalendarCol.Data + "'  data-column='" + col.Name + "'>" + ValueTo + "</a>" : ValueTo.ToString();

                            var _span = $"<span hidden-row={_hourCount[col.Name].Row} class='columntooltip' data-toggle='popover' data-contents='@@tooltip@@'>{_val}</span>";
                            _formatteddata += $"<div class='dataclass { datacol.Name}_class'>{_span }</div>";
                            // for column aggregate
                            if (summary.ContainsKey(col.Data))
                                summary[col.Data][2 * DataColumnsincrementer] = Convert.ToInt32(summary[col.Data][2 * DataColumnsincrementer]) + summaryval;
                            // for row aggregate
                            if (!Rowsummaryval.ContainsKey(datacol.Name))
                                Rowsummaryval.Add(datacol.Name, new List<object> { summaryval });
                            else
                                Rowsummaryval[datacol.Name].Add(summaryval);

                            DataColumnsincrementer++;
                        }
                    }
                    _tooltip += "</table>";
                    _formattedTable.Rows[i][col.Data] = _formatteddata.ToString().Replace("@@tooltip@@", _tooltip.ToBase64());
                }
                var tooltip = "<table>";
                var formatteddata = string.Empty;
                int incre = -1;
                foreach (var xx in Rowsummaryval)
                {
                    incre++;
                    summary[_formattedTable.Columns.Count - 2][2 * incre] = Convert.ToInt32(summary[_formattedTable.Columns.Count - 2][2 * incre]) + xx.Value.Sum(x => Convert.ToInt32(x));
                    tooltip += $"<tr><td> {xx.Key} &nbsp; : &nbsp; {xx.Value.Sum(x => Convert.ToInt32(x))}</td></tr>";
                    var _val = (xx.Value.Sum(x => Convert.ToInt32(x)) == 0) ? "" : xx.Value.Sum(x => Convert.ToInt32(x)).ToString();
                    var _span = $"<span class='columntooltip' data-toggle='popover' data-contents='@@tooltip@@'>{_val}</span>";
                    formatteddata += $"<div class='dataclass { xx.Key}_class'>{_span }</div>";
                }
                tooltip += "</table>";
                _formattedTable.Rows[i][_formattedTable.Columns.Count - 2] = formatteddata.ToString().Replace("@@tooltip@@", tooltip.ToBase64());
            }
            catch (Exception e)
            {
                Log.Info("PreProcessing in CalendarDayWise Exception........." + e.StackTrace);
                Log.Info("PreProcessing in CalendarDayWise Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
            }
        }

        public PrePrcessorReturn PreProcessingPivot(ref EbDataSet _dataset, List<Param> Parameters, ref EbDataVisualization _dv, User _user)
        {
            try
            {
                var _user_culture = CultureHelper.GetSerializedCultureInfo(_user.Preference.Locale).GetCultureInfo();

                var colCount = _dataset.Tables[0].Columns.Count;

                dataset = _dataset;
                EbDataSet tempdataset = new EbDataSet();
                Globals globals = new Globals();
                Dictionary<int, List<object>> summary = new Dictionary<int, List<object>>();
                //this.CreateCustomcolumn4Pivot(_dataset, ref tempdataset, Parameters, ref _dv, ref _hourCount);
                EbDataTable _formattedTable = new EbDataTable();
                _dv.Columns = new DVColumnCollection();
                var columnY = _pivotConfig.Rows[0].Name;
                var columnX = _pivotConfig.Columns[0].Name;
                var columnZ = _pivotConfig.Values[0].Name;
                _dv.Columns.Add(new DVBaseColumn { Data = 0, Name = columnY, sTitle = columnY, Type = _pivotConfig.Rows[0].Type, bVisible = true });
                _formattedTable.Columns.Add(_formattedTable.NewDataColumn(0, columnY, EbDbTypes.String));
                int i = 1;
                for (int j = 0; j < _dataset.Tables[0].Rows.Count; j++)
                {
                    string colname = _dataset.Tables[0].Rows[j][columnX].ToString();
                    DVBaseColumn newcol = new DVBaseColumn { Data = i, Name = colname, sTitle = colname, Type = _pivotConfig.Columns[0].Type, bVisible = true };
                    if (!_dv.Columns.Contains(colname))
                    {
                        _dv.Columns.Add(newcol);
                        summary.Add(i, new List<object> { 0, 0 });
                        _formattedTable.Columns.Add(_formattedTable.NewDataColumn(i++, colname, _pivotConfig.Columns[0].Type));
                    }
                }
                _formattedTable.Columns.Add(_formattedTable.NewDataColumn(i, "Total", EbDbTypes.Int32));
                _dv.Columns.Add(new DVBaseColumn { Data = i++, Name = "Total", sTitle = "Total", Type = EbDbTypes.Int32, bVisible = true });
                _formattedTable.Columns.Add(_formattedTable.NewDataColumn(i, "serial", EbDbTypes.Int32));
                List<string> columnYValues = new List<string>();
                List<string> columnZValues = new List<string>();
                //RowColletion rows = null;
                int k = 0;
                for (i = 0; i < _dataset.Tables[0].Rows.Count; i++)
                {
                    EbDataRow row = _dataset.Tables[0].Rows[i];
                    string colYVal = row[columnY].ToString();
                    if (!columnYValues.Contains(colYVal))
                    {
                        //EbDataRow[] rows = _dataset.Tables[0].Rows.Select(columnY + "='" + colYVal + "'");
                        columnYValues.Add(colYVal);
                        _formattedTable.Rows.Add(_formattedTable.NewDataRow2());
                        _formattedTable.Rows[k][_formattedTable.Columns.Count - 1] = (k + 1);
                        _formattedTable.Rows[k][0] = colYVal;
                        columnZValues = new List<string>();
                        foreach (DVBaseColumn col in _dv.Columns)
                        {
                            if (col.Name == row[columnX].ToString())
                            {
                                _formattedTable.Rows[k][col.Name] = row[columnZ].ToString();
                                columnZValues.Add(row[columnZ].ToString());
                                summary[col.Data][0] = Convert.ToInt32(summary[col.Data][0]) + Convert.ToInt32(row[columnZ]);
                            }
                        }
                        _formattedTable.Rows[k][_formattedTable.Columns.Count - 2] = columnZValues.Sum(x => Convert.ToInt32(x));
                        k++;
                    }
                }

                _formattedTable.Rows.Add(_formattedTable.NewDataRow2());
                _formattedTable.Rows[k][0] = "Total";
                foreach (DVBaseColumn col in _dv.Columns)
                {
                    if (summary.Keys.Contains(col.Data))
                        _formattedTable.Rows[k][col.Name] = summary[col.Data][0];
                }
                //DataTable2FormatedTable4Pivot(_dataset.Tables[0].Rows[i], ref _dv, ref _formattedTable);
                return new PrePrcessorReturn { FormattedTable = _formattedTable, rows = _dataset.Tables[0].Rows };
            }

            catch (Exception e)
            {
                Log.Info("Before PreProcessing in PreProcessingPivot  Exception........." + e.StackTrace);
                Log.Info("Before PreProcessing in PreProcessingPivot  Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
            }
            return null;
        }

        private void DataTable2FormatedTable4Pivot(EbDataRow row, ref EbDataVisualization dv, ref EbDataTable formattedTable, int i)
        {
            IntermediateDic = new Dictionary<int, object>();
            //formattedTable.Rows[i][formattedTable.Columns.Count - 1] = i + 1;//serial
            //CreateIntermediateDict(row, dv, _user_culture, _user, ref formattedTable, ref globals, false, i);
            var rowdata = string.Empty;
            foreach (DVBaseColumn col in _pivotConfig.Rows)
            {
                if (row[col.Name] != null)
                    formattedTable.Rows.Add(formattedTable.NewDataRow2());

            }
            var j = 0;
            foreach (DVBaseColumn col in _pivotConfig.Rows)
            {
                object _unformattedData = row[col.Name] == null ? "" : row[col.Name];
                object _formattedData = IntermediateDic[col.Data] == null ? "" : IntermediateDic[col.Data];
                formattedTable.Rows[i + j++][0] = _formattedData;
            }

            foreach (DVBaseColumn col in dv.Columns)
            {

            }
        }

        public void CreateCustomcolumn4Pivot(EbDataSet dataset, ref EbDataSet tempdataset, List<Param> parameters, ref EbDataVisualization dv, ref Dictionary<string, DynamicObj> hourCount)
        {
            throw new NotImplementedException();
        }

        public string GetKey(DateTime st, DateTime end)
        {
            string newstr = st.ToString("dd/MM/yyyy:HH:mm:ss") + end.ToString("dd/MM/yyyy:HH:mm:ss");
            return Regex.Replace(newstr, "[^a-zA-Z0-9_]+", "_");
        }

        public object formatColumn(CalendarDynamicColumn col, object _unformattedData, CultureInfo _user_culture, User _user, ref Globals globals)
        {
            try
            {
                var cults = col.GetColumnCultureInfo(_user_culture);
                object _formattedData = _unformattedData;

                if (col.RenderType == EbDbTypes.Decimal || col.RenderType == EbDbTypes.Int32 || col.RenderType == EbDbTypes.Int64)
                {
                    if (col.SubType == NumericSubType.None)
                    {
                        return _formattedData;
                    }
                    else
                    {
                        return ConverToTime(col, _formattedData);
                    }
                }
                else
                    return _formattedData;
            }
            catch (Exception e)
            {
                Log.Info("PreProcessing in Calendar Exception........." + e.StackTrace + "Column Name ....." + col.Name);
                Log.Info("PreProcessing in Calendar Exception........." + e.Message + "Column Name ....." + col.Name);
                this._Responsestatus.Message = e.Message;
            }
            return 0;
        }

        public string ConverToTime(CalendarDynamicColumn col, object Value)
        {
            TimeSpan time = new TimeSpan(0, 0, 0, 0);
            if (col.SubType == NumericSubType.Time_Interval_In_Hour)
                time = TimeSpan.FromHours(Convert.ToDouble(Value));
            else if (col.SubType == NumericSubType.Time_Interval_In_Minute)
                time = TimeSpan.FromMinutes(Convert.ToDouble(Value));
            else if (col.SubType == NumericSubType.Time_Interval_In_Second)
                time = TimeSpan.FromSeconds(Convert.ToDouble(Value));

            if (col.SubTypeFormat == NumericSubTypeFromat.Days)
            {
                return ToReadableString(time);
            }

            else if (col.SubTypeFormat == NumericSubTypeFromat.Hours)
            {
                string formatted = string.Format("{0}{1}{2}",
                    time.Hours > 0 ? string.Format("{0} h ", time.Days * 24 + time.Hours) : string.Empty,
                    time.Minutes > 0 ? string.Format("{0} m ", time.Minutes) : string.Empty,
                    time.Seconds > 0 ? string.Format("{0} s", time.Seconds) : string.Empty);
                return formatted;
            }

            return string.Empty;
        }

        public string ToReadableString(TimeSpan span)
        {
            string formatted = string.Format("{0}{1}{2}{3}",
                span.Duration().Days > 0 ? string.Format("{0}d ", span.Days) : string.Empty,
                span.Duration().Hours > 0 ? string.Format("{0}h ", span.Hours) : string.Empty,
                span.Duration().Minutes > 0 ? string.Format("{0}m ", span.Minutes) : string.Empty,
                span.Duration().Seconds > 0 ? string.Format("{0}s", span.Seconds) : string.Empty);

            if (string.IsNullOrEmpty(formatted)) formatted = string.Empty;

            return formatted;
        }

        public void ModifyEbColumns(DVBaseColumn col, ref object _formattedData, object _unformattedData)
        {
            try
            {
                if (col.Name == "eb_created_by" || col.Name == "eb_lastmodified_by" || col.Name == "eb_createdby")
                {
                    try
                    {
                        int user_id = Convert.ToInt32(_unformattedData);
                        if (user_id == 0)
                            _formattedData = "";
                        else if (this._ebSolution.Users != null && this._ebSolution.Users.ContainsKey(user_id))
                        {
                            _formattedData = this._ebSolution.Users[user_id];
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message + e.StackTrace);
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
            catch (Exception e)
            {
                Log.Info("Modify EbColumns in datatable Exception........." + e.StackTrace + "Column Name  ......" + col.Name);
                Log.Info("Modify EbColumns in datatable Exception........." + e.Message + "Column Name  ......" + col.Name);
                this._Responsestatus.Message = e.Message;
            }
        }

        public void DateTimeformat(object _unformattedData, ref object _formattedData, ref EbDataRow row, DVBaseColumn col, CultureInfo cults, User _user)
        {
            try
            {
                _unformattedData = (_unformattedData == DBNull.Value) ? DateTime.MinValue : _unformattedData;
                if ((col as DVDateTimeColumn).Format == DateFormat.Date)
                {
                    if ((col as DVDateTimeColumn).Pattern == DatePattern.MMMM_yyyy)
                        _formattedData = (((DateTime)_unformattedData).Date != DateTime.MinValue) ? Convert.ToDateTime(_unformattedData).ToString(cults.DateTimeFormat.YearMonthPattern) : string.Empty;
                    else if ((col as DVDateTimeColumn).Pattern == DatePattern.MMM_yyyy)
                        _formattedData = (((DateTime)_unformattedData).Date != DateTime.MinValue) ? Convert.ToDateTime(_unformattedData).ToString("MMM" + cults.DateTimeFormat.DateSeparator + "yyyy") : string.Empty;
                    else
                        _formattedData = (((DateTime)_unformattedData).Date != DateTime.MinValue) ? Convert.ToDateTime(_unformattedData).ToString("d", cults.DateTimeFormat) : string.Empty;
                    if (col.Data < row.Count)
                        row[col.Data] = Convert.ToDateTime(_unformattedData).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                }
                else if ((col as DVDateTimeColumn).Format == DateFormat.DateTime)
                {
                    if ((col as DVDateTimeColumn).ConvretToUsersTimeZone)
                        _formattedData = (((DateTime)_unformattedData).Date != DateTime.MinValue) ? Convert.ToDateTime(_unformattedData).ConvertFromUtc(_user.Preference.TimeZone).ToString(cults.DateTimeFormat.ShortDatePattern + " " + cults.DateTimeFormat.ShortTimePattern) : string.Empty;
                    else
                        _formattedData = (((DateTime)_unformattedData).Date != DateTime.MinValue) ? Convert.ToDateTime(_unformattedData).ToString(cults.DateTimeFormat.ShortDatePattern + " " + cults.DateTimeFormat.ShortTimePattern) : string.Empty;
                    if (col.Data < row.Count)
                        row[col.Data] = Convert.ToDateTime(_unformattedData).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                }
                else if ((col as DVDateTimeColumn).Format == DateFormat.Time)
                {
                    DateTime dt;
                    if (col.RenderType == EbDbTypes.Time)
                    {
                        if (!DateTime.MinValue.Equals(_unformattedData))
                            dt = DateTime.MinValue + (TimeSpan)_unformattedData;
                        else
                            dt = Convert.ToDateTime(_unformattedData);
                    }
                    else
                        dt = Convert.ToDateTime(_unformattedData);
                    if (!DateTime.MinValue.Equals(dt) && dt.TimeOfDay != TimeSpan.Zero)
                    {
                        if ((col as DVDateTimeColumn).ConvretToUsersTimeZone)
                            _formattedData = dt.ConvertFromUtc(_user.Preference.TimeZone).ToString(cults.DateTimeFormat.ShortTimePattern);
                        else
                            _formattedData = dt.ToString(cults.DateTimeFormat.ShortTimePattern);
                        if (col.Data < row.Count)
                            row[col.Data] = dt.ToString("HH:mm:ss", CultureInfo.InvariantCulture);
                    }
                    else
                        _formattedData = string.Empty;
                }
            }
            catch (Exception e)
            {
                Log.Info("DateTime Conversion in datatable Exception........." + e.StackTrace + "Column Name....." + col.Name);
                Log.Info("DateTime Conversion in datatable Exception........." + e.Message + "Column Name....." + col.Name);
                this._Responsestatus.Message = e.Message;
            }
        }

        public void conditinallyformatColumn(DVBaseColumn col, ref object _formattedData, object _unformattedData, EbDataRow row, ref Globals globals)
        {
            try
            {
                foreach (ColumnCondition cond in col.ConditionalFormating)
                {
                    if (cond is AdvancedCondition)
                    {
                        bool result = (cond as AdvancedCondition).EvaluateExpression(row, ref globals);
                        if ((cond as AdvancedCondition).RenderAS == AdvancedRenderType.Default)
                        {
                            if (result == (cond as AdvancedCondition).GetBoolValue())
                                _formattedData = "<div class='conditionformat' style='background-color:" + cond.BackGroundColor + ";color:" + cond.FontColor + ";'>" + _formattedData + "</div>";
                        }
                        else
                        {
                            if (result == (cond as AdvancedCondition).GetBoolValue())
                                _formattedData = "<i class='fa fa-check' aria-hidden='true'  style='color:green'></i>";
                            else
                                _formattedData = "<i class='fa fa-times' aria-hidden='true' style='color:red'></i>";
                        }
                    }
                    if (cond.CompareValues(_unformattedData))
                    {
                        _formattedData = "<div class='conditionformat' style='background-color:" + cond.BackGroundColor + ";color:" + cond.FontColor + ";'>" + _formattedData + "</div>";
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info("Condition Formatting in datatable Exception........." + e.StackTrace + "Column Name....." + col.Name);
                Log.Info("Condition Formatting in datatable Exception........." + e.Message + "Column Name....." + col.Name);
                this._Responsestatus.Message = e.Message;
            }

        }

        public void conditinallyformatColumnforElse(DVBaseColumn col, ref object _formattedData)
        {
            try
            {
                foreach (ColumnCondition cond in col.ConditionalFormating)
                {
                    if (cond is AdvancedCondition)
                    {
                        if ((cond as AdvancedCondition).RenderAS == AdvancedRenderType.Icon)
                        {
                            _formattedData = "<i class='fa fa-times' aria-hidden='true' style='color:red'></i>";
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info("Condition Formatting in datatable Exception........." + e.StackTrace + "Column Name....." + col.Name);
                Log.Info("Condition Formatting in datatable Exception........." + e.Message + "Column Name....." + col.Name);
                this._Responsestatus.Message = e.Message;
            }

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

        public void DoRowGroupingCommon(EbDataRow currentRow, EbDataVisualization Visualization, CultureInfo Culture, User _user,
            ref EbDataTable FormattedTable, bool IsMultiLevelRowGrouping, ref Dictionary<string, GroupingDetails> RowGrouping,
            ref string PreviousGroupingText, ref int CurSortIndex, ref int SerialCount, int PrevRowIndex, int dvColCount, int TotalLevels,
            ref List<int> AggregateColumnIndexes, ref List<DVBaseColumn> RowGroupingColumns, int RowCount)
        {
            CurSortIndex += TotalLevels + 30;

            int delimCount = 1;
            string TempGroupingText = CreateCollectionKey(currentRow, IsMultiLevelRowGrouping, BlankText, TotalLevels, RowGroupingColumns, PrevRowIndex, ref delimCount, Culture, _user);

            if (TempGroupingText.Equals(PreviousGroupingText) == false)
            {
                SerialCount = 0;
                FormattedTable.Rows[PrevRowIndex][dvColCount] = ++SerialCount;
                CreateHeaderAndFooterPairs(currentRow, AggregateColumnIndexes, RowGroupingColumns, RowGrouping, Visualization.Columns, TotalLevels, IsMultiLevelRowGrouping, Culture, TempGroupingText, ref CurSortIndex, dvColCount, _user, Visualization.AutoGen);

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
                if (PrevRowIndex == RowCount - 1)
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

        private void SetFinalFooterRow(EbDataRow currentRow, List<DVBaseColumn> rowGroupingColumns, bool IsMultiLevelRowGrouping, Dictionary<string, GroupingDetails> RowGrouping, int i, string TempGroupingText, int CurSortIndex, CultureInfo _user_culture, User _user)
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

        public List<int> GetAggregateIndexes(DVColumnCollection VisualizationColumns)
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
                string tempValue = IntermediateDic[Column.Data].ToString().Trim();
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
            int TotalLevels, bool IsMultiLevelGrouping, CultureInfo culture, string TempGroupingText, ref int CurSortIndex, int ColumnCount, User _user, bool IsAutoGendv)
        {
            List<string> TempKey = CreateRowGroupingKeys(CurrentRow, _rowGroupingColumns, (TotalLevels > 1) ? true : false, culture, _user);
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
                        rowGrouping[headerKey].IsAutoGen = IsAutoGendv;
                        rowGrouping[footerKey].IsAutoGen = IsAutoGendv;
                        rowGrouping[footerKey].ShowCheckbox = this.showCheckboxColumn;
                        rowGrouping[headerKey].ShowCheckbox = this.showCheckboxColumn;
                        rowGrouping[headerKey].TableId = this.TableId;
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
                    rowGrouping[headerKey].IsAutoGen = IsAutoGendv;
                    rowGrouping[footerKey].IsAutoGen = IsAutoGendv;
                    rowGrouping[footerKey].ShowCheckbox = this.showCheckboxColumn;
                    rowGrouping[headerKey].ShowCheckbox = this.showCheckboxColumn;
                    rowGrouping[headerKey].TableId = this.TableId;
                }
            }
        }

        private List<string> CreateRowGroupingKeys(EbDataRow CurrentRow, List<DVBaseColumn> RowGroupingColumns, bool IsMultiLevelRowGrouping, CultureInfo _user_culture, User _user)
        {
            List<string> TempKey = new List<string>();
            string TempStr = string.Empty;
            foreach (DVBaseColumn column in RowGroupingColumns)
            {
                string tempvalue = IntermediateDic[column.Data].ToString().Trim();
                if (IsMultiLevelRowGrouping)
                {
                    TempKey.Add(((TempKey.Count > 0) ? TempKey.Last() + GroupDelimiter : string.Empty) + ((tempvalue.IsNullOrEmpty()) ? BlankText : tempvalue));
                }
                else
                {
                    TempStr += (TempStr.Equals(string.Empty)) ? ((tempvalue.IsNullOrEmpty()) ? BlankText : tempvalue) : GroupDelimiter + ((tempvalue.IsNullOrEmpty()) ? BlankText : tempvalue);
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
            this.TableId = request.TableId;
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
            var parameters = DataHelper.GetParams(this.EbConnectionFactory.ObjectsDB, _isPaged, request.Params, request.Length, request.Start);
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
            // EbDataTable _formattedDataTable = null;
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
            FileInfo file = new FileInfo(System.IO.Path.Combine(sFileName));
            return file;
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

    public class DynamicObj
    {
        public string Row;
        public object Value;
    }

    public class HourCountDictionary : Dictionary<string, DynamicObj>
    {
        public new void Clear()
        {
            foreach (string key in this.Keys)
            {
                this[key] = new DynamicObj();
            }
        }
    }

    public class ApprovalData
    {
        public string Action_unique_id;
        public string Stage_unique_id;
        public string My_action_id;
        public string Form_data_id;
        public string Form_ref_id;
    }
}
