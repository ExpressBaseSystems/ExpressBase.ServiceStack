using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Logging;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Text;

namespace ExpressBase.ServiceStack
{
    [Authenticate]
    public class DataVisService : EbBaseService
    {
        public DataVisService(IEbConnectionFactory _dbf) : base(_dbf) { }
        
        [CompressResponse]
        public DataSourceDataResponse Any(DataVisDataRequest request)
        {
            this.Log.Info("data request");

            DataSourceDataResponse dsresponse = null;

            EbDataVisualization _dV = request.EbDataVisualization;

            //if (request.WhichConsole == "uc")
            //    _dVSet = this.Redis.Get<EbDataVisualizationSet>(request.RefId + request.UserId.ToString());
            //else //dc
            //    _dVSet = this.Redis.Get<EbDataVisualizationSet>(request.RefId);

            _dV.AfterRedisGet(this.Redis as RedisClient);

            string _sql = null;

            if (_dV.EbDataSource != null)
            {
                StringBuilder _sb = new StringBuilder();

                if (request.TFilters != null)
                {
                    foreach (Dictionary<string, string> _dic in request.TFilters)
                    {
                        var op = _dic["o"]; var col = _dic["c"]; var val = _dic["v"];

                        if (op == "x*")
                            _sb.Append(string.Format("LOWER({0})::text LIKE LOWER('{1}%') ", col, val));
                        else if (op == "*x")
                            _sb.Append(string.Format("LOWER({0})::text LIKE LOWER('%{1}') ", col, val));
                        else if (op == "*x*")
                            _sb.Append(string.Format("LOWER({0})::text LIKE LOWER('%{1}%') ", col, val));
                        else if (op == "=")
                            _sb.Append(string.Format("LOWER({0}::text) = LOWER('{1}') ", col, val));
                        else
                            _sb.Append(string.Format("{0} {1} '{2}' ", col, op, val));
                    }
                }

                string __innerSql = _dV.EbDataSource.SqlDecoded();
                string _where = (_sb.Length > 0) ? "WHERE " + string.Join(" AND ", _sb) : string.Empty;
                string _orderby = (string.IsNullOrEmpty(request.OrderByCol)) ? "1" : string.Format("{0} {1}", request.OrderByCol, ((request.OrderByDir == 2) ? "DESC" : "ASC"));
                _sql = string.Format("WITH __OUTER99 AS ({0}) SELECT * FROM __OUTER99 {1} {2}", __innerSql, _where, _orderby);

                this.Log.Info("_ds *****" + _sql);
            }

            var parameters = new List<System.Data.Common.DbParameter>();
            bool _isPaged = (_sql.ToLower().Contains("@offset") && _sql.ToLower().Contains("@limit"));
            if (_isPaged)
            {
                parameters.AddRange(new System.Data.Common.DbParameter[]
                {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("@limit", EbDbTypes.Int32, request.Length),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("@offset", EbDbTypes.Int32, request.Start),
                });
            }

            if (request.Params != null)
            {
                foreach (Dictionary<string, string> param in request.Params)
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param["name"]), (EbDbType)Convert.ToInt32(param["type"]), param["value"]));
            }

            var _dataset = _dV.DoQueries4DataVis(_sql, this.EbConnectionFactory, parameters.ToArray());

            //-- 
            int _recordsTotal = 0, _recordsFiltered = 0;
            if (_isPaged)
            {
                Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsTotal);
                Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsFiltered);
            }
            _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[0].Rows.Count;
            _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[0].Rows.Count;
            //-- 

            dsresponse = new DataSourceDataResponse
            {
                Draw = request.Draw,
                Data = (_dataset.Tables.Count > 1) ? _dataset.Tables[1].Rows : _dataset.Tables[0].Rows,
                RecordsTotal = _recordsTotal,
                RecordsFiltered = _recordsFiltered
            };
            this.Log.Info("dsresponse*****" + dsresponse.Data);

            return dsresponse;
        }
      
        [CompressResponse]
        public DataSourceColumnsResponse Any(DataVisColumnsRequest request)
        {
            EbDataVisualization _dV = request.EbDataVisualization;
            _dV.AfterRedisGet(this.Redis as RedisClient);
            var _ds = _dV.EbDataSource;

            string _dsRedisKey = string.Format("{0}_columns", _dV.DataSourceRefId);

            EbDataSet _dataset = null;
            bool _isPaged = false;
            DataSourceColumnsResponse resp = this.Redis.Get<DataSourceColumnsResponse>(_dsRedisKey);

            if (resp == null || resp.Columns == null || resp.Columns.Count == 0)
            {
                resp = new DataSourceColumnsResponse();
                resp.Columns = new List<ColumnColletion>();

                if (_ds != null)
                {
                    Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns Sql: " + _ds.SqlDecoded());

                    string _sql = _ds.SqlDecoded().Replace("@and_search", string.Empty).Replace("@orderby", "1");
                    _isPaged = (_sql.ToLower().Contains("@offset") && _sql.ToLower().Contains("@limit"));

                    var parameters = new List<System.Data.Common.DbParameter>();
                    if (_isPaged)
                    {
                        parameters.AddRange(new System.Data.Common.DbParameter[]
                        {
                            this.EbConnectionFactory.ObjectsDB.GetNewParameter("@limit", EbDbTypes.Int32, 0),
                            this.EbConnectionFactory.ObjectsDB.GetNewParameter("@offset", EbDbTypes.Int32, 0)
                        });
                    }

                    if (request.Params != null)
                    {
                        foreach (Dictionary<string, string> param in request.Params)
                            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param["name"]), (EbDbType)Convert.ToInt32(param["type"]), param["value"]));
                    }

                    Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns Parameters Added");

                    try
                    {
                        _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray());

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
                    if (element.EbObjectType.Equals(EbObjectTypes.TableVisualization) || element.EbObjectType.Equals(EbObjectTypes.ChartVisualization))
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
                    if (element.EbObjectType.Equals(EbObjectTypes.TableVisualization) || element.EbObjectType.Equals(EbObjectTypes.ChartVisualization))
                    {
                        dvTaggedList.Add(element);
                    }
                }

            }

            return new EbObjectWithRelatedDVResponse { Dsobj = dsobj, DvList = dvList, DvTaggedList = dvTaggedList };
        }
    }
}

