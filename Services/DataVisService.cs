using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [DefaultView("ds")]
    [Authenticate]
    public class DataVisService : EbBaseService
    {
        public DataVisService(ITenantDbFactory _dbf) : base(_dbf) { }
        
        [CompressResponse]
        public DataSourceDataResponse Any(DataVisDataRequest request)
        {
            this.Log.Info("data request");

            //var dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format("SELECT obj_bytea FROM eb_objects_ver WHERE id={0}", request.Id));
            //var dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format(@"
            //    SELECT EOV.obj_json FROM eb_objects_ver EOV 
            //    INNER JOIN eb_objects EO
            //    ON EO.id = EOV.eb_objects_id  AND EO.obj_last_ver_id =EOV.ver_num AND EOV.eb_objects_id={0}", request.RefId)
            //   );

            //var dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format(@"
            //    SELECT EOV.obj_bytea FROM eb_objects_ver EOV 
            //    INNER JOIN eb_objects EO
            //    ON EO.id = EOV.eb_objects_id  AND EOV.ver_num={0} AND EOV.eb_objects_id={1}", request.VersionId, request.Id)
            //    );

            //this.Log.Info("dt.Rows.Count *****" + dt.Rows.Count);
            //this.Log.Info("ProtoBuf_DeSerialize *****" + EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]));
            DataSourceDataResponse dsresponse = null;

            //if (dt.Rows.Count > 0)
            //{
                var _ds = this.Redis.Get<EbDataSource>(request.RefId); //EbSerializers.Json_Deserialize<EbDataSource>(dt.Rows[0][0].ToString());
                this.Log.Info("_ds *****" + _ds.SqlDecoded());
                string _sql = string.Empty;

                if (_ds != null)
                {
                    string _c = string.Empty;

                    if (request.TFilters != null)
                    {
                        foreach (Dictionary<string, string> _dic in request.TFilters)
                        {
                            var op = _dic["o"]; var col = _dic["c"]; var val = _dic["v"];

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

                    _sql = _ds.SqlDecoded().Replace("@and_search", _c);
                }
                this.Log.Info("search ok");
                _sql = _sql.Replace("@orderby",
                    (string.IsNullOrEmpty(request.OrderByCol)) ? "id" : string.Format("{0} {1}", request.OrderByCol, ((request.OrderByDir == 2) ? "DESC" : "ASC")));
                this.Log.Info("order ok");
                var parameters = new List<System.Data.Common.DbParameter>();
                parameters.AddRange(new System.Data.Common.DbParameter[]
                {
                    this.TenantDbFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, request.Length),
                    this.TenantDbFactory.ObjectsDB.GetNewParameter("@offset", System.Data.DbType.Int32, request.Start),
                });

                if (request.Params != null)
                {
                    foreach (Dictionary<string, string> param in request.Params)
                        parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param["name"]), (System.Data.DbType)Convert.ToInt32(param["type"]), param["value"]));
                }
                this.Log.Info("GO**********************" + _sql);
                var _dataset = this.TenantDbFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray());
                this.Log.Info(">>>>>> _dataset.Tables.Count: " + _dataset.Tables.Count + ", " + _dataset.ToJson());

                //-- 
                bool _isPaged = (_sql.ToLower().Contains("@offset") && _sql.ToLower().Contains("@limit"));
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
            //}

            //return this.Request.ToOptimizedResult<DataSourceDataResponse>(dsresponse);
            return dsresponse;
        }

      
        [CompressResponse]
        public DataSourceColumnsResponse Any(DataVisColumnsRequest request)
        {
            string _dsRedisKey = string.Format("{0}_columns", request.RefId);
            EbDataSet _dataset = null;
            bool _isPaged = false;
            DataSourceColumnsResponse resp = this.Redis.Get<DataSourceColumnsResponse>(_dsRedisKey);

            if (resp == null || resp.Columns == null || resp.Columns.Count == 0)
            {
                resp = new DataSourceColumnsResponse();
                resp.Columns = new List<ColumnColletion>();

                var _ds = this.Redis.Get<EbDataSource>(request.RefId); // EbSerializers.Json_Deserialize<EbDataSource>(dt.Rows[0][0].ToString());
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
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, 0),
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("@offset", System.Data.DbType.Int32, 0)
                        });
                    }

                    if (request.Params != null)
                    {
                        foreach (Dictionary<string, string> param in request.Params)
                            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param["name"]), (System.Data.DbType)Convert.ToInt32(param["type"]), param["value"]));
                    }

                    Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns Parameters Added");

                    try
                    {
                        _dataset = this.TenantDbFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray());

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
    }
}

