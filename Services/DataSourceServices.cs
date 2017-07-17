using ExpressBase.Common;
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
    public class DataSourceService : EbBaseService
    {
        [Authenticate]
        [CompressResponse]
        public DataSourceDataResponse Post(DataSourceDataRequest request)
        {
            this.Log.Info("data request");
            base.ClientID = request.TenantAccountId;

            //var dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format("SELECT obj_bytea FROM eb_objects_ver WHERE id={0}", request.Id));
            var dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format(@"
                SELECT EOV.obj_bytea FROM eb_objects_ver EOV 
                INNER JOIN eb_objects EO
                ON EO.id = EOV.eb_objects_id  AND EO.obj_last_ver_id =EOV.ver_num AND EOV.eb_objects_id={0}", request.Id)
               );

            //var dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format(@"
            //    SELECT EOV.obj_bytea FROM eb_objects_ver EOV 
            //    INNER JOIN eb_objects EO
            //    ON EO.id = EOV.eb_objects_id  AND EOV.ver_num={0} AND EOV.eb_objects_id={1}", request.VersionId, request.Id)
            //    );

            this.Log.Info("dt.Rows.Count *****" + dt.Rows.Count);
            //this.Log.Info("ProtoBuf_DeSerialize *****" + EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]));
            DataSourceDataResponse dsresponse = null;

            if (dt.Rows.Count > 0)
            {
                var _ds = EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]);
                this.Log.Info("_ds *****" + _ds.Sql);
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

                    _sql = _ds.Sql.Replace("@and_search", _c);
                }
                this.Log.Info("search ok");
                _sql = _sql.Replace("@orderby",
                    (string.IsNullOrEmpty(request.OrderByCol)) ? "id" : string.Format("{0} {1}", request.OrderByCol, ((request.OrderByDir == 2) ? "DESC" : "ASC")));
                this.Log.Info("order ok");
                var parameters = new List<System.Data.Common.DbParameter>();
                parameters.AddRange(new System.Data.Common.DbParameter[]
                {
                    this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, request.Length),
                    this.DatabaseFactory.ObjectsDB.GetNewParameter("@offset", System.Data.DbType.Int32, request.Start),
                });

                if (request.Params != null)
                {
                    foreach (Dictionary<string, string> param in request.Params)
                        parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param["name"]), (System.Data.DbType)Convert.ToInt32(param["type"]), param["value"]));
                }
                this.Log.Info("GO**********************" + _sql);
                var _dataset = (request.Length > 0) ? this.DatabaseFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray()) : this.DatabaseFactory.ObjectsDB.DoQueries(_sql);
                this.Log.Info(">>>>>> _dataset.Tables.Count: " + _dataset.Tables.Count);
                dsresponse = new DataSourceDataResponse
                {
                    Draw = request.Draw,
                    Data = (_dataset.Tables.Count > 1) ? _dataset.Tables[1].Rows : _dataset.Tables[0].Rows,
                    RecordsTotal = (request.Length > 0) ? Convert.ToInt32(_dataset.Tables[0].Rows[0][0]) : _dataset.Tables[0].Rows.Count,
                    RecordsFiltered = (request.Length > 0) ? Convert.ToInt32(_dataset.Tables[0].Rows[0][0]) : _dataset.Tables[0].Rows.Count
                };
                this.Log.Info("dsresponse*****" + dsresponse.Data);
            }

            //return this.Request.ToOptimizedResult<DataSourceDataResponse>(dsresponse);
            return dsresponse;
        }

        [Authenticate]
        [CompressResponse]
        public DataSourceColumnsResponse Any(DataSourceColumnsRequest request)
        {
            base.ClientID = request.TenantAccountId;
            ColumnColletion columns = null;
            //ColumnColletion columns = this.Redis.Get<ColumnColletion>(string.Format("{0}_ds_{1}_columns", request.TenantAccountId, request.Id));
            //if (columns == null || columns.Count == 0)
            //{
            request.SearchText = base.Request.QueryString["searchtext"];
            request.SearchText = string.IsNullOrEmpty(request.SearchText) ? "" : request.SearchText; // @txtsearch
            request.OrderByDirection = base.Request.QueryString["order[0][dir]"]; //@order_dir
            request.SelectedColumnName = base.Request.QueryString["col"]; // @selcol

            //string _sql = string.Format("SELECT obj_bytea FROM eb_objects_ver WHERE id={0}", request.Id);
            string _sql = string.Format(@"
SELECT 
    EOV.obj_bytea 
FROM 
    eb_objects_ver EOV, eb_objects EO 
WHERE
    EO.id = EOV.eb_objects_id AND 
    EO.obj_last_ver_id = EOV.ver_num AND 
    EO.id = {0}", request.Id);

            var dt = this.DatabaseFactory.ObjectsDB.DoQuery(_sql);
            Log.Info("dt.Rows.Count********" + dt.Rows.Count);
            if (dt.Rows.Count > 0)
            {
                var _ds = EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]);
                Log.Info("sql********" + _ds.Sql);
                if (_ds != null)
                {
                    _sql = _ds.Sql.Replace("@and_search",
                        (string.IsNullOrEmpty(request.SearchText)) ? string.Empty : string.Format("AND {0}::text LIKE '%{1}%'", request.SelectedColumnName, request.SearchText));

                    _sql = _sql.Replace("@orderby",
                    (string.IsNullOrEmpty(request.SelectedColumnName)) ? "id" : string.Format("{0} {1}", request.SelectedColumnName, request.OrderByDirection));

                    var parameters = new List<System.Data.Common.DbParameter>();
                    parameters.AddRange(new System.Data.Common.DbParameter[]
                    {
                            this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, 0),
                            this.DatabaseFactory.ObjectsDB.GetNewParameter("@offset", System.Data.DbType.Int32, 0),
                    });

                    if (request.Params != null)
                    {
                        foreach (Dictionary<string, string> param in request.Params)
                            parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param["name"]), (System.Data.DbType)Convert.ToInt32(param["type"]), param["value"]));
                    }

                    Log.Info("reached Here....");
                    _sql = (_sql.IndexOf(";") > 0) ? _sql.Substring(_sql.IndexOf(";") + 1) : _sql;
                    try
                    {
                        Log.Info("_sql: " + _sql);
                        var dt2 = this.DatabaseFactory.ObjectsDB.DoQuery(_sql, parameters.ToArray());
                        columns = dt2.Columns;
                        Log.Info(columns);
                        this.Redis.Set<ColumnColletion>(string.Format("{0}_ds_{1}_columns", request.TenantAccountId, request.Id), columns);
                    }
                    catch (Exception e)
                    {
                        Log.Info("e.Message" + e.Message);
                        columns = this.Redis.Get<ColumnColletion>(string.Format("{0}_ds_{1}_columns", request.TenantAccountId, request.Id));
                        if (columns != null || columns.Count > 0)
                            this.Redis.Remove(string.Format("{0}_ds_{1}_columns", request.TenantAccountId, request.Id));
                    }
                }
            }
            // }

            return new DataSourceColumnsResponse
            {
                Columns = columns
            };
        }
    }
}

