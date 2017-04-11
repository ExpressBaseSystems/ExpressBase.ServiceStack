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
        public object Post(DataSourceDataRequest request)
        {
            this.Log.Info("data request");
            base.ClientID = request.TenantAccountId;

            var dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format("SELECT obj_bytea FROM eb_objects WHERE id={0}", request.Id));

            DataSourceDataResponse dsresponse = null;

            if (dt.Rows.Count > 0)
            {
                var _ds = EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]);

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

                    _sql = _ds.Sql.Replace("@and_searchplaceholder", _c);
                }

                _sql = _sql.Replace("@orderbyplaceholder",
                    (string.IsNullOrEmpty(request.OrderByCol)) ? "id" : string.Format("{0} {1}", request.OrderByCol, ((request.OrderByDir == 2) ? "DESC" : "ASC")));
                
                var parameters = new List<System.Data.Common.DbParameter>();
                parameters.AddRange(new System.Data.Common.DbParameter[]
                {
                    this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, request.Length),
                    this.DatabaseFactory.ObjectsDB.GetNewParameter("@last_id", System.Data.DbType.Int32, request.Start),
                });

                if (request.Params != null) {
                    foreach (Dictionary<string, string> param in request.Params)
                        parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param["name"]), (System.Data.DbType)Convert.ToInt32(param["type"]), param["value"]));
                }

                var _dataset = (request.Length > 0) ? this.DatabaseFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray()) : this.DatabaseFactory.ObjectsDB.DoQueries(_sql);

                dsresponse = new DataSourceDataResponse
                {
                    Draw = request.Draw,
                    Data = (request.Length > 0) ? _dataset.Tables[1].Rows : _dataset.Tables[0].Rows,
                    RecordsTotal = (request.Length > 0) ? Convert.ToInt32(_dataset.Tables[0].Rows[0][0]) : _dataset.Tables[0].Rows.Count,
                    RecordsFiltered = (request.Length > 0) ? Convert.ToInt32(_dataset.Tables[0].Rows[0][0]) : _dataset.Tables[0].Rows.Count
                };
            }

            return dsresponse;
        }

        public object Any(DataSourceColumnsRequest request)
        {
            base.ClientID = request.TenantAccountId;

            //ColumnColletion columns = this.Redis.Get<ColumnColletion>(string.Format("{0}_ds_{1}_columns", request.TenantAccountId, request.Id));
            ColumnColletion columns = null;
            if (columns == null)
            {
                request.SearchText = base.Request.QueryString["searchtext"];
                request.SearchText = string.IsNullOrEmpty(request.SearchText) ? "" : request.SearchText; // @txtsearch
                request.OrderByDirection = base.Request.QueryString["order[0][dir]"]; //@order_dir
                request.SelectedColumnName = base.Request.QueryString["col"]; // @selcol

                string _sql = string.Format("SELECT obj_bytea FROM eb_objects WHERE id={0}", request.Id);

                var dt = this.DatabaseFactory.ObjectsDB.DoQuery(_sql);

                if (dt.Rows.Count > 0)
                {
                    var _ds = EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]);
                    if (_ds != null)
                    {
                        _sql = _ds.Sql.Replace("@and_searchplaceholder",
                            (string.IsNullOrEmpty(request.SearchText)) ? string.Empty : string.Format("AND {0}::text LIKE '%{1}%'", request.SelectedColumnName, request.SearchText));

                        _sql = _sql.Replace("@orderbyplaceholder",
                        (string.IsNullOrEmpty(request.SelectedColumnName)) ? "id" : string.Format("{0} {1}", request.SelectedColumnName, request.OrderByDirection));
                        
                        var parameters = new List<System.Data.Common.DbParameter>();
                        parameters.AddRange(new System.Data.Common.DbParameter[]
                        {
                            this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, 0),
                            this.DatabaseFactory.ObjectsDB.GetNewParameter("@last_id", System.Data.DbType.Int32, 0),
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
                            var dt2 = this.DatabaseFactory.ObjectsDB.DoQuery(_sql, parameters.ToArray());
                            columns = dt2.Columns;
                            Log.Info(columns);
                            this.Redis.Set<ColumnColletion>(string.Format("{0}_ds_{1}_columns", request.TenantAccountId, request.Id), columns);
                        }
                        catch (Exception e)
                        {
                            Log.Info("e.Message" + e.Message);
                        }
                    }
                }
            }

            return new DataSourceColumnsResponse
            {
                Columns = columns
            };
        }
    }
}

