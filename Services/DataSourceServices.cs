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
        public object Any(DataSourceDataRequest request)
        {
            ILog log = LogManager.GetLogger(GetType());
            log.Info("data request");
            base.ClientID = request.TenantAccountId;

            request.SearchText = base.Request.QueryString["searchtext"];
            //request.SearchTextcollection = string.IsNullOrEmpty(request.SearchText) ? "" : request.SearchText; // @txtsearch
            request.OrderByDirection = base.Request.QueryString["order[0][dir]"]; //@order_dir
            request.SearchColumnName = base.Request.QueryString["search_col"]; // @search_col
            request.OrderColumnName = base.Request.QueryString["order_col"]; // @order_col

            List<string> searchColumn = new List<string>();
            List<string> searchValue = new List<string>();
            List<string> operatorValue = new List<string>();

            if (!string.IsNullOrEmpty(request.SearchColumnName))
                searchColumn = new List<string>(request.SearchColumnName.Split(','));
            if (!string.IsNullOrEmpty(request.SearchText))
                searchValue = new List<string>(request.SearchText.Split(','));
            if (!string.IsNullOrEmpty(base.Request.QueryString["selectedvalue"]))
                operatorValue = new List<string>(base.Request.QueryString["selectedvalue"].Split(','));

            var dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format("SELECT obj_bytea FROM eb_objects WHERE id={0}", request.Id));

            DataSourceDataResponse dsresponse = null;

            if (dt.Rows.Count > 0)
            {
                var _ds = EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]);

                if (_ds != null)
                {
                    // NEED TO REWORK
                    string _sql = string.Empty;
                    if (searchColumn.Count == 0)
                        _sql = _ds.Sql.Replace("@and_searchplaceholder", string.Empty);
                    else
                    {
                        string _c = string.Empty;
                        for (int j = 0; j < searchColumn.Count; j++)
                        {

                            if (operatorValue[j] == "x*")
                                _c += string.Format("AND LOWER({0})::text LIKE LOWER('{1}%') ", searchColumn[j], searchValue[j]);
                            else if (operatorValue[j] == "*x")
                                _c += string.Format("AND LOWER({0})::text LIKE LOWER('%{1}') ", searchColumn[j], searchValue[j]);
                            else if (operatorValue[j] == "*x*")
                                _c += string.Format("AND LOWER({0})::text LIKE LOWER('%{1}%') ", searchColumn[j], searchValue[j]);
                            else if (operatorValue[j] == "=")
                                _c += string.Format("AND LOWER({0})::text LIKE LOWER('{1}') ", searchColumn[j], searchValue[j]);
                            else
                                _c += string.Format("AND {0} {1} '{2}' ", searchColumn[j], operatorValue[j], searchValue[j]);
                        }
                        _sql = _ds.Sql.Replace("@and_searchplaceholder", _c);
                    }
                    _sql = _sql.Replace("@orderbyplaceholder",
                     (string.IsNullOrEmpty(request.OrderColumnName)) ? "id" : string.Format("{0} {1}", request.OrderColumnName, request.OrderByDirection));

                    var parameters = new List<System.Data.Common.DbParameter>();
                    {
                        parameters.AddRange(new System.Data.Common.DbParameter[]
                        {
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, request.Length),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@last_id", System.Data.DbType.Int32, request.Start+1),
                        });

                        if (request.Params != null) {
                            foreach (EbParam param in request.Params)
                                parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param.Name), System.Data.DbType.Date, Convert.ToDateTime(param.Value)));
                        }
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
            }

            return dsresponse;
        }

        public object Any(DataSourceColumnsRequest request)
        {
            ILog log = LogManager.GetLogger(GetType());
            log.Info("column request");
            log.Info(request.Params);
            base.ClientID = request.TenantAccountId;

            ColumnColletion columns = base.SessionBag.Get<ColumnColletion>(string.Format("ds_{0}_columns", request.Id));
            //if (columns == null)
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
                        {
                            parameters.AddRange(new System.Data.Common.DbParameter[]
                            {
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, 0),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@last_id", System.Data.DbType.Int32, 0),
                            });

                            if (request.Params != null)
                            {
                                foreach (EbParam param in request.Params)
                                    parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", param.Name), System.Data.DbType.Date, Convert.ToDateTime(param.Value)));
                            }
                        }

                        _sql = (_sql.IndexOf(";") > 0) ? _sql.Substring(_sql.IndexOf(";") + 1) : _sql;
                        var dt2 = this.DatabaseFactory.ObjectsDB.DoQuery(_sql, parameters.ToArray());
                        columns = dt2.Columns;
                        log.Info(columns);
                        base.SessionBag.Set<ColumnColletion>(string.Format("ds_{0}_columns", request.Id), dt2.Columns);
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

