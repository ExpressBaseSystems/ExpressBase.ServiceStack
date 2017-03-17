using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.Text;
using System.Runtime.Serialization;
using ExpressBase.Common;
using ExpressBase.Data;
using System;
using ExpressBase.Objects;
using System.Collections.Generic;
using ExpressBase.ServiceStack.Services;

namespace ExpressBase.ServiceStack
{
    [Route("/ds")]
    [Route("/ds/data/{Id}")]
    public class DataSourceDataRequest : IReturn<DataSourceDataResponse>
    {
        public int Id { get; set; }

        public int Start { get; set; }

        public int Length { get; set; }

        public int Draw { get; set; }

        public string SearchText { get; set; }

        public string OrderByDirection { get; set; }

        public string OrderColumnName { get; set; }

        public string SearchColumnName { get; set; }

        public Dictionary<string, string> colvalues { get; set; }
    }

    [Route("/ds")]
    [Route("/ds/columns/{Id}")]
    public class DataSourceColumnsRequest : IReturn<DataSourceColumnsResponse>
    {
        public int Id { get; set; }

        public string SearchText { get; set; }

        public string OrderByDirection { get; set; }

        public string SelectedColumnName { get; set; }

        public Dictionary<string,string> colvalues { get; set; }
}

    [DataContract]
    [Csv(CsvBehavior.FirstEnumerable)]
    public class DataSourceDataResponse
    {
        [DataMember(Order = 1)]
        public int Draw { get; set; }

        [DataMember(Order = 2)]
        public int RecordsTotal { get; set; }

        [DataMember(Order = 3)]
        public int RecordsFiltered { get; set; }

        [DataMember(Order = 4)]
        public RowColletion Data { get; set; }
    }

    [DataContract]
    [Csv(CsvBehavior.FirstEnumerable)]
    public class DataSourceColumnsResponse
    {
        [DataMember(Order = 1)]
        public ColumnColletion Columns { get; set; }
    }

    [ClientCanSwapTemplates]
    [DefaultView("ds")]
    public class DataSourceService : EbBaseService
    {
        public object Get(DataSourceDataRequest request)
        {
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

            var datefrom = string.Empty;
            var dateto = string.Empty;
            if (!string.IsNullOrEmpty(request.colvalues["from"]) && !string.IsNullOrEmpty(request.colvalues["to"]))
            {
                datefrom = request.colvalues["from"];
                dateto = request.colvalues["to"];
            }


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

                    var parameters = new System.Data.Common.DbParameter[0];
                    if (!string.IsNullOrEmpty(datefrom) && !string.IsNullOrEmpty(dateto))
                    {
                        parameters = new System.Data.Common.DbParameter[4]
                        {
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, request.Length),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@last_id", System.Data.DbType.Int32, request.Start+1),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@date1", System.Data.DbType.DateTime, Convert.ToDateTime(datefrom)),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@date2", System.Data.DbType.DateTime, Convert.ToDateTime(dateto))
                        };
                    }
                    else
                    {
                        parameters = new System.Data.Common.DbParameter[2]
                            {
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32,request.Length),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@last_id", System.Data.DbType.Int32, request.Start+1),
                            };
                    }

                    var _dataset = (request.Length > 0) ? this.DatabaseFactory.ObjectsDB.DoQueries(_sql, parameters) : this.DatabaseFactory.ObjectsDB.DoQueries(_sql);

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

        public object Get(DataSourceColumnsRequest request)
        {
            ColumnColletion columns = base.SessionBag.Get<ColumnColletion>(string.Format("ds_{0}_columns", request.Id));
            //if (columns == null)
            {
                request.SearchText = base.Request.QueryString["searchtext"];
                request.SearchText = string.IsNullOrEmpty(request.SearchText) ? "" : request.SearchText; // @txtsearch
                request.OrderByDirection = base.Request.QueryString["order[0][dir]"]; //@order_dir
                request.SelectedColumnName = base.Request.QueryString["col"]; // @selcol

                var datefrom = string.Empty;
                var dateto = string.Empty;
                if (!request.colvalues.IsNullOrEmpty())
                {
                    datefrom = request.colvalues["from"];
                    dateto = request.colvalues["to"];
                }

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
                        System.Data.Common.DbParameter[] parameters = null;
                        if (!string.IsNullOrEmpty(datefrom) && !string.IsNullOrEmpty(dateto))
                        {
                            parameters = new System.Data.Common.DbParameter[4]
                            {
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, 0),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@last_id", System.Data.DbType.Int32, 0),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@date1", System.Data.DbType.DateTime,Convert.ToDateTime(datefrom)),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@date2", System.Data.DbType.DateTime, Convert.ToDateTime(dateto))
                            };
                        }
                        else
                        {
                            parameters = new System.Data.Common.DbParameter[2]
                                {
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@limit", System.Data.DbType.Int32, 0),
                                this.DatabaseFactory.ObjectsDB.GetNewParameter("@last_id", System.Data.DbType.Int32, 0),
                                };
                        }    
                            

                        _sql = (_sql.IndexOf(";") > 0) ? _sql.Substring(_sql.IndexOf(";") + 1) : _sql;
                        var dt2 = this.DatabaseFactory.ObjectsDB.DoQuery(_sql, parameters);
                        columns = dt2.Columns;

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

