using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.Text;
using System.Runtime.Serialization;
using ExpressBase.Common;
using ExpressBase.Data;
using System;
using ExpressBase.Objects;
using System.Collections.Generic;

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
    }

    [Route("/ds")]
    [Route("/ds/columns/{Id}")]
    public class DataSourceColumnsRequest : IReturn<DataSourceColumnsResponse>
    {
        public int Id { get; set; }

        public string SearchText { get; set; }

        public string OrderByDirection { get; set; }

        public string SelectedColumnName { get; set; }
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
    public class DataSourceService : Service
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
            List<string> selectedValue = new List<string>();
            if (!string.IsNullOrEmpty(request.SearchColumnName))
                searchColumn = new List<string>(request.SearchColumnName.Split(','));
            if (!string.IsNullOrEmpty(request.SearchText))
                searchValue = new List<string>(request.SearchText.Split(','));
            if (!string.IsNullOrEmpty(base.Request.QueryString["selectedvalue"]))
                selectedValue = new List<string>(base.Request.QueryString["selectedvalue"].Split(','));
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            var dt = df.ObjectsDatabase.DoQuery(string.Format("SELECT obj_bytea FROM eb_objects WHERE id={0}", request.Id));

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

                            if (selectedValue[j] == "null")
                                _c += string.Format("AND LOWER({0})::text LIKE LOWER('%{1}%') ", searchColumn[j], searchValue[j]);
                            else if (selectedValue[j] == "B")
                            {
                                List<string> lst = new List<string>();
                                lst = new List<string>(searchValue[j].Split('@'));
                                if (Convert.ToInt32(lst[0]) < Convert.ToInt32(lst[1]))
                                    _c += string.Format("AND {0} > '{1}' AND {0} < '{2}' ", searchColumn[j], lst[0], lst[1]);
                                else
                                    _c += string.Format("AND {0} > '{1}' AND {0} < '{2}' ", searchColumn[j], lst[1], lst[0]);
                            }

                            else
                                _c += string.Format("AND {0} {1} '{2}' ", searchColumn[j], selectedValue[j], searchValue[j]);
                        }
                        _sql = _ds.Sql.Replace("@and_searchplaceholder", _c);
                    }
                    _sql = _sql.Replace("@orderbyplaceholder",
                     (string.IsNullOrEmpty(request.OrderColumnName)) ? "id" : string.Format("{0} {1}", request.OrderColumnName, request.OrderByDirection));

                    var parameters = new System.Data.Common.DbParameter[2]
                    {
                        df.ObjectsDatabase.GetNewParameter("@limit", System.Data.DbType.Int32, request.Length),
                        df.ObjectsDatabase.GetNewParameter("@last_id", System.Data.DbType.Int32, request.Start+1)
                    };

                    var _dataset = (request.Length > 0) ? df.ObjectsDatabase.DoQueries(_sql, parameters) : df.ObjectsDatabase.DoQueries(_sql);

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
            if (columns == null)
            {
                request.SearchText = base.Request.QueryString["searchtext"];
                request.SearchText = string.IsNullOrEmpty(request.SearchText) ? "" : request.SearchText; // @txtsearch
                request.OrderByDirection = base.Request.QueryString["order[0][dir]"]; //@order_dir
                request.SelectedColumnName = base.Request.QueryString["col"]; // @selcol

                string _sql = string.Format("SELECT obj_bytea FROM eb_objects WHERE id={0}", request.Id);

                var e = LoadTestConfiguration();
                DatabaseFactory df = new DatabaseFactory(e);
                var dt = df.ObjectsDatabase.DoQuery(_sql);

                if (dt.Rows.Count > 0)
                {
                    var _ds = EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]);
                    if (_ds != null)
                    {
                        _sql = _ds.Sql.Replace("@and_searchplaceholder",
                            (string.IsNullOrEmpty(request.SearchText)) ? string.Empty : string.Format("AND {0}::text LIKE '%{1}%'", request.SelectedColumnName, request.SearchText));

                        _sql = _sql.Replace("@orderbyplaceholder",
                        (string.IsNullOrEmpty(request.SelectedColumnName)) ? "id" : string.Format("{0} {1}", request.SelectedColumnName, request.OrderByDirection));

                        var parameters = new System.Data.Common.DbParameter[2]
                        {
                            df.ObjectsDatabase.GetNewParameter("@limit", System.Data.DbType.Int32, 0),
                            df.ObjectsDatabase.GetNewParameter("@last_id", System.Data.DbType.Int32, 0)
                        };

                        _sql = (_sql.IndexOf(";") > 0) ? _sql.Substring(_sql.IndexOf(";") + 1) : _sql;
                        var dt2 = df.ObjectsDatabase.DoQuery(_sql, parameters);
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

        private void InitDb(string path)
        {
            EbConfiguration e = new EbConfiguration()
            {
                ClientID = "xyz0007",
                ClientName = "XYZ Enterprises Ltd.",
                LicenseKey = "00288-22558-25558",
            };

            e.DatabaseConfigurations.Add(EbDatabases.EB_OBJECTS, new EbDatabaseConfiguration(EbDatabases.EB_OBJECTS, DatabaseVendors.PGSQL, "AlArz2014", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_DATA, new EbDatabaseConfiguration(EbDatabases.EB_DATA, DatabaseVendors.PGSQL, "AlArz2014", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_ATTACHMENTS, new EbDatabaseConfiguration(EbDatabases.EB_ATTACHMENTS, DatabaseVendors.PGSQL, "AlArz2014", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_LOGS, new EbDatabaseConfiguration(EbDatabases.EB_LOGS, DatabaseVendors.PGSQL, "AlArz2014", "localhost", 5432, "postgres", "infinity", 500));

            byte[] bytea = EbSerializers.ProtoBuf_Serialize(e);
            EbFile.Bytea_ToFile(bytea, path);
        }

        public static EbConfiguration ReadTestConfiguration(string path)
        {
            return EbSerializers.ProtoBuf_DeSerialize<EbConfiguration>(EbFile.Bytea_FromFile(path));
        }

        private EbConfiguration LoadTestConfiguration()
        {
            InitDb(@"G:\xyz1.conn");
            return ReadTestConfiguration(@"G:\xyz1.conn");
        }
    }
}

