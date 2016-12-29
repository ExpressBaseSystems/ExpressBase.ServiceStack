using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.Text;
using System.Runtime.Serialization;
using ExpressBase.Common;
using ExpressBase.Data;
using System;
using ExpressBase.UI;

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

        public string Search { get; set; }
    }

    [Route("/ds")]
    [Route("/ds/columns/{Id}")]
    public class DataSourceColumnsRequest : IReturn<DataSourceColumnsResponse>
    {
        public int Id { get; set; }
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
            request.Search = base.Request.QueryString["search[value]"];
            request.Search = string.IsNullOrEmpty(request.Search) ? "" : request.Search;

            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            var dt = df.ObjectsDatabase.DoQuery(string.Format("SELECT obj_bytea FROM eb_objects WHERE id={0}", request.Id));

            DataSourceDataResponse dsresponse = null;

            if (dt.Rows.Count > 0)
            {
                var _ds = EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]);

                if (_ds != null)
                {
                    //string _sql_orig = _ds.Sql.ReplaceAll(";", string.Empty);
                    //string _sql = string.Empty;

                    //if (request.Length > 0)
                    //{
                    //    //_sql = string.Format("SELECT * FROM ({0}) AAA WHERE id>{1} ORDER BY id LIMIT {2}", _ds.Sql.ReplaceAll(";", string.Empty), ((request.Draw - 1) * request.Length), request.Length);
                    //    _sql = string.Format("SELECT COUNT(*) FROM ({0}) AAA;", _sql_orig) + _sql;
                    //}

                    var parameters = new System.Data.Common.DbParameter[3] 
                    {
                        df.ObjectsDatabase.GetNewParameter("@limit", System.Data.DbType.Int32, request.Length),
                        df.ObjectsDatabase.GetNewParameter("@last_id", System.Data.DbType.Int32, ((request.Draw - 1) * request.Length)),
                        df.ObjectsDatabase.GetNewParameter("@search", System.Data.DbType.String, request.Search)
                    };

                    var _dataset = df.ObjectsDatabase.DoQueries(_ds.Sql, parameters);

                    dsresponse = new DataSourceDataResponse
                    {
                        Draw = request.Draw,
                        Data = (_dataset.Tables.Count > 1) ? _dataset.Tables[1].Rows : _dataset.Tables[0].Rows,
                        RecordsTotal = (_dataset.Tables.Count > 1) ? Convert.ToInt32(_dataset.Tables[0].Rows[0][0]) : _dataset.Tables[0].Rows.Count,
                        RecordsFiltered = (_dataset.Tables.Count > 1) ? Convert.ToInt32(_dataset.Tables[0].Rows[0][0]) : _dataset.Tables[0].Rows.Count
                    };
                }
            }

            return dsresponse;
        }

        public object Get(DataSourceColumnsRequest request)
        {
            string _sql = string.Format("SELECT obj_bytea FROM eb_objects WHERE id={0}", request.Id);

            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            var dt = df.ObjectsDatabase.DoQuery(_sql);

            DataSourceColumnsResponse dsresponse = null;

            if (dt.Rows.Count > 0)
            {
                var _ds = EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]);
                if (_ds != null)
                {
                    var parameters = new System.Data.Common.DbParameter[3]
                    {
                        df.ObjectsDatabase.GetNewParameter("@limit", System.Data.DbType.Int32, 0),
                        df.ObjectsDatabase.GetNewParameter("@last_id", System.Data.DbType.Int32, 0),
                        df.ObjectsDatabase.GetNewParameter("@search", System.Data.DbType.String, string.Empty)
                    };

                    var dt2 = df.ObjectsDatabase.DoQuery(_ds.Sql.Substring(_ds.Sql.IndexOf(';') + 1), parameters);

                    dsresponse = new DataSourceColumnsResponse
                    {
                        Columns = dt2.Columns
                    };
                }
            }

            return dsresponse;
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
            InitDb(@"D:\xyz1.conn");
            return ReadTestConfiguration(@"D:\xyz1.conn");
        }
    }
}
