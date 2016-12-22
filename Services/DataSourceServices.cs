using System.Collections.Generic;
using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.OrmLite;
using ServiceStack.Text;
using System.Runtime.Serialization;
using ExpressBase.Common;
using ExpressBase.Data;
using System;
using System.Text;

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
        public RowColletion Data { get; set; }

        [DataMember(Order = 2)]
        public int Draw { get; set; }
    }

    [DataContract]
    [Csv(CsvBehavior.FirstEnumerable)]
    public class DataSourceColumnsResponse
    {
        [DataMember(Order = 1)]
        public ColumnColletion Columns { get; set; }
    }

    [DataContract]
    [Route("/ds", "POST")]
    public class EbDataSource
    {
        [DataMember(Order = 1)]
        [AutoIncrement]
        public int Id { get; set; }

        [DataMember(Order = 2)]
        public string Name { get; set; }

        [DataMember(Order = 3)]
        public string Sql { get; set; }

        public EbDataSource() { }
        public EbDataSource(string name, string sql)
        {
            this.Name = name;
            this.Sql = sql;
        }
    }

    [ClientCanSwapTemplates]
    [DefaultView("ds")]
    public class DataSourceService : Service
    {
        public object Get(DataSourceDataRequest request)
        {
            string _sql = string.Format("SELECT obj_bytea FROM eb_objects WHERE id={0}", request.Id);
            
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            var dt = df.ObjectsDatabase.DoQuery(_sql);

            DataSourceDataResponse dsresponse = null;

            if (dt.Rows.Count > 0)
            {
                var _ds = EbSerializers.ProtoBuf_DeSerialize<EbDataSource>((byte[])dt.Rows[0][0]);

                if (_ds != null)
                {
                    string sql = (request.Length > 0)
                        ? string.Format("SELECT * FROM ({0}) AAA LIMIT {1} OFFSET {2}", _ds.Sql.ReplaceAll(";", string.Empty), request.Length, request.Start)
                        : _ds.Sql;
                    var dt2 = df.ObjectsDatabase.DoQuery(sql);

                    dsresponse = new DataSourceDataResponse
                    {
                        Draw = request.Draw,
                        Data = dt2.Rows
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
                    var dt2 = df.ObjectsDatabase.DoQuery(string.Format("SELECT * FROM ({0}) AAA LIMIT 0", _ds.Sql.ReplaceAll(";", string.Empty)));

                    dsresponse = new DataSourceColumnsResponse
                    {
                        Columns = dt2.Columns
                    };
                }
            }

            return dsresponse;
        }

        public object Post(EbDataSource ds)
        {
            try
            {
                var e = LoadTestConfiguration();
                DatabaseFactory df = new DatabaseFactory(e);
                using (var con = df.ObjectsDatabase.GetNewConnection())
                {
                    con.Open();
                    var cmd = df.ObjectsDatabase.GetNewCommand(con, "INSERT INTO eb_objects (object_name, obj_bytea) VALUES (@object_name, @obj_bytea);");
                    cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("object_name", System.Data.DbType.String, ds.Name));
                    cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("obj_bytea", System.Data.DbType.Binary, EbSerializers.ProtoBuf_Serialize(ds)));
                    cmd.ExecuteNonQuery();
                    return true;
                };
            }
            catch (Exception e) { }

            return false;
        }

        private void InitDb(string path)
        {
            EbConfiguration e = new EbConfiguration()
            {
                ClientID = "xyz0007",
                ClientName = "XYZ Enterprises Ltd.",
                LicenseKey = "00288-22558-25558",
            };

            e.DatabaseConfigurations.Add(EbDatabases.EB_OBJECTS, new EbDatabaseConfiguration(EbDatabases.EB_OBJECTS, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_DATA, new EbDatabaseConfiguration(EbDatabases.EB_DATA, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_ATTACHMENTS, new EbDatabaseConfiguration(EbDatabases.EB_ATTACHMENTS, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));
            e.DatabaseConfigurations.Add(EbDatabases.EB_LOGS, new EbDatabaseConfiguration(EbDatabases.EB_LOGS, DatabaseVendors.PGSQL, "eb_objects", "localhost", 5432, "postgres", "infinity", 500));

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
