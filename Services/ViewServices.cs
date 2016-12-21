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
    [Route("/viewst")]
    [Route("/viewst/{Id}")]
    public class ViewRequest : IReturn<ViewResponse>
    {
        public int Id { get; set; }
    }

    [DataContract]
    [Csv(CsvBehavior.FirstEnumerable)]
    public class ViewResponse
    {
        [DataMember(Order = 1)]
        public EbDataTable Data { get; set; }
    }

    [DataContract]
    [Route("/viewst", "POST")]
    public class View
    {
        [DataMember(Order = 1)]
        [AutoIncrement]
        public int Id { get; set; }

        [DataMember(Order = 2)]
        public string Name { get; set; }

        [DataMember(Order = 3)]
        public string Sql { get; set; }

        public View() { }
        public View(int id, string name, string sql)
        {
            Id = id;
            this.Name = name;
            this.Sql = sql;
        }
    }

    [ClientCanSwapTemplates]
    [DefaultView("Viewst")]
    public class ViewService : Service
    {
        public object Get(ViewRequest request)
        {
            string _sql = string.Format("SELECT obj_bytea FROM eb_objects WHERE id={0}", request.Id);
            
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            var dt = df.ObjectsDatabase.DoQuery(_sql);

            var _view = EbSerializers.ProtoBuf_DeSerialize<View>((byte[])dt.Rows[0][0]);
            var dt2 = df.ObjectsDatabase.DoQuery(_view.Sql);

            return new ViewResponse
            {
                Data = dt2
            };
        }

        public object Post(View request)
        {
            try
            {
                var e = LoadTestConfiguration();
                DatabaseFactory df = new DatabaseFactory(e);
                using (var con = df.ObjectsDatabase.GetNewConnection())
                {
                    con.Open();
                    var cmd = df.ObjectsDatabase.GetNewCommand(con, "INSERT INTO eb_objects (object_name, obj_bytea) VALUES (@object_name, @obj_bytea);");
                    cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("object_name", System.Data.DbType.String, request.Name));
                    cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("obj_bytea", System.Data.DbType.Binary, EbSerializers.ProtoBuf_Serialize(request)));
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
