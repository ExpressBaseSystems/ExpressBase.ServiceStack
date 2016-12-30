using System.Collections.Generic;
using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.OrmLite;
using ServiceStack.Text;
using System.Runtime.Serialization;
using ExpressBase.Common;
using ExpressBase.Data;
using System;
using ExpressBase.UI;
using System.Data.Common;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [DefaultView("Form")]
    public class EbObjectService : Service
    {
        public object Get(EbObjectRequest request)
        {
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            EbDataTable dt = null;
            using (var con = df.ObjectsDatabase.GetNewConnection())
            {
                con.Open();
                string _where_clause = (request.Id > 0) ? string.Format("WHERE id={0}", request.Id) : string.Empty;
                dt = df.ObjectsDatabase.DoQuery(string.Format("SELECT id, obj_name, obj_bytea, obj_type FROM eb_objects {0};", _where_clause));
            };

            List<EbObjectWrapper> f = new List<EbObjectWrapper>();
            foreach (EbDataRow dr in dt.Rows)
            {
                var _form = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    Bytea = dr[2] as byte[],
                    EbObjectType = (dr[3] == System.DBNull.Value || Convert.ToInt32(dr[3]) == 0) ? EbObjectType.Form : (EbObjectType)Convert.ToInt32(dr[3]),
                });

                f.Add(_form);
            }

            return new EbObjectResponse { Data = f };
        }

        public object Post(EbObjectWrapper request)
        {
            bool result = false;

            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            using (var con = df.ObjectsDatabase.GetNewConnection())
            {
                con.Open();
                DbCommand cmd = null;

                if (request.Id == 0)
                {
                    cmd = df.ObjectsDatabase.GetNewCommand(con, "INSERT INTO eb_objects (obj_name, obj_bytea, obj_type) VALUES (@obj_name, @obj_bytea, @obj_type);");
                    cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                }
                else
                {
                    cmd = df.ObjectsDatabase.GetNewCommand(con, "UPDATE eb_objects SET obj_name=@obj_name, obj_bytea=@obj_bytea WHERE id=@id;");
                    cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("@id", System.Data.DbType.Int32, request.Id));
                }

                cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("@obj_bytea", System.Data.DbType.Binary, request.Bytea));

                cmd.ExecuteNonQuery();
                result = true;
            };

            return result;
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
            if (!System.IO.File.Exists(path))
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

