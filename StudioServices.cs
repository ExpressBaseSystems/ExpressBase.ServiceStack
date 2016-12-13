using System.Collections.Generic;
using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.OrmLite;
using ServiceStack.Text;
using System.Runtime.Serialization;
using ExpressBase.Common;
using ExpressBase.Data;
using System;

namespace ExpressBase.ServiceStack
{
    [Route("/forms")]
    [Route("/forms/{Id}")]
    public class SearchForms : IReturn<FormResponse>
    {
        public int Id { get; set; }
    }

    [Route("/forms/delete/{Id}")]
    public class DeleteForm
    {
        public int Id { get; set; }
    }

    [DataContract]
    [Csv(CsvBehavior.FirstEnumerable)]
    public class FormResponse
    {
        [DataMember(Order = 1)]
        public List<Form> Forms { get; set; }
    }

    [DataContract]
    [Route("/forms", "POST")]
    public class Form
    {
        [DataMember(Order = 1)]
        [AutoIncrement]
        public int Id { get; set; }

        [DataMember(Order = 2)]
        public string Name { get; set; }

        [DataMember(Order = 4)]
        public byte[] Bytea { get; set; }

        public Form() { }
        public Form(int id, string name, byte[] bytea)
        {
            Id = id;
            this.Name = name;
            this.Bytea = bytea;
        }
    }

    [ClientCanSwapTemplates]
    [DefaultView("Forms")]
    public class FormsService : Service
    {
        public object Get(SearchForms request)
        {
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            EbDataTable dt = null;
            using (var con = df.ObjectsDatabase.GetNewConnection())
            {
                con.Open();
                dt = df.ObjectsDatabase.DoQuery("SELECT * FROM eb_objects;");
            };

            List<Form> lf = new List<Form>();
            foreach (EbDataRow dr in dt.Rows)
            {
                var _id = Convert.ToInt32(dr[0]);
                bool bAddMe = (request.Id == 0) ? true : (request.Id > 0 && request.Id == _id);

                if (bAddMe)
                {
                    lf.Add(new Form
                    {
                        Id = Convert.ToInt32(dr[0]),
                        Name = dr[1].ToString(),
                        Bytea = dr[2] as byte[]
                    });
                }
            }

            return new FormResponse
            {
                Forms = lf
            };
        }

        public object Any(DeleteForm request)
        {
            return Get(new SearchForms());
        }

        public object Post(Form request)
        {
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            using (var con = df.ObjectsDatabase.GetNewConnection())
            {
                con.Open();
                var cmd = df.ObjectsDatabase.GetNewCommand(con, "INSERT INTO eb_objects (object_name, obj_bytea) VALUES (@object_name, @obj_bytea);");
                cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("object_name", System.Data.DbType.String, request.Name));
                cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter("obj_bytea", System.Data.DbType.Binary, request.Bytea));
                cmd.ExecuteNonQuery();
            };

            return Get(new SearchForms());
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

