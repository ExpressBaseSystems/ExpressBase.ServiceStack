using ExpressBase.Common;
using ExpressBase.Data;
using ExpressBase.Security;
using Microsoft.AspNetCore.Http;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    [DataContract]
    [Route("/register", "POST")]
    public class Register : IReturn<RegisterationResponse>
    {
        [DataMember(Order = 1)]
        public string Email { get; set; }

        [DataMember(Order = 2)]
        public string Password { get; set; }

        [DataMember(Order = 3)]
        public string FirstName { get; set; }

        [DataMember(Order = 4)]
        public string LastName { get; set; }

        [DataMember(Order = 5)]
        public string MiddleName { get; set; }

        [DataMember(Order = 6)]
        public DateTime dob { get; set; }

        [DataMember(Order = 7)]
        public string Phnoprimary { get; set; }

        [DataMember(Order = 8)]
        public string Phnosecondary { get; set; }

        [DataMember(Order = 9)]
        public string Landline { get; set; }

        [DataMember(Order = 10)]
        public string Extension { get; set; }

        [DataMember(Order = 11)]
        public string Locale { get; set; }

        [DataMember(Order = 12)]
        public string Alternateemail { get; set; }

        [DataMember(Order = 13)]
        public byte[] Profileimg { get; set; }
    }

    [DataContract]
    public class RegisterationResponse
    {
        [DataMember(Order = 1)]
        public bool Registereduser { get; set; }
    }

    [DataContract]
    [Route("/register/{TableId}", "POST")]
    public class CheckIfUnique : IReturn<CheckIfUniqueResponse>
    {

        [DataMember(Order = 0)]
        public Dictionary<int, string> Colvalues { get; set; }

        [DataMember(Order = 1)]
        public int TableId { get; set; }
        //[DataMember(Order = 1)]
        //public string ColumnName { get; set; }
        //[DataMember(Order = 2)]
        //public string ColumnValue { get; set; }

    }
    [DataContract]
    public class CheckIfUniqueResponse
    {
        [DataMember(Order = 1)]
        public bool uniqueno { get; set; }
    }

    [ClientCanSwapTemplates]
    public class Registerservice : Service
    {
        public RegisterationResponse Any(Register request)
        {
            bool u = User.Create(request.Email, request.Password, request.FirstName, request.LastName, request.MiddleName, request.dob, request.Phnoprimary, request.Phnosecondary, request.Landline, request.Extension, request.Locale, request.Alternateemail, request.Profileimg);
            return new RegisterationResponse
            {
                Registereduser = u
            };
        }

        private EbTableCollection tcol;
        private EbTableColumnCollection ccol;

        public bool Post(CheckIfUnique request)
        {
            List<string> _whclause_sb = new List<string>(request.Colvalues.Count);

            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);

            LoadCache();

           
            foreach (int key in request.Colvalues.Keys)
                _whclause_sb.Add(string.Format("{0}=@{0}", ccol[key].Name));

            string _sql = string.Format("SELECT COUNT(*) FROM {0} WHERE {1}", tcol[request.TableId].Name, _whclause_sb.ToArray().Join(" AND "));

            //var dt = df.ObjectsDatabase.DoQuery(_sql);
            using (var _con = df.ObjectsDatabase.GetNewConnection())
            {
                _con.Open();
                var _cmd = df.ObjectsDatabase.GetNewCommand(_con, _sql);
                foreach (KeyValuePair<int, string> dict in request.Colvalues)
                {
                    if (ccol.ContainsKey(dict.Key))
                        
                        _cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter(string.Format("@{0}", ccol[dict.Key].Name), ccol[dict.Key].Type, dict.Value));
                }

                return (Convert.ToInt32(_cmd.ExecuteScalar()) == 0);
            }
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

        private void LoadCache()
        {
            tcol = new EbTableCollection();
            ccol = new EbTableColumnCollection();

            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            string sql = "SELECT id,tablename FROM eb_tables;" + "SELECT id,columnname,columntype FROM eb_columns;";
            var dt1 = df.ObjectsDatabase.DoQueries(sql);
            foreach (EbDataRow dr in dt1.Tables[0].Rows)
            {
                EbTable ebt = new EbTable
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString()
                };

                tcol.Add(ebt.Id, ebt);
            }
            foreach(EbDataRow dr1 in dt1.Tables[1].Rows)
            {
                EbTableColumn ebtc = new EbTableColumn
                {
                    ColId = Convert.ToInt32(dr1[0]),
                    Name = dr1[1].ToString(),
                    Type = (DbType)(dr1[2])
                };
                if (!ccol.ContainsKey(ebtc.ColId))
                {
                    ccol.Add(ebtc.ColId, ebtc);
                }
            }
   
            
            }
        }
    }

