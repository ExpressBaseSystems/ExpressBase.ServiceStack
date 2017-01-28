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
using ServiceStack.Redis;
using ExpressBase.Objects;

namespace ExpressBase.ServiceStack.Services
{
    [DataContract]
    [Route("/insert", "POST")]
    public class Register : IReturn<bool>
    {

        [DataMember(Order = 0)]
        public Dictionary<string, object> Colvalues { get; set; }

        [DataMember(Order = 1)]
        public int TableId { get; set; }

    }


    [DataContract]
    [Route("/uc", "POST")]
    public class CheckIfUnique : IReturn<bool>
    {
        [DataMember(Order = 0)]
        public Dictionary<string, object> Colvalues { get; set; }

        [DataMember(Order = 1)]
        public int TableId { get; set; }

    }

    [DataContract]
    [Route("/view/{ColId}", "GET")]
    public class ViewUser : IReturn<ViewResponse>
    {

        [DataMember(Order = 1)]
        public int ColId { get; set; }

    }

    [DataContract]
    public class ViewResponse
    {
        [DataMember(Order = 1)]
        public Dictionary<string, object> Viewvalues { get; set; }
    }

    [DataContract]
    [Route("/edit", "POST")]
    public class EditUser : IReturn<bool>
    {

        [DataMember(Order = 0)]
        public int colid { get; set; }
        //public Dictionary<int, object> Condvalues { get; set; }

        [DataMember(Order = 1)]
        public Dictionary<string, object> Colvalues { get; set; }

        [DataMember(Order = 2)]
        public int TableId { get; set; }

    }


    [ClientCanSwapTemplates]
    public class Registerservice : Service
    {
        private EbTableCollection tcol;
        private EbTableColumnCollection ccol;

        //public bool Post(EditUser request)
        //{
        //    List<string> _values_sb = new List<string>(request.Colvalues.Count);
        //    List<string> _where_sb = new List<string>(request.Colvalues.Count);

        //    var e = LoadTestConfiguration();
        //    DatabaseFactory df = new DatabaseFactory(e);

        //    LoadCache();


        //    foreach (int key in request.Colvalues.Keys)
        //    {
        //        _values_sb.Add(string.Format("{0} = @{0}", ccol[key].Name));

        //    }
        //    string _sql = string.Format("UPDATE {0} SET {1} WHERE id = {2} RETURNING id", tcol[request.TableId].Name, _values_sb.ToArray().Join(", "), request.colid);


        //    using (var _con = df.ObjectsDatabase.GetNewConnection())
        //    {
        //        _con.Open();
        //        var _cmd = df.ObjectsDatabase.GetNewCommand(_con, _sql);
        //        foreach (KeyValuePair<int, object> dict in request.Colvalues)
        //        {
        //            if (ccol.ContainsKey(dict.Key))

        //                _cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter(string.Format("@{0}", ccol[dict.Key].Name), ccol[dict.Key].Type, dict.Value));
        //        }



        //        int result = Convert.ToInt32(_cmd.ExecuteNonQuery());
        //        //string sql = string.Format("INSERT INTO eb_auditlog(tableid,dataid,operations) VALUES((SELECT id FROM eb_tables WHERE tablename={0}),{1},{2})", tcol[request.TableId].Name, result, 1);
        //        //var cmd2 = df.ObjectsDatabase.GetNewCommand(_con, sql);
        //        //cmd2.ExecuteNonQuery();
        //        return (result > 0);
        //    }
        //}

        public ViewResponse Any(ViewUser request)
        {
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);

            Dictionary<string, object> Colvalues = new Dictionary<string, object>();

            string sql = string.Format("select * from eb_users where id= {0} ", request.ColId);
            var dt = df.ObjectsDatabase.DoQuery(sql);
            foreach (EbDataRow dr in dt.Rows)
            {
                foreach (EbDataColumn col in dt.Columns)
                    Colvalues.Add(col.ColumnName, dr[col.ColumnIndex]);
            }
            return new ViewResponse
            {
                Viewvalues = Colvalues
            };
        }

        public bool Any(Register request)
        {
            bool bResult=false;
            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", Convert.ToInt32(request.Colvalues["fId"])));
            var uniquelist = _form.GetControlsByPropertyValue<bool>("Unique", true, Objects.EnumOperator.Equal);
            foreach (EbControl control in uniquelist)
            {
                var chkifuq = new CheckIfUnique();
                chkifuq.TableId = _form.Table.Id;
                chkifuq.Colvalues = new Dictionary<string, object>();
                chkifuq.Colvalues.Add(control.Name, request.Colvalues[control.Name]);
                bResult = this.Post(chkifuq);
                if (!bResult)
                    break;
            }

            if(bResult)
            {
                List<string> _params = new List<string>(request.Colvalues.Count);
                List<string> _values = new List<string>(request.Colvalues.Count);

                var e = LoadTestConfiguration();
                DatabaseFactory df = new DatabaseFactory(e);

                tcol = redisClient.Get<EbTableCollection>("EbTableCollection");
                ccol = redisClient.Get<EbTableColumnCollection>("EbTableColumnCollection");


                foreach (string key in request.Colvalues.Keys)
                {
                    if (ccol.ContainsKey(key))
                    {
                        _values.Add(string.Format("{0}", ccol[key].Name));
                        _params.Add(string.Format("@{0}", ccol[key].Name));
                    }

                }
                string _sql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", tcol[request.TableId].Name, _values.ToArray().Join(","), _params.ToArray().Join(","));
                //var dt = df.ObjectsDatabase.DoQuery(_sql);
                using (var _con = df.ObjectsDatabase.GetNewConnection())
                {
                    _con.Open();
                    var _cmd = df.ObjectsDatabase.GetNewCommand(_con, _sql);
                    foreach (KeyValuePair<string, object> dict in request.Colvalues)
                    {
                        if (ccol.ContainsKey(dict.Key))

                            _cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter(string.Format("@{0}", dict.Key), ccol[dict.Key].Type, dict.Value));
                    }

                    return (Convert.ToInt32(_cmd.ExecuteScalar()) == 0);
                }
            }
            return bResult;
            
        }

        public bool Post(CheckIfUnique request)
        {
            // CIUinner(
            List<string> _whclause_sb = new List<string>(request.Colvalues.Count);
            using (var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$"))
            {
                tcol = redisClient.Get<EbTableCollection>("EbTableCollection");
                ccol = redisClient.Get<EbTableColumnCollection>("EbTableColumnCollection");
            }
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);

            foreach (string key in request.Colvalues.Keys)
                _whclause_sb.Add(string.Format("{0}=@{0}", ccol[key].Name));

            string _sql = string.Format("SELECT COUNT(*) FROM {0} WHERE {1}", tcol[request.TableId].Name, _whclause_sb.ToArray().Join(" AND "));
            using (var _con = df.ObjectsDatabase.GetNewConnection())
            {
                _con.Open();
                var _cmd = df.ObjectsDatabase.GetNewCommand(_con, _sql);
                foreach (KeyValuePair<string, object> dict in request.Colvalues)
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
            e.DatabaseConfigurations.Add(EbDatabases.EB_LOGS, new EbDatabaseConfiguration(EbDatabases.EB_LOGS, DatabaseVendors.PGSQL, "AlArz2014", "localhost", 5432, "localhost", "infinity", 500));

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
