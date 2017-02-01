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
    public class FormPersistRequest : IReturn<bool>
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
    public class View : IReturn<ViewResponse>
    {

        [DataMember(Order = 1)]
        public int ColId { get; set; }

        [DataMember(Order = 2)]
        public int TableId { get; set; }

        [DataMember(Order = 3)]
        public int FId { get; set; }

    }

    [DataContract]
    public class ViewResponse
    {
     

        [DataMember(Order = 1)]
        public EbForm ebform { get; set; }
    }

    [DataContract]
    [Route("/edit", "POST")]
    public class EditUser : IReturn<bool>
    {

        [DataMember(Order = 0)]
        public int colid { get; set; }
      

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

        public ViewResponse Any(View request)
        {
            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            tcol = redisClient.Get<EbTableCollection>("EbTableCollection");
            Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", request.FId));
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            string sql = string.Format("select * from {0} where id= {1} ", tcol[request.TableId].Name, request.ColId);
            var ds = df.ObjectsDatabase.DoQueries(sql);
            _form.SetData(ds);
            return new ViewResponse
            {
                ebform = _form
            };
        }

        public bool Any(FormPersistRequest request)
        {
            bool bResult = false;
            bool uResult = true;
            List<string> list = new List<string>();
            Dictionary<string, object> dict = new Dictionary<string, object>();
            var e = LoadTestConfiguration();
            DatabaseFactory df = new DatabaseFactory(e);
            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            tcol = redisClient.Get<EbTableCollection>("EbTableCollection");
            ccol = redisClient.Get<EbTableColumnCollection>("EbTableColumnCollection");
            
            if (Convert.ToBoolean(request.Colvalues["isUpdate"]))
                {
                var _ebform = redisClient.Get<EbForm>("cacheform");
                _ebform.Init4Redis();
                var editunique = _ebform.GetControlsByPropertyValue<bool>("Unique", true, Objects.EnumOperator.Equal);
                foreach (string key in request.Colvalues.Keys)
                {
                    var control = _ebform.GetControl(key);

                    if (control != null)
                    {
                        if ((control.GetData().ToString()) != (request.Colvalues[key].ToString()))
                        {
                            dict[key] = request.Colvalues[key].ToString();

                        }
                    }
                    else
                    {
                        dict[key] = request.Colvalues[key].ToString();
                    }

                }
                uResult = Uniquetest(dict);
                if (uResult)
                {
                    int result;
                    List<string> _values_sb = new List<string>(list.Count);
                    List<string> _where_sb = new List<string>(list.Count);

                    foreach (string key in request.Colvalues.Keys)
                    {
                        foreach (KeyValuePair<string, object> element in dict)
                        {
                            if (request.Colvalues[key].ToString() == element.Value.ToString())
                            {
                                if (ccol.ContainsKey(element.Key))
                                    _values_sb.Add(string.Format("{0} = @{0}", ccol[key].Name));
                            }
                        }
                    }
                    string _sql = string.Format("UPDATE {0} SET {1} WHERE id = {2} RETURNING id", tcol[request.TableId].Name, _values_sb.ToArray().Join(", "), request.Colvalues["DataId"]);

                    using (var _con = df.ObjectsDatabase.GetNewConnection())
                    {
                        _con.Open();
                        var _cmd = df.ObjectsDatabase.GetNewCommand(_con, _sql);
                        foreach (KeyValuePair<string, object> element in dict)
                        {
                            var myKey = request.Colvalues.FirstOrDefault(x => x.Value == element.Value).Key;
                            if (ccol.ContainsKey(myKey))

                                _cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter(string.Format("@{0}", ccol[myKey].Name), ccol[myKey].Type, element.Value));
                        }
                        result = Convert.ToInt32(_cmd.ExecuteNonQuery());
                    }
                    if (result > 0)
                        return true;
                    else
                        return false;
                }
            }
            bResult = Uniquetest(request.Colvalues);
            if (bResult)
            {
                List<string> _params = new List<string>(request.Colvalues.Count);
                List<string> _values = new List<string>(request.Colvalues.Count);
                Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", Convert.ToInt32(request.Colvalues["fId"])));
                foreach (string key in request.Colvalues.Keys)
                {
                    var _control = _form.GetControl(key);
                    if (_control != null && !_control.SkipPersist)
                    {
                        if (ccol.ContainsKey(key))
                        {
                            _values.Add(string.Format("{0}", ccol[key].Name));
                            _params.Add(string.Format("@{0}", ccol[key].Name));
                        }
                    }
                }
                string _sql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", tcol[request.TableId].Name, _values.ToArray().Join(","), _params.ToArray().Join(","));
               
                using (var _con = df.ObjectsDatabase.GetNewConnection())
                {
                    _con.Open();
                    var _cmd = df.ObjectsDatabase.GetNewCommand(_con, _sql);
                    foreach (var key in request.Colvalues.Keys)
                    {
                        if (ccol.ContainsKey(key))
                            _cmd.Parameters.Add(df.ObjectsDatabase.GetNewParameter(string.Format("@{0}", key), ccol[key].Type, request.Colvalues[key]));
                    }

                    return (Convert.ToInt32(_cmd.ExecuteScalar()) == 0);
                }
            }
            return uResult;

        }

        public bool Uniquetest(Dictionary<string, object> dict)
        {
            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            bool bResult = false;
            Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", Convert.ToInt32(dict["fId"])));
            var uniquelist = _form.GetControlsByPropertyValue<bool>("Unique", true, Objects.EnumOperator.Equal);
            foreach (EbControl control in uniquelist)
            {
                if (dict.ContainsKey(control.Name))
                {
                    var chkifuq = new CheckIfUnique();
                    chkifuq.TableId = _form.Table.Id;
                    chkifuq.Colvalues = new Dictionary<string, object>();
                    chkifuq.Colvalues.Add(control.Name, dict[control.Name]);
                    bResult = this.Post(chkifuq);
                    if (!bResult)
                        break;
                }
                else
                {
                    bResult = true;

                }
            }
            return bResult;
        }

        public bool Post(CheckIfUnique request)
        {
           
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
