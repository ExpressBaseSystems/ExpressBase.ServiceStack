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

    [ClientCanSwapTemplates]
    public class Registerservice : EbBaseService
    {
        private EbTableCollection tcol;
        private EbTableColumnCollection ccol;

        public ViewResponse Any(View request)
        {
            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            tcol = redisClient.Get<EbTableCollection>("EbTableCollection");
            Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", request.FId));
            string sql = string.Format("select * from {0} where id= {1} AND eb_del='false' ", tcol[request.TableId].Name, request.ColId);
            var ds = this.DatabaseFactory.ObjectsDB.DoQueries(sql);
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
            string upsql="";

            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            tcol = redisClient.Get<EbTableCollection>("EbTableCollection");
            ccol = redisClient.Get<EbTableColumnCollection>("EbTableColumnCollection");
            bResult = Uniquetest(request.Colvalues);

            if (Convert.ToBoolean(request.Colvalues["isUpdate"]))
            {
                var _ebform = redisClient.Get<EbForm>("cacheform");
                _ebform.Init4Redis();
                upsql += string.Format("INSERT INTO eb_auditlog(tableid, dataid, eb_fid, operations, timestamp)VALUES({0},{1},{2},{3},'{4}');", request.Colvalues["TableId"], request.Colvalues["DataId"], request.Colvalues["FId"], 1, DateTime.Now);
                foreach (string key in request.Colvalues.Keys)
                {
                    var control = _ebform.GetControl(key);
                    if (control == null)
                    {
                        dict[key] = request.Colvalues[key].ToString();
                       

                    }
                    else if (request.Colvalues[key] != null && ccol.ContainsKey(key))
                    {
                        if ((control.GetData()).ToString() != (request.Colvalues[key].ToString()))
                        {
                            dict[key] = request.Colvalues[key].ToString();
                          //  upsql += string.Format("INSERT INTO eb_auditlogdetails(auditlogid,tableid, columnid, oldvalue, newvalue)VALUES((SELECT currval('eb_auditlog_id_seq')),{0},{1},'{2}','{3}');",  dict["TableId"], ccol[key].Id, control.GetData().ToString(), dict[key].ToString());
                        }
                    }
                    

                }
                foreach(string key in dict.Keys)
                {
                    string nn = dict["TableId"].ToString();
                    if (ccol.ContainsKey(key))
                    {
                        upsql += string.Format("INSERT INTO eb_auditlogdetails(auditlogid,tableid, columnid, oldvalue, newvalue)VALUES((SELECT currval('eb_auditlog_id_seq')),{0},{1},(SELECT {2} FROM {4} WHERE id ={3}),'{5}');", dict["TableId"], ccol[key].Id, key, request.Colvalues["DataId"], tcol[Convert.ToInt32(dict["TableId"])].Name,dict[key]);
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
               
                    upsql += string.Format("UPDATE {0} SET {1} WHERE id = {2} RETURNING id", tcol[request.TableId].Name, _values_sb.ToArray().Join(", "), request.Colvalues["DataId"]);
                  
                    using (var _con = this.DatabaseFactory.ObjectsDB.GetNewConnection())
                    {
                        _con.Open();
                        var _cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(_con, upsql);
                        foreach (KeyValuePair<string, object> element in dict)
                        {
                            var myKey = request.Colvalues.FirstOrDefault(x => x.Value == element.Value).Key;
                            if (ccol.ContainsKey(myKey))

                                _cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", ccol[myKey].Name), ccol[myKey].Type, element.Value));
                        }
                        result = Convert.ToInt32(_cmd.ExecuteNonQuery());
                    }
                    if (result > 0)
                    {        
                        return true;
                    }

                    else
                        return false;

                }
                else
                {
                    return uResult;
                }
            }

            else if (bResult)
            {
                List<string> _params = new List<string>(request.Colvalues.Count);
                List<string> _values = new List<string>(request.Colvalues.Count);
                Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", Convert.ToInt32(request.Colvalues["FId"])));
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

                string _sql = string.Format("INSERT INTO {0} ({1}) VALUES ({2});", tcol[request.TableId].Name, _values.ToArray().Join(","), _params.ToArray().Join(","));
                       _sql += string.Format("INSERT INTO eb_auditlog(tableid,dataid,eb_fid,operations,timestamp)VALUES({0},(SELECT currval('{1}_id_seq')),{2},{3},'{4}')", request.Colvalues["TableId"], tcol[request.TableId].Name, request.Colvalues["FId"], 0, DateTime.Now);

                using (var _con = this.DatabaseFactory.ObjectsDB.GetNewConnection())
                {
                    _con.Open();
                    int DId;
                    var _cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(_con, _sql);
                    foreach (var key in request.Colvalues.Keys)
                    {
                        if (ccol.ContainsKey(key))
                            _cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", key), ccol[key].Type, request.Colvalues[key]));
                    }
                    if ((DId = Convert.ToInt32(_cmd.ExecuteNonQuery())) != 0)
                    {
                      
                        return true;
                    }
                    
                }
            }
            return bResult;

        }

        public bool Uniquetest(Dictionary<string, object> dict)
        {
            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            bool bResult = false;
            Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", Convert.ToInt32(dict["FId"])));
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

            foreach (string key in request.Colvalues.Keys)
                _whclause_sb.Add(string.Format("{0}=@{0}", ccol[key].Name));

            string _sql = string.Format("SELECT COUNT(*) FROM {0} WHERE {1}", tcol[request.TableId].Name, _whclause_sb.ToArray().Join(" AND "));
            using (var _con = this.DatabaseFactory.ObjectsDB.GetNewConnection())
            {
                _con.Open();
                var _cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(_con, _sql);
                foreach (KeyValuePair<string, object> dict in request.Colvalues)
                {
                    if (ccol.ContainsKey(dict.Key))

                        _cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter(string.Format("@{0}", ccol[dict.Key].Name), ccol[dict.Key].Type, dict.Value));
                }

                return (Convert.ToInt32(_cmd.ExecuteScalar()) == 0);
            }
        }
    }
}
