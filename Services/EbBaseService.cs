using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ServiceStack;
using ServiceStack.Redis;
using ExpressBase.Data;
using ExpressBase.Common;
using System.IO;
using ExpressBase.Objects;
using System.Data;
using ServiceStack.Auth;
using System.Configuration;
using ServiceStack.Configuration;

namespace ExpressBase.ServiceStack
{
    public class EbBaseService : Service
    {
        internal string ClientID { get; set; }

        internal DatabaseFactory DatabaseFactory
        {
            get
            {
                EbClientConf conf = null;

                using (var client = this.Redis)
                {
                    string key = string.Format("EbClientConf_{0}", this.ClientID);

                    conf = client.Get<EbClientConf>(key);
                    if (conf == null)
                    {
                        string path = Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName;
                        var infraconf = EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(Path.Combine(path, "EbInfra.conn")));

                        var df = new DatabaseFactory(infraconf);
                        var bytea = df.InfraDB_RO.DoQuery<byte[]>(string.Format("SELECT config FROM eb_tenantaccount WHERE cid='{0}'", this.ClientID));

                        if (bytea == null)
                            throw new Exception("Unauthorized!");
                        conf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea);

                        client.Set<EbClientConf>(key, conf);
                    }
                }

                return new DatabaseFactory(conf);
            }
        }

        private void LoadCache()
        {
            using (var redisClient = this.Redis)
            {
                EbTableCollection tcol = redisClient.Get<EbTableCollection>("EbTableCollection");
                EbTableColumnCollection ccol = redisClient.Get<EbTableColumnCollection>("EbTableColumnCollection");

                //if (tcol == null || ccol == null)
                {
                    tcol = new EbTableCollection();
                    ccol = new EbTableColumnCollection();

                    string sql = "SELECT id,tablename FROM eb_tables;" + "SELECT id,columnname,columntype FROM eb_tablecolumns;";
                    var dt1 = this.DatabaseFactory.ObjectsDB.DoQueries(sql);

                    foreach (EbDataRow dr in dt1.Tables[0].Rows)
                    {
                        EbTable ebt = new EbTable
                        {
                            Id = Convert.ToInt32(dr[0]),
                            Name = dr[1].ToString()
                        };

                        tcol.Add(ebt.Id, ebt);
                    }

                    foreach (EbDataRow dr1 in dt1.Tables[1].Rows)
                    {
                        EbTableColumn ebtc = new EbTableColumn
                        {
                            Type = (DbType)(dr1[2]),
                            Id = Convert.ToInt32(dr1[0]),
                            Name = dr1[1].ToString(),
                        };
                        if (!ccol.ContainsKey(ebtc.Name))
                        {
                            ccol.Add(ebtc.Name, ebtc);
                        }
                    }

                    redisClient.Set<EbTableCollection>("EbTableCollection", tcol);
                    redisClient.Set<EbTableColumnCollection>("EbTableColumnCollection", ccol);
                }
            }
        }
    }
}
