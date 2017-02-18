﻿using System;
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

namespace ExpressBase.ServiceStack
{
    public class EbBaseService : Service
    {
        internal Int64 ClientID {  get { return 100000001; } }

        internal RedisClient RedisClient
        {
            get { return new RedisClient("139.59.39.130", 6379, "Opera754$"); }
        }

        internal DatabaseFactory DatabaseFactory
        {
            get
            {
                EbClientConf conf = null;

                using (var client = this.RedisClient)
                {
                    string key = string.Format("EbClientConf_{0}", this.ClientID);

                    conf = client.Get<EbClientConf>(key);
                    //if (conf == null)
                    {
                        string path = Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName;
                        var infraconf = EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(Path.Combine(path, "EbInfra.conn")));

                        var df = new DatabaseFactory(infraconf);
                        var bytea = df.InfraDB_RO.DoQuery<byte[]>(string.Format("SELECT conf FROM eb_clients WHERE cid={0}", this.ClientID));
                        conf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea);

                        client.Set<EbClientConf>(key, conf);
                    }
                }

                return new DatabaseFactory(conf);
            }
        }

        private void LoadCache()
        {
            using (var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$"))
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