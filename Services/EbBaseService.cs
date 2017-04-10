using System;
using System.Collections.Generic;
using ServiceStack;
using ExpressBase.Data;
using ExpressBase.Common;
using System.IO;
using ExpressBase.Objects;
using System.Data;
using ServiceStack.Logging;

namespace ExpressBase.ServiceStack
{
    public class EbBaseService : Service
    {
        internal string ClientID { get; set; }

        private static Dictionary<string, string> _infraDbSqlQueries;
        internal static Dictionary<string, string> InfraDbSqlQueries
        {
            get
            {
                if (_infraDbSqlQueries == null)
                {
                    _infraDbSqlQueries = new Dictionary<string, string>();
                    _infraDbSqlQueries.Add("KEY1", "SELECT id, accountname, profilelogo FROM eb_tenantaccount WHERE tenantid=@tenantid");
                }

                return _infraDbSqlQueries;
            }
        }

        private static DatabaseFactory _infraDf;
        internal static DatabaseFactory InfraDatabaseFactory
        {
            get
            {
                if (_infraDf == null)
                {
                    string path = Path.Combine(Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName, "EbInfra.conn");
                    _infraDf = new DatabaseFactory(EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(path)));
                }

                return _infraDf;
            }
        }

        internal ILog Log { get { return LogManager.GetLogger(GetType()); } }

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
                        var bytea = InfraDatabaseFactory.InfraDB_RO.DoQuery<byte[]>(string.Format("SELECT config FROM eb_tenantaccount WHERE cid='{0}'", this.ClientID));

                        if (bytea == null)
                            this.Response.ReturnAuthRequired();
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
                if (!string.IsNullOrEmpty(this.ClientID))
                {
                    EbTableCollection tcol = redisClient.Get<EbTableCollection>(string.Format("EbTableCollection_{0}",this.ClientID));
                    EbTableColumnCollection ccol = redisClient.Get<EbTableColumnCollection>(string.Format("EbTableColumnCollection_{0}", this.ClientID));
                    if (tcol == null || ccol == null)
                    {
                        tcol = new EbTableCollection();
                        ccol = new EbTableColumnCollection();
                        string sql = "SELECT id,tablename FROM eb_tables;" + "SELECT id,columnname,columntype,table_id FROM eb_tablecolumns;";
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
                                TableId = Convert.ToInt32(dr1[3])
                            };
                            ccol.Add(ebtc.Name, ebtc);
                            
                        }

                        redisClient.Set<EbTableCollection>(string.Format("EbTableCollection_{0}", this.ClientID), tcol);
                        redisClient.Set<EbTableColumnCollection>(string.Format("EbTableColumnCollection_{0}", this.ClientID), ccol);
                    }
                }
                else
                {
                    EbTableCollection tcol = redisClient.Get<EbTableCollection>("EbInfraTableCollection");
                    EbTableColumnCollection ccol = redisClient.Get<EbTableColumnCollection>("EbInfraTableColumnCollection");

                    if (tcol == null || ccol == null)
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
                           ccol.Add(ebtc.Name, ebtc);
                           
                        }

                        redisClient.Set<EbTableCollection>("EbInfraTableCollection", tcol);
                        redisClient.Set<EbTableColumnCollection>("EbInfraTableColumnCollection", ccol);
                    }
                }
            }
        }
    }
}
