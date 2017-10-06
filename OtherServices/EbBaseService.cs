using System;
using System.Collections.Generic;
using ServiceStack;
using ExpressBase.Data;
using ExpressBase.Common;
using System.IO;
using ExpressBase.Objects;
using System.Data;
using ServiceStack.Logging;
using ServiceStack.Web;
using ServiceStack.Messaging;
using ExpressBase.ServiceStack;
using ExpressBase.Common.Data;
using ServiceStack.RabbitMq;
using System.Net.Http;
using RestSharp;

namespace ExpressBase.Objects.ServiceStack_Artifacts
{
    public class EbBaseService : Service
    {
        protected TenantDbFactory TenantDbFactory { get; private set; }

        protected  RestClient RestClient  { get; private set; }
        
        protected RabbitMqProducer MessageProducer3 { get; private set; }

        protected RabbitMqQueueClient MessageQueueClient { get; private set; }

        protected RedisServerEvents ServerEvents { get; private set; }

        public EbBaseService() { }

        public EbBaseService(ITenantDbFactory _dbf)
        {
            this.TenantDbFactory = _dbf as TenantDbFactory;
        }

        public EbBaseService(IMessageProducer _mqp)
        {

            this.MessageProducer3 = _mqp as RabbitMqProducer;
        }

        public EbBaseService(RestSharp.IRestClient _rest)
        {
            this.RestClient = _rest as RestClient;
        }

        public EbBaseService(IMessageProducer _mqp, IMessageQueueClient _mqc)
        {

            this.MessageProducer3 = _mqp as RabbitMqProducer;
            this.MessageQueueClient = _mqc as RabbitMqQueueClient;
        }

        public EbBaseService(IMessageProducer _mqp, IMessageQueueClient _mqc, IServerEvents _se)
        {
            this.MessageProducer3 = _mqp as RabbitMqProducer;
            this.MessageQueueClient = _mqc as RabbitMqQueueClient;
            this.ServerEvents = _se as RedisServerEvents;
        }

        public EbBaseService(ITenantDbFactory _dbf, IMessageProducer _mqp)
        {
            this.TenantDbFactory = _dbf as TenantDbFactory;
            this.MessageProducer3 = _mqp as RabbitMqProducer;
        }

        public EbBaseService(ITenantDbFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc)
        {
            this.TenantDbFactory = _dbf as TenantDbFactory;
            this.MessageProducer3 = _mqp as RabbitMqProducer;
            this.MessageQueueClient = _mqc as RabbitMqQueueClient;
        }
        
        private static Dictionary<string, string> _infraDbSqlQueries;

        public static Dictionary<string, string> InfraDbSqlQueries
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

        public ILog Log { get { return LogManager.GetLogger(GetType()); } }


        //private void LoadCache()
        //{
        //    using (var redisClient = this.Redis)
        //    {
        //        if (!string.IsNullOrEmpty(this.ClientID))
        //        {
        //            EbTableCollection tcol = redisClient.Get<EbTableCollection>(string.Format("EbTableCollection_{0}",this.ClientID));
        //            EbTableColumnCollection ccol = redisClient.Get<EbTableColumnCollection>(string.Format("EbTableColumnCollection_{0}", this.ClientID));
        //            if (tcol == null || ccol == null)
        //            {
        //                tcol = new EbTableCollection();
        //                ccol = new EbTableColumnCollection();
        //                string sql = "SELECT id,tablename FROM eb_tables;" + "SELECT id,columnname,columntype,table_id FROM eb_tablecolumns;";
        //                var dt1 = this.DatabaseFactory.ObjectsDB.DoQueries(sql);
        //                foreach (EbDataRow dr in dt1.Tables[0].Rows)
        //                {
        //                    EbTable ebt = new EbTable
        //                    {
        //                        Id = Convert.ToInt32(dr[0]),
        //                        Name = dr[1].ToString()
        //                    };

        //                    tcol.Add(ebt.Id, ebt);
        //                }

        //                foreach (EbDataRow dr1 in dt1.Tables[1].Rows)
        //                {
        //                    EbTableColumn ebtc = new EbTableColumn
        //                    {
        //                        Type = (DbType)(dr1[2]),
        //                        Id = Convert.ToInt32(dr1[0]),
        //                        Name = dr1[1].ToString(),
        //                        TableId = Convert.ToInt32(dr1[3])
        //                    };
        //                    ccol.Add(ebtc.Name, ebtc);

        //                }

        //                redisClient.Set<EbTableCollection>(string.Format("EbTableCollection_{0}", this.ClientID), tcol);
        //                redisClient.Set<EbTableColumnCollection>(string.Format("EbTableColumnCollection_{0}", this.ClientID), ccol);
        //            }
        //        }
        //        else
        //        {
        //            EbTableCollection tcol = redisClient.Get<EbTableCollection>("EbInfraTableCollection");
        //            EbTableColumnCollection ccol = redisClient.Get<EbTableColumnCollection>("EbInfraTableColumnCollection");

        //            if (tcol == null || ccol == null)
        //            {
        //                tcol = new EbTableCollection();
        //                ccol = new EbTableColumnCollection();

        //                string sql = "SELECT id,tablename FROM eb_tables;" + "SELECT id,columnname,columntype FROM eb_tablecolumns;";
        //                var dt1 = this.DatabaseFactory.ObjectsDB.DoQueries(sql);

        //                foreach (EbDataRow dr in dt1.Tables[0].Rows)
        //                {
        //                    EbTable ebt = new EbTable
        //                    {
        //                        Id = Convert.ToInt32(dr[0]),
        //                        Name = dr[1].ToString()
        //                    };

        //                    tcol.Add(ebt.Id, ebt);
        //                }

        //                foreach (EbDataRow dr1 in dt1.Tables[1].Rows)
        //                {
        //                    EbTableColumn ebtc = new EbTableColumn
        //                    {
        //                        Type = (DbType)(dr1[2]),
        //                        Id = Convert.ToInt32(dr1[0]),
        //                        Name = dr1[1].ToString(),
        //                    };
        //                   ccol.Add(ebtc.Name, ebtc);

        //                }

        //                redisClient.Set<EbTableCollection>("EbInfraTableCollection", tcol);
        //                redisClient.Set<EbTableColumnCollection>("EbInfraTableColumnCollection", ccol);
        //            }
        //        }
        //    }
        //}
    }
}
