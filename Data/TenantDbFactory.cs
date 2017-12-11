using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Common.Data.MongoDB;
using ExpressBase.Common.Messaging;
using ExpressBase.Common.Messaging.Twilio;
using ExpressBase.Data;
using Funq;
using ServiceStack;
using ServiceStack.Logging;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class TenantDbFactory : ITenantDbFactory
    {
        public IDatabase ObjectsDB { get; private set; }

        public IDatabase DataDB { get; private set; }

        public IDatabase DataDBRO { get; private set; }

        public INoSQLDatabase FilesDB { get; private set; }

        public IDatabase LogsDB { get; private set; }

        public ISMSService SMSService { get; private set; }

        private EbSolutionConnections _config { get; set; }

        private RedisClient Redis { get; set; }

        private string TenantId { get; set; }

        internal TenantDbFactory(string tenantId, IRedisClient redis)
        {
            this.TenantId = tenantId;

            ILog log = LogManager.GetLogger(GetType());

            log.Info("tdbfact"+ tenantId);
            if (tenantId != null)
            {
                _config = redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", tenantId));
                log.Info("tdbfact" + _config.ToString());
                InitDatabases();
            }
        }

        //Call from ServiceStack
        public TenantDbFactory(Container c)
        {
            this.Redis = c.Resolve<IRedisClientsManager>().GetClient() as RedisClient;

            this.TenantId = (HostContext.RequestContext.Items.Contains("TenantAccountId")) ? HostContext.RequestContext.Items["TenantAccountId"].ToString() : "expressbase"; // check the security issue

            if (this.TenantId != null)
            {
                _config = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", this.TenantId));

                InitDatabases();
            }
        }

        private void InitDatabases()
        {
            // CHECK IF CONNECTION IS LIVE

            if (_config.ObjectsDbConnection.DatabaseVendor == DatabaseVendors.PGSQL)
                ObjectsDB = new PGSQLDatabase(_config.ObjectsDbConnection);

            if (_config.DataDbConnection.DatabaseVendor == DatabaseVendors.PGSQL)
                DataDB = new PGSQLDatabase(_config.DataDbConnection);

            //To be Done
            if (_config.DataDbConnection.DatabaseVendor == DatabaseVendors.PGSQL)
                DataDBRO = new PGSQLDatabase(_config.DataDbConnection);

            FilesDB = new MongoDBDatabase(this.TenantId, _config.FilesDbConnection);

            LogsDB = new PGSQLDatabase(_config.LogsDbConnection);

            if(_config.SMSConnection != null )
                SMSService = new TwilioService(_config.SMSConnection);
        }
    }
}
