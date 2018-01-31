using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Data.MongoDB;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;

namespace ExpressBase.ServiceStack.Services
{
    public class ConnectionManager : EbBaseService
    {
        public ConnectionManager(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _mqp, _mqc) { }

        [Authenticate]
        public GetConnectionsResponse Post(GetConnectionsRequest req)
        {
            GetConnectionsResponse resp = new GetConnectionsResponse();
            resp.EBSolutionConnections = this.Redis.Get<EbConnections>(string.Format(CoreConstants.SOLUTION_CONNECTION_REDIS_KEY, req.TenantAccountId));

            return resp;
        }

        [Authenticate]
        public void Post(InitialSolutionConnectionsRequest request)
        {
            EbConnections _solutionConnections = new EbConnections
            {
                ObjectsDbConnection = new EbObjectsDbConnection
                {
                    DatabaseVendor = DatabaseVendors.PGSQL,
                    Server = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_SERVER),
                    Port = Convert.ToInt16(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_PORT)),
                    DatabaseName = request.SolutionId,
                    IsDefault = true,
                    NickName = request.SolutionId + "_Initial",
                    UserName = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER), // CREATE NEW USER FOR SOLUTION
                    Password = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_PASSWORD), // CREATE NEW PASS FOR SOLUTION
                    Timeout = Convert.ToInt16(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_TIMEOUT))
                },
                DataDbConnection = new EbDataDbConnection
                {
                    DatabaseVendor = DatabaseVendors.PGSQL,
                    Server = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_SERVER),
                    Port = Convert.ToInt16(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_PORT)),
                    DatabaseName = request.SolutionId,
                    IsDefault = true,
                    NickName = request.SolutionId + "_Initial",
                    UserName = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER), // CREATE NEW USER FOR SOLUTION
                    Password = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_PASSWORD), // CREATE NEW PASS FOR SOLUTION
                    Timeout = Convert.ToInt16(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_TIMEOUT))
                },
                //To Be Deleted
                FilesDbConnection = new EbFilesDbConnection
                {
                    FilesDbVendor = FilesDbVendors.MongoDB,
                    IsDefault = true,
                    NickName = request.SolutionId + "_Initial",
                    FilesDB_url = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_INFRA_FILES_DB_URL)
                }
            };

            _solutionConnections.ObjectsDbConnection.Persist(request.SolutionId, this.EbConnectionFactory, true, request.UserId);
            _solutionConnections.DataDbConnection.Persist(request.SolutionId, this.EbConnectionFactory, true, request.UserId);
            _solutionConnections.FilesDbConnection.Persist(request.SolutionId, this.EbConnectionFactory, true, request.UserId);

            this.Redis.Set<EbConnections>(string.Format(CoreConstants.SOLUTION_CONNECTION_REDIS_KEY, request.SolutionId), _solutionConnections);
        }

        [Authenticate]
        public void Post(ChangeSMTPConnectionRequest request)
        {
            request.SMTPConnection.Persist(request.TenantAccountId, this.InfraConnectionFactory, request.IsNew, request.UserId);
            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeDataDBConnectionRequest request)
        {
            request.DataDBConnection.Persist(request.TenantAccountId, this.InfraConnectionFactory, request.IsNew, request.UserId);
            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeObjectsDBConnectionRequest request)
        {
            request.ObjectsDBConnection.Persist(request.TenantAccountId, this.InfraConnectionFactory, request.IsNew, request.UserId);
            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeFilesDBConnectionRequest request)
        {
            request.FilesDBConnection.Persist(request.TenantAccountId, this.InfraConnectionFactory, request.IsNew, request.UserId);
            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeSMSConnectionRequest request)
        {
            request.SMSConnection.Persist(request.TenantAccountId, this.InfraConnectionFactory, request.IsNew, request.UserId);
            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        public TestConnectionResponse Post(TestConnectionRequest request)
        {
            TestConnectionResponse res = new TestConnectionResponse() { ConnectionStatus = true };

            IDatabase DataDB = null;
            if (request.DataDBConnection.DatabaseVendor == DatabaseVendors.PGSQL)
                DataDB = new PGSQLDatabase(request.DataDBConnection);
            else if (request.DataDBConnection.DatabaseVendor == DatabaseVendors.ORACLE)
                DataDB = new OracleDB(request.DataDBConnection);

            try
            {
                DataDB.DoNonQuery("CREATE TABLE eb_testConnection(id integer,connection_status text)");
            }
            catch (Exception e)
            {
                res.ConnectionStatus = false;
            }

            return res;
        }

        public TestFileDbconnectionResponse Post(TestFileDbconnectionRequest request)
        {
            TestFileDbconnectionResponse res = new TestFileDbconnectionResponse();
            try
            {
                MongoDBDatabase mongo = new MongoDBDatabase(request.UserId.ToString(), request.FilesDBConnection);
                res.ConnectionStatus = true;
            }
            catch (Exception e)
            {
                res.ConnectionStatus = false;
            }
            return res;
        }
    }
}
