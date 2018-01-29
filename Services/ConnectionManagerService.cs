using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Common.Data.MongoDB;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Data.Common;

namespace ExpressBase.ServiceStack.Services
{
    public class ConnectionManager : EbBaseService
    {
        public ConnectionManager(ITenantDbFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _mqp, _mqc) { }

        [Authenticate]
        public GetConnectionsResponse Post(GetConnectionsRequest req)
        {
            GetConnectionsResponse resp = new GetConnectionsResponse();           
            resp.EBSolutionConnections = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", req.TenantAccountId));
            
            return resp;
        }

        [Authenticate]
        public void Post(InitialSolutionConnectionsRequest request)
        {
            EbSolutionConnections infraConnections = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", "expressbase"));

            infraConnections.DataDbConnection.DatabaseName = request.SolutionId;
            infraConnections.DataDbConnection.IsDefault = true;
            infraConnections.DataDbConnection.NickName = request.SolutionId + "_Initial";

            infraConnections.ObjectsDbConnection.DatabaseName = request.SolutionId;
            infraConnections.ObjectsDbConnection.IsDefault = true;
            infraConnections.ObjectsDbConnection.NickName = request.SolutionId + "_Initial";

            infraConnections.FilesDbConnection.IsDefault = true;
            infraConnections.FilesDbConnection.NickName = request.SolutionId + "_Initial";

            infraConnections.DataDbConnection.Persist(request.SolutionId, this.TenantDbFactory, true, request.UserId);
            infraConnections.ObjectsDbConnection.Persist(request.SolutionId, this.TenantDbFactory, true, request.UserId);
            infraConnections.FilesDbConnection.Persist(request.SolutionId, this.TenantDbFactory, true, request.UserId);

            this.Redis.Set<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", request.SolutionId), infraConnections);

            //EbDataDbConnection ebDataDbConnection = new EbDataDbConnection
            //{
            //    IsDefault = true,
            //    NickName = request.SolutionId + "_Initial",
            //    DatabaseVendor = infraConnections.DataDbConnection.DatabaseVendor,
            //    Port = infraConnections.DataDbConnection.Port,
            //    Server = infraConnections.DataDbConnection.Server,
            //    Timeout = infraConnections.DataDbConnection.Timeout,
            //    DatabaseName = request.SolutionId,
            //    UserName = infraConnections.DataDbConnection.UserName,
            //    Password = infraConnections.DataDbConnection.Password
            //};

            //EbObjectsDbConnection ebObjectDbConnection = new EbObjectsDbConnection
            //{
            //    IsDefault = true,
            //    NickName = request.SolutionId + "_Initial",
            //    DatabaseVendor = infraConnections.ObjectsDbConnection.DatabaseVendor,
            //    Port = infraConnections.ObjectsDbConnection.Port,
            //    Server = infraConnections.ObjectsDbConnection.Server,
            //    Timeout = infraConnections.ObjectsDbConnection.Timeout,
            //    DatabaseName = request.SolutionId,
            //    UserName = infraConnections.ObjectsDbConnection.UserName,
            //    Password = infraConnections.ObjectsDbConnection.Password
            //};

            //EbFilesDbConnection ebFileDbConnection = new EbFilesDbConnection
            //{
            //    IsDefault = true,
            //    NickName = request.SolutionId + "_Initial",
            //    FilesDB_url = infraConnections.FilesDbConnection.FilesDB_url,
            //};

            //ebDataDbConnection.Persist(request.SolutionId, this.TenantDbFactory, true);
            //ebObjectDbConnection.Persist(request.SolutionId, this.TenantDbFactory, true);
            //ebFileDbConnection.Persist(request.SolutionId, this.TenantDbFactory, true);

            //base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.SolutionId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeSMTPConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.SMTPConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew, request.UserId);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeDataDBConnectionRequest request)
        {           
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);            
            request.DataDBConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew, request.UserId);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeObjectsDBConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);
            request.ObjectsDBConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew, request.UserId);
            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeFilesDBConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.FilesDBConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew, request.UserId);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeSMSConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.SMSConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew, request.UserId);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        public TestConnectionResponse Post(TestConnectionRequest request)
        {
            TestConnectionResponse res = new TestConnectionResponse() { ConnectionStatus= true };

            IDatabase DataDB = null;
            if (request.DataDBConnection.DatabaseVendor == DatabaseVendors.PGSQL)
                DataDB = new PGSQLDatabase(request.DataDBConnection);
            else if (request.DataDBConnection.DatabaseVendor == DatabaseVendors.ORACLE)
                DataDB = new OracleDB(request.DataDBConnection);

            try
            {
                DataDB.DoNonQuery("CREATE TABLE eb_testConnection(id integer,connection_status text)");
            }
            catch(Exception e)
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
            catch(Exception e)
            {
                res.ConnectionStatus = false;
            }
            return res;
        }
    }
}
