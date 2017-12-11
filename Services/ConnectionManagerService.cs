using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
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
        public void Post(ChangeSMTPConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.SMTPConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeDataDBConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.DataDBConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeObjectsDBConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.ObjectsDBConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeFilesDBConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.FilesDBConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeSMSConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.SMSConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }
    }
}
