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

            resp.EBSolutionConnections = new EbSolutionConnections();

            if (req.ConnectionType == 0)
            {
                EbSolutionConnections dummy = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", req.TenantAccountId));

                resp.EBSolutionConnections = dummy;

                if (resp.EBSolutionConnections.DataDbConnection != null)
                    resp.EBSolutionConnections.DataDbConnection.Password = "EBDummyPassword";

                if (resp.EBSolutionConnections.ObjectsDbConnection != null)
                    resp.EBSolutionConnections.ObjectsDbConnection.Password = "EBDummyPassword";

                if (resp.EBSolutionConnections.SMTPConnection != null)
                    resp.EBSolutionConnections.SMTPConnection.Password = "EBDummyPassword";
            }

            else if (req.ConnectionType == (int)EbConnectionTypes.SMTP)
            {
                EbSolutionConnections dummy = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", req.TenantAccountId));

                resp.EBSolutionConnections.SMTPConnection = new SMTPConnection();
                resp.EBSolutionConnections.SMTPConnection = dummy.SMTPConnection;

                if (resp.EBSolutionConnections.SMTPConnection != null)
                    resp.EBSolutionConnections.SMTPConnection.Password = "EBDummyPassword";
            }

            else if (req.ConnectionType == (int)EbConnectionTypes.EbDATA)
            {
                EbSolutionConnections dummy = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", req.TenantAccountId));

                resp.EBSolutionConnections.DataDbConnection = new EbDataDbConnection();
                resp.EBSolutionConnections.DataDbConnection = dummy.DataDbConnection;

                if (resp.EBSolutionConnections.DataDbConnection != null)
                    resp.EBSolutionConnections.DataDbConnection.Password = "EBDummyPassword";
            }

            else if (req.ConnectionType == (int)EbConnectionTypes.SMS)
            {
                EbSolutionConnections dummy = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", req.TenantAccountId));

                resp.EBSolutionConnections.SMSConnection = new SMSConnection();
                resp.EBSolutionConnections.SMSConnection = dummy.SMSConnection;

                if (resp.EBSolutionConnections.DataDbConnection != null)
                    resp.EBSolutionConnections.SMSConnection.Password = "EBDummyPassword";
            }
            
            return resp;
        }

        [Authenticate]
        public void Post(ChangeSMTPConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.SMTPConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequestTest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeDataDBConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.DataDBConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequestTest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeFilesDBConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.FilesDBConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequestTest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }

        [Authenticate]
        public void Post(ChangeSMSConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory("expressbase", this.Redis);

            request.SMSConnection.Persist(request.TenantAccountId, dbFactory, request.IsNew);

            base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequestTest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
        }
    }
}
