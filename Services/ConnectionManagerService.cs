using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Security.Cryptography;

namespace ExpressBase.ServiceStack.Services
{
    public class ConnectionManager : EbBaseService
    {
        public ConnectionManager(ITenantDbFactory _dbf) : base(_dbf) { }
        public GetConnectionsResponse Post(GetConnectionsRequest req)
        {
            GetConnectionsResponse resp = new GetConnectionsResponse();

            EbSolutionConnections dummy = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}_test", req.TenantAccountId));

            dummy.DataDbConnection.Password = "EBDummyPassword";
            dummy.ObjectsDbConnection.Password = "EBDummyPassword";
            //dummy.EmailConnection.Password = "EBDummyPassword";

            //if (resp.EBSolutionConnections.EbTier == (EbTiers)Enum.Parse(typeof(EbTiers), "Unlimited"))
            //if (resp.EBSolutionConnections.EbTier == null)
            //{
            resp.EBSolutionConnections.DataDbConnection = new EbDataDbConnection{ DatabaseName = dummy.DataDbConnection.DatabaseName };
            resp.EBSolutionConnections.DataDbConnection = dummy.DataDbConnection;
            resp.EBSolutionConnections.ObjectsDbConnection = dummy.ObjectsDbConnection;
            //resp.EBSolutionConnections.EmailConnection = dummy.EmailConnection;
            //}
            return resp;
        }
    }
}
