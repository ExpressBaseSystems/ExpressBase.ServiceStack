using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;

namespace ExpressBase.ServiceStack.Services
{
    public class ConnectionManager : EbBaseService
    {
        public ConnectionManager(ITenantDbFactory _dbf) : base(_dbf) { }
        public EbSolutionConnections Post(GetConnectionsRequest req)
        {
            EbSolutionConnections conn = new EbSolutionConnections();
            conn = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolCon22222_{0}", req.TenantAccountId));
            return conn;
        }
    }
}
