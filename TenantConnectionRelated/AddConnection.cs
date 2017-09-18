using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.Objects.TenantConnectionsRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System.Data.Common;

namespace ExpressBase.ServiceStack.TenantConnectionRelated
{
    public class AddConnection : EbBaseService
    {
        public AddConnection(ITenantDbFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _mqp, _mqc) { }

        [Authenticate]
        public bool Post(AddSMTPConnectionRequest req)
        {
            using (var con = TenantDbFactory.DataDB.GetNewConnection())
            {
                string sql = "INSERT INTO eb_connections (con_type, solution_id, nick_name, con_obj) VALUES (@con_type, @solution_id, @nick_name, @con_obj)";
                DbParameter[] parameters = { this.TenantDbFactory.DataDB.GetNewParameter("con_type", System.Data.DbType.String, EbConnectionTypes.Email),
                            this.TenantDbFactory.DataDB.GetNewParameter("solution_id", System.Data.DbType.String, req.TenantAccountId),
                            this.TenantDbFactory.DataDB.GetNewParameter("nick_name", System.Data.DbType.String, req.SMTPConnection.NickName),
                            this.TenantDbFactory.DataDB.GetNewParameter("con_obj", NpgsqlTypes.NpgsqlDbType.Json,EbSerializers.Json_Serialize(req.SMTPConnection) )};
                var iCount = TenantDbFactory.DataDB.DoNonQuery(sql, parameters);

                base.MessageProducer3.Publish(new RefreshSolutionConnectionsRequests() { TenantAccountId = req.TenantAccountId, UserId = req.UserId });

                return (iCount > 0);
            }
        }
    }
}
