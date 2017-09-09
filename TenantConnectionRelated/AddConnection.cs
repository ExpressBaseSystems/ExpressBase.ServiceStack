using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Objects.Objects.TenantConnectionsRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.TenantConnectionRelated
{
    public class AddConnection : EbBaseService
    {
        public AddConnection(ITenantDbFactory _dbf, IMessageQueueClient _mqc, IMessageProducer _mqp) : base(_dbf, _mqc, _mqp) { }

        [Authenticate]
        public bool Post(AddSMTPConnectionRequest req) {

            using (var con = TenantDbFactory.DataDB.GetNewConnection())
            {
                string sql = "INSERT INTO eb_connections (con_type, solution_id, nick_name, con_obj) VALUES (@con_type, @solution_id, @nick_name, @con_obj)";
                DbParameter[] parameters = { this.TenantDbFactory.DataDB.GetNewParameter("con_type", System.Data.DbType.String, EbConnectionTypes.Email),
                            this.TenantDbFactory.DataDB.GetNewParameter("solution_id", System.Data.DbType.String, req.TenantAccountId),
                            this.TenantDbFactory.DataDB.GetNewParameter("nick_name", System.Data.DbType.String, req.SMTPConnection.NickName),
                            this.TenantDbFactory.DataDB.GetNewParameter("con_obj", NpgsqlTypes.NpgsqlDbType.Json,EbSerializers.Json_Serialize(req.SMTPConnection) )};
                var iCount = TenantDbFactory.DataDB.DoNonQuery(sql,parameters);

                base.MessageProducer2.Publish(new RefreshSolutionConnectionsRequests() { TenantAccountId = req.TenantAccountId, UserId = req.UserId } );

                return (iCount > 0);
            }
        }
    }
}
