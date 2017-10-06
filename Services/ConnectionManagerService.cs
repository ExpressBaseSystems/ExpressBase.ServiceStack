using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
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

            //dummy.DataDbConnection.Password = "EBDummyPassword";
            //dummy.ObjectsDbConnection.Password = "EBDummyPassword";
            //dummy.EmailConnection.Password = "EBDummyPassword";

            //if (resp.EBSolutionConnections.EbTier == (EbTiers)Enum.Parse(typeof(EbTiers), "Unlimited"))
            //if (resp.EBSolutionConnections.EbTier == null)
            //{
            //resp.EBSolutionConnections.DataDbConnection = new EbDataDbConnection{ DatabaseName = dummy.DataDbConnection.DatabaseName };
            //resp.EBSolutionConnections.DataDbConnection = dummy.DataDbConnection;
            //resp.EBSolutionConnections.ObjectsDbConnection = dummy.ObjectsDbConnection;
            //resp.EBSolutionConnections.EmailConnection = dummy.EmailConnection;
            //}
            return resp;
        }

        [Authenticate]
        public void Post(ChangeSMTPConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory(request.TenantAccountId, this.Redis);

            if (request.IsNew)
            {
                using (var con = TenantDbFactory.DataDB.GetNewConnection())
                {
                    string sql = "INSERT INTO eb_connections (con_type, solution_id, nick_name, con_obj) VALUES (@con_type, @solution_id, @nick_name, @con_obj)";
                    DbParameter[] parameters = { this.TenantDbFactory.DataDB.GetNewParameter("con_type", System.Data.DbType.String, EbConnectionTypes.SMTP),
                            this.TenantDbFactory.DataDB.GetNewParameter("solution_id", System.Data.DbType.String, request.TenantAccountId),
                            this.TenantDbFactory.DataDB.GetNewParameter("nick_name", System.Data.DbType.String, request.SMTPConnection.NickName),
                            this.TenantDbFactory.DataDB.GetNewParameter("con_obj", NpgsqlTypes.NpgsqlDbType.Json,EbSerializers.Json_Serialize(request.SMTPConnection) )};
                    var iCount = TenantDbFactory.DataDB.DoNonQuery(sql, parameters);   
                }

                base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
            }

            else if (!request.IsNew)
            {
                EbSolutionConnections CurrentConnections = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", request.TenantAccountId));

                if (request.SMTPConnection.Password == "EBDummyPassword") { request.SMTPConnection.Password = CurrentConnections.SMTPConnection.Password; }

                if (request.SMTPConnection.ToJson() != CurrentConnections.SMTPConnection.ToJson())
                {
                    //    using (var con = TenantDbFactory.DataDB.GetNewConnection())
                    //    {
                    //        string sql = "INSERT INTO eb_connections (con_type, solution_id, nick_name, con_obj) VALUES (@con_type, @solution_id, @nick_name, @con_obj)";
                    //        DbParameter[] parameters = { this.TenantDbFactory.DataDB.GetNewParameter("con_type", System.Data.DbType.String, EbConnectionTypes.SMTP),
                    //            this.TenantDbFactory.DataDB.GetNewParameter("solution_id", System.Data.DbType.String, request.TenantAccountId),
                    //            this.TenantDbFactory.DataDB.GetNewParameter("nick_name", System.Data.DbType.String, request.SMTPConnection.NickName),
                    //            this.TenantDbFactory.DataDB.GetNewParameter("con_obj", NpgsqlTypes.NpgsqlDbType.Json,EbSerializers.Json_Serialize(request.SMTPConnection) )};
                    //        var iCount = TenantDbFactory.DataDB.DoNonQuery(sql, parameters);

                    base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
                }
            }

        }

        [Authenticate]
        public void Post(ChangeDataDBConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory(request.TenantAccountId, this.Redis);

            if (request.IsNew)
            {
                using (var con = TenantDbFactory.DataDB.GetNewConnection())
                {
                    string sql = "INSERT INTO eb_connections (con_type, solution_id, nick_name, con_obj) VALUES (@con_type, @solution_id, @nick_name, @con_obj)";
                    DbParameter[] parameters = { this.TenantDbFactory.DataDB.GetNewParameter("con_type", System.Data.DbType.String, EbConnectionTypes.EbDATA),
                            this.TenantDbFactory.DataDB.GetNewParameter("solution_id", System.Data.DbType.String, request.TenantAccountId),
                            this.TenantDbFactory.DataDB.GetNewParameter("nick_name", System.Data.DbType.String, request.DataDBConnection.DatabaseName),
                            this.TenantDbFactory.DataDB.GetNewParameter("con_obj", NpgsqlTypes.NpgsqlDbType.Json,EbSerializers.Json_Serialize(request.DataDBConnection) )};
                    var iCount = TenantDbFactory.DataDB.DoNonQuery(sql, parameters);
                }

                base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
            }

            else if (!request.IsNew)
            {
                EbSolutionConnections CurrentConnections = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", request.TenantAccountId));

                if (request.DataDBConnection.Password == "EBDummyPassword") { request.DataDBConnection.Password = CurrentConnections.DataDbConnection.Password; }

                if (request.DataDBConnection.ToJson() != CurrentConnections.DataDbConnection.ToJson())
                {
                    //using (var con = TenantDbFactory.DataDB.GetNewConnection())
                    //{
                    //    string sql = "INSERT INTO eb_connections (con_type, solution_id, nick_name, con_obj) VALUES (@con_type, @solution_id, @nick_name, @con_obj)";
                    //    DbParameter[] parameters = { this.TenantDbFactory.DataDB.GetNewParameter("con_type", System.Data.DbType.String, EbConnectionTypes.EbDATA),
                    //        this.TenantDbFactory.DataDB.GetNewParameter("solution_id", System.Data.DbType.String, request.TenantAccountId),
                    //        this.TenantDbFactory.DataDB.GetNewParameter("nick_name", System.Data.DbType.String, request.DataDBConnection.DatabaseName),
                    //        this.TenantDbFactory.DataDB.GetNewParameter("con_obj", NpgsqlTypes.NpgsqlDbType.Json,EbSerializers.Json_Serialize(request.DataDBConnection) )};
                    //    var iCount = TenantDbFactory.DataDB.DoNonQuery(sql, parameters);
                    //}

                    base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
                }
            }
        }

        [Authenticate]
        public void Post(ChangeFilesDBConnectionRequest request)
        {
            TenantDbFactory dbFactory = new TenantDbFactory(request.TenantAccountId, this.Redis);

            if (request.IsNew)
            {
                using (var con = TenantDbFactory.DataDB.GetNewConnection())
                {
                    string sql = "INSERT INTO eb_connections (con_type, solution_id, con_obj) VALUES (@con_type, @solution_id, @con_obj)";
                    DbParameter[] parameters = { this.TenantDbFactory.DataDB.GetNewParameter("con_type", System.Data.DbType.String, EbConnectionTypes.EbFILES),
                            this.TenantDbFactory.DataDB.GetNewParameter("solution_id", System.Data.DbType.String, request.TenantAccountId),
                            this.TenantDbFactory.DataDB.GetNewParameter("con_obj", NpgsqlTypes.NpgsqlDbType.Json,EbSerializers.Json_Serialize(request.FilesDBConnection) )};
                    var iCount = TenantDbFactory.DataDB.DoNonQuery(sql, parameters);
                }
                base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
            }

            else if (!request.IsNew)
            {
                EbSolutionConnections CurrentConnections = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", request.TenantAccountId));

                if (request.FilesDBConnection.ToJson() != CurrentConnections.FilesDbConnection.ToJson())
                {
                    //using (var con = TenantDbFactory.DataDB.GetNewConnection())
                    //{
                    //    string sql = "INSERT INTO eb_connections (con_type, solution_id, nick_name, con_obj) VALUES (@con_type, @solution_id, @nick_name, @con_obj)";
                    //    DbParameter[] parameters = { this.TenantDbFactory.DataDB.GetNewParameter("con_type", System.Data.DbType.String, EbConnectionTypes.EbDATA),
                    //        this.TenantDbFactory.DataDB.GetNewParameter("solution_id", System.Data.DbType.String, request.TenantAccountId),
                    //        this.TenantDbFactory.DataDB.GetNewParameter("nick_name", System.Data.DbType.String, request.DataDBConnection.DatabaseName),
                    //        this.TenantDbFactory.DataDB.GetNewParameter("con_obj", NpgsqlTypes.NpgsqlDbType.Json,EbSerializers.Json_Serialize(request.DataDBConnection) )};
                    //    var iCount = TenantDbFactory.DataDB.DoNonQuery(sql, parameters);
                    //}

                    base.MessageProducer3.Publish(new RefreshSolutionConnectionsMqRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId });
                }
            }


        }

        [Authenticate]
        public void Post(ChangeSMSConnectionRequest request)
        {
        }

    }
}
