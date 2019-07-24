using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Data.MongoDB;
using ExpressBase.Common.Messaging;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace ExpressBase.ServiceStack.Services
{
    public class ConnectionManager : EbBaseService
    {
        public ConnectionManager(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc, IEbMqClient _mq) : base(_dbf, _mqp, _mqc, _mq)
        {
        }

        [Authenticate]
        public RefreshSolutionConnectionsAsyncResponse Post(RefreshSolutionConnectionsBySolutionIdAsyncRequest request)
        {
            RefreshSolutionConnectionsAsyncResponse res = new RefreshSolutionConnectionsAsyncResponse();
            try
            {
                res = this.MQClient.Post<RefreshSolutionConnectionsAsyncResponse>(new RefreshSolutionConnectionsBySolutionIdAsyncRequest() { SolutionId = request.SolutionId, UserId = request.UserId });
                return res;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception:" + e.ToString());
                res.ResponseStatus.Message = e.Message;
                return res;
            }
        }

        [Authenticate]
        public GetConnectionsResponse Post(GetConnectionsRequest req)
        {
            GetConnectionsResponse resp = new GetConnectionsResponse();
            resp.EBSolutionConnections = this.Redis.Get<EbConnectionsConfig>(string.Format(CoreConstants.SOLUTION_INTEGRATION_REDIS_KEY, req.SolutionId));
            // if (resp.EBSolutionConnections == null)
            //using (var con = this.InfraConnectionFactory.DataDB.GetNewConnection() as Npgsql.NpgsqlConnection)
            //{
            //    con.Open();
            //    string sql = @"SELECT id, con_type, con_obj FROM eb_connections WHERE solution_id = @solution_id AND eb_del = 'F'";
            //    DataTable dt = new DataTable();
            //    EbConnectionsConfig cons = new EbConnectionsConfig();

            //    var ada = new Npgsql.NpgsqlDataAdapter(sql, con);
            //    ada.SelectCommand.Parameters.Add(new Npgsql.NpgsqlParameter("solution_id", NpgsqlTypes.NpgsqlDbType.Text) { Value = req.SolutionId });
            //    ada.Fill(dt);

            //    if (dt.Rows.Count != 0)
            //    {
            //        EbSmsConCollection _smscollection = new EbSmsConCollection();
            //        EbMailConCollection _mailcollection = new EbMailConCollection();
            //        foreach (DataRow dr in dt.Rows)
            //        {
            //            if (dr["con_type"].ToString() == EbConnectionTypes.EbDATA.ToString())
            //            {
            //                cons.DataDbConfig = EbSerializers.Json_Deserialize<EbDataDbConnection>(dr["con_obj"].ToString());
            //                cons.DataDbConfig.Id = (int)dr["id"];
            //            }
            //            else if (dr["con_type"].ToString() == EbConnectionTypes.EbDATA_RO.ToString())
            //            {
            //                cons.DataDbConfig = EbSerializers.Json_Deserialize<EbDataDbConnection>(dr["con_obj"].ToString());
            //                cons.DataDbConfig.Id = (int)dr["id"];
            //            }
            //            else if (dr["con_type"].ToString() == EbConnectionTypes.EbOBJECTS.ToString())
            //            {
            //                cons.ObjectsDbConfig = EbSerializers.Json_Deserialize<EbObjectsDbConnection>(dr["con_obj"].ToString());
            //                cons.ObjectsDbConfig.Id = (int)dr["id"];
            //            }
            //            //else if (dr["con_type"].ToString() == EbConnectionTypes.EbFILES.ToString())
            //            //    cons.FilesDbConnection = EbSerializers.Json_Deserialize<EbFilesDbConnection>(dr["con_obj"].ToString());
            //            else if (dr["con_type"].ToString() == EbConnectionTypes.EbLOGS.ToString())
            //            {
            //                cons.LogsDbConnection = EbSerializers.Json_Deserialize<EbLogsDbConnection>(dr["con_obj"].ToString());
            //                cons.LogsDbConnection.Id = (int)dr["id"];
            //            }
            //            else if (dr["con_type"].ToString() == EbConnectionTypes.SMTP.ToString())
            //            {
            //                EbEmail temp = EbSerializers.Json_Deserialize<EbEmail>(dr["con_obj"].ToString());
            //                temp.Id = (int)dr["id"];
            //                _mailcollection.Add(temp);
            //            }
            //            else if (dr["con_type"].ToString() == EbConnectionTypes.SMS.ToString())
            //            {
            //                ISMSConnection temp = EbSerializers.Json_Deserialize<ISMSConnection>(dr["con_obj"].ToString());
            //                temp.Id = (int)dr["id"];
            //                _smscollection.Add(temp);
            //            }
            //            else if (dr["con_type"].ToString() == EbConnectionTypes.Cloudinary.ToString())
            //            {
            //                cons.CloudinaryConnection = EbSerializers.Json_Deserialize<EbCloudinaryConnection>(dr["con_obj"].ToString());
            //                cons.CloudinaryConnection.Id = (int)dr["id"];
            //            }
            //            else if (dr["con_type"].ToString() == EbConnectionTypes.FTP.ToString())
            //            {
            //                cons.FTPConnection = EbSerializers.Json_Deserialize<EbFTPConnection>(dr["con_obj"].ToString());
            //                cons.FTPConnection.Id = (int)dr["id"];
            //            }// ... More to come
            //        }
            //        cons.SMSConnections = _smscollection;
            //        cons.EmailConnections = _mailcollection;
            //        Redis.Set<EbConnectionsConfig>(string.Format(CoreConstants.SOLUTION_CONNECTION_REDIS_KEY, req.SolutionId), cons);
            //        resp.EBSolutionConnections = cons;
            //    }
            //}
            return resp;
        }

        [Authenticate]
        public void Post(InitialSolutionConnectionsRequest request)
        {
            EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();


            _solutionConnections.DataDbConfig.DatabaseName = request.NewSolnId;
            _solutionConnections.DataDbConfig.NickName = request.NewSolnId + "_Initial";
            _solutionConnections.DataDbConfig.UserName = request.DbUsers.AdminUserName;
            _solutionConnections.DataDbConfig.Password = request.DbUsers.AdminPassword;
            _solutionConnections.DataDbConfig.ReadOnlyUserName = (request.DbUsers.ReadOnlyUserName != String.Empty) ? request.DbUsers.ReadOnlyUserName : request.DbUsers.AdminUserName;
            _solutionConnections.DataDbConfig.ReadOnlyPassword = (request.DbUsers.ReadOnlyPassword != string.Empty) ? request.DbUsers.ReadOnlyPassword : request.DbUsers.AdminPassword;
            _solutionConnections.DataDbConfig.ReadWriteUserName = (request.DbUsers.ReadWriteUserName != string.Empty) ? request.DbUsers.ReadWriteUserName : request.DbUsers.AdminUserName;
            _solutionConnections.DataDbConfig.ReadWritePassword = (request.DbUsers.ReadWritePassword != string.Empty) ? request.DbUsers.ReadWritePassword : request.DbUsers.AdminPassword;

            int confid = _solutionConnections.DataDbConfig.PersistIntegrationConf(request.NewSolnId, this.InfraConnectionFactory, request.UserId);
            EbIntegration _obj = new EbIntegration
            {
                Id = 0,
                ConfigId = confid,
                Preference = ConPreferences.PRIMARY,
                Type = EbConnectionTypes.EbDATA
            };
            int conid = _obj.PersistIntegration(request.NewSolnId, this.InfraConnectionFactory, request.UserId);
            this.Redis.Set<EbConnectionsConfig>(string.Format(CoreConstants.SOLUTION_INTEGRATION_REDIS_KEY, request.NewSolnId), _solutionConnections);
        }

        //[Authenticate]
        //public void Post(ChangeSMTPConnectionRequest request)
        //{
        //    request.Email.Persist(request.SolutionId, this.InfraConnectionFactory, request.IsNew, request.UserId);
        //    base.MessageProducer3.Publish(new RefreshSolutionConnectionsRequest() { SolnId = request.SolutionId, UserId = request.UserId });
        //}

        //[Authenticate]
        //public ChangeConnectionResponse Post(ChangeDataDBConnectionRequest request)
        //{
        //    ChangeConnectionResponse res = new ChangeConnectionResponse();
        //    try
        //    {
        //        request.DataDBConnection.Persist(request.SolutionId, this.InfraConnectionFactory, request.IsNew, request.UserId);

        //        var myService = base.ResolveService<EbDbCreateServices>();
        //        var result = myService.Post(new EbDbCreateRequest() { dbName = request.DataDBConnection.DatabaseName, ischange = true, DataDBConnection = request.DataDBConnection, UserId = request.UserId, SolnId = request.SolutionId });
        //        base.MessageProducer3.Publish(new RefreshSolutionConnectionsRequest()
        //        {
        //            SolnId = request.SolutionId,
        //            UserId = request.UserId,
        //            BToken = (!String.IsNullOrEmpty(this.Request.Authorization)) ? this.Request.Authorization.Replace("Bearer", string.Empty).Trim() : String.Empty,
        //            RToken = (!String.IsNullOrEmpty(this.Request.Headers["rToken"])) ? this.Request.Headers["rToken"] : String.Empty
        //        });
        //    }
        //    catch (Exception e)
        //    {
        //        res.ResponseStatus.Message = e.Message;
        //    }

        //    return res;
        //}

        //[Authenticate]
        //public ChangeConnectionResponse Post(ChangeCloudinaryConnectionRequest request)
        //{
        //    ChangeConnectionResponse res = new ChangeConnectionResponse();
        //    try
        //    {
        //        request.ImageManipulateConnection.Persist(request.SolutionId, this.InfraConnectionFactory, request.IsNew, request.UserId);

        //        base.MessageProducer3.Publish(new RefreshSolutionConnectionsRequest()
        //        {
        //            SolnId = request.SolutionId,
        //            UserId = request.UserId,
        //            BToken = (!String.IsNullOrEmpty(this.Request.Authorization)) ? this.Request.Authorization.Replace("Bearer", string.Empty).Trim() : String.Empty,
        //            RToken = (!String.IsNullOrEmpty(this.Request.Headers["rToken"])) ? this.Request.Headers["rToken"] : String.Empty
        //        });
        //    }
        //    catch (Exception e)
        //    {
        //        res.ResponseStatus.Message = e.Message;
        //    }

        //    return res;
        //}

        //[Authenticate]
        //public ChangeConnectionResponse Post(ChangeObjectsDBConnectionRequest request)
        //{
        //    ChangeConnectionResponse res = new ChangeConnectionResponse();
        //    try
        //    {
        //        request.ObjectsDBConnection.Persist(request.SolutionId, this.InfraConnectionFactory, request.IsNew, request.UserId);
        //        base.MessageProducer3.Publish(new RefreshSolutionConnectionsRequest() { SolnId = request.SolutionId, UserId = request.UserId });
        //    }
        //    catch (Exception e)
        //    {
        //        res.ResponseStatus.Message = e.Message;
        //    }
        //    return res;
        //}

        //[Authenticate]
        //public ChangeConnectionResponse Post(ChangeFilesDBConnectionRequest request)
        //{
        //    ChangeConnectionResponse res = new ChangeConnectionResponse();
        //    try
        //    {
        //        request.FilesDBConnection.Persist(request.SolnId, this.InfraConnectionFactory, request.IsNew, request.UserId);
        //        base.MessageProducer3.Publish(new RefreshSolutionConnectionsRequest() { SolnId = request.SolnId, UserId = request.UserId });
        //    }
        //    catch (Exception e)
        //    {
        //        res.ResponseStatus.Message = e.Message;
        //    }
        //    return res;
        //}

        //[Authenticate]
        //public ChangeConnectionResponse Post(ChangeFTPConnectionRequest request)
        //{
        //    ChangeConnectionResponse res = new ChangeConnectionResponse();
        //    try
        //    {
        //        request.FTPConnection.Persist(request.SolutionId, this.InfraConnectionFactory, request.IsNew, request.UserId);
        //        base.MessageProducer3.Publish(new RefreshSolutionConnectionsRequest()
        //        {
        //            SolnId = request.SolutionId,
        //            UserId = request.UserId,
        //            BToken = (!String.IsNullOrEmpty(this.Request.Authorization)) ? this.Request.Authorization.Replace("Bearer", string.Empty).Trim() : String.Empty,
        //            RToken = (!String.IsNullOrEmpty(this.Request.Headers["rToken"])) ? this.Request.Headers["rToken"] : String.Empty
        //        });
        //    }
        //    catch (Exception e)
        //    {
        //        res.ResponseStatus.Message = e.Message;
        //    }
        //    return res;
        //}

        //[Authenticate]
        //public ChangeConnectionResponse Post(ChangeSMSConnectionRequest request)
        //{
        //    ChangeConnectionResponse res = new ChangeConnectionResponse();
        //    try
        //    {
        //        request.SMSConnection.Persist(request.SolutionId, this.InfraConnectionFactory, request.IsNew, request.UserId);
        //        base.MessageProducer3.Publish(new RefreshSolutionConnectionsRequest() { SolnId = request.SolutionId, UserId = request.UserId });
        //    }
        //    catch (Exception e)
        //    {
        //        res.ResponseStatus.Message = e.Message;
        //    }
        //    return res;
        //}

        public TestConnectionResponse Post(TestConnectionRequest request)
        {
            TestConnectionResponse res = new TestConnectionResponse();
            bool IsAdmin = false;
            IDatabase DataDB = null;
            if (request.DataDBConfig.DatabaseVendor == DatabaseVendors.PGSQL)
                DataDB = new PGSQLDatabase(request.DataDBConfig);
            else if (request.DataDBConfig.DatabaseVendor == DatabaseVendors.ORACLE)
                DataDB = new OracleDB(request.DataDBConfig);
            else if (request.DataDBConfig.DatabaseVendor == DatabaseVendors.MYSQL)
                DataDB = new MySqlDB(request.DataDBConfig);

            try
            {
                var dt = DataDB.DoQuery(DataDB.EB_USER_ROLE_PRIVS.Replace("@uname", request.DataDBConfig.UserName));

                if (request.DataDBConfig.DatabaseVendor == DatabaseVendors.PGSQL)
                {
                    string[] adminroles = Enum.GetNames(typeof(PGSQLSysRoles));
                    List<string> adroleslist = adminroles.OfType<string>().ToList();
                    foreach (var dr in dt.Rows)
                    {
                        if (adroleslist.Contains(dr[0]))     //IsAdmin = (adroleslist.Contains(dr[0])) ? true : false;
                            IsAdmin = true;
                        else
                        {
                            IsAdmin = false;
                            break;
                        }
                    }
                    res.ConnectionStatus = IsAdmin;

                }
                else if (request.DataDBConfig.DatabaseVendor == DatabaseVendors.ORACLE)
                {
                    string[] adminroles = Enum.GetNames(typeof(OracleSysRoles));
                    List<string> adroleslist = adminroles.OfType<string>().ToList();
                    foreach (var dr in dt.Rows)
                    {
                        if (adroleslist.Contains(dr[0]))
                            IsAdmin = true;
                        else
                        {
                            IsAdmin = false;
                            break;
                        }
                    }
                    res.ConnectionStatus = IsAdmin;

                }

                else if (request.DataDBConfig.DatabaseVendor == DatabaseVendors.MYSQL)
                {
                    string[] adminroles = Enum.GetNames(typeof(MySqlSysRoles));
                    List<string> adroleslist = adminroles.OfType<string>().ToList();
                    List<string> adroleslistv1 = Enum.GetNames(typeof(MySqlSysRolesv1)).ToList();
                    adroleslist = adroleslist.ConvertAll(s => s.Replace("_", " "));
                    foreach (var dr in dt.Rows)
                    {
                        if (adroleslist.Contains(dr[0]) || adroleslistv1.Contains(dr[0]))
                            IsAdmin = true;
                        else
                        {
                            IsAdmin = false;
                            break;
                        }
                    }
                    res.ConnectionStatus = IsAdmin;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception:" + e.ToString());
                res.ConnectionStatus = IsAdmin;
            }
            return res;
        }

        //public TestFileDbconnectionResponse Post(TestFileDbconnectionRequest request)
        //{
        //    TestFileDbconnectionResponse res = new TestFileDbconnectionResponse();
        //    try
        //    {
        //        MongoDBDatabase mongo = new MongoDBDatabase(request.UserId.ToString(), request.FilesDBConnection);
        //        res.ConnectionStatus = true;
        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine("Exception:" + e.ToString());
        //        res.ConnectionStatus = false;
        //    }
        //    return res;
        //}

        //--------------------------------------------------------------------------------Integrations-----------------------------------------------------
        public AddDBResponse Post(AddDBRequest request)
        {
            AddDBResponse res = new AddDBResponse();
            try
            {
                res.nid = request.DbConfig.PersistIntegrationConf(request.SolnId, this.InfraConnectionFactory/*, request.IsNew*/, request.UserId);

            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }

        public AddTwilioResponse Post(AddTwilioRequest request)
        {
            AddTwilioResponse res = new AddTwilioResponse();
            try
            {
                request.Config.PersistIntegrationConf(request.SolnId, this.InfraConnectionFactory, /*request.IsNew,*/ request.UserId);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }

        public AddETResponse Post(AddETRequest request)
        {
            AddETResponse res = new AddETResponse();
            try
            {
                request.Config.PersistIntegrationConf(request.SolnId, this.InfraConnectionFactory, request.UserId);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }

        public AddMongoResponse Post(AddMongoRequest request)
        {
            AddMongoResponse res = new AddMongoResponse();
            try
            {
                request.Config.PersistIntegrationConf(request.SolnId, this.InfraConnectionFactory, request.UserId);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }
        public AddSmtpResponse Post(AddSmtpRequest request)
        {
            AddSmtpResponse res = new AddSmtpResponse();
            try
            {
                request.Config.PersistIntegrationConf(request.SolnId, this.InfraConnectionFactory, request.UserId);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }
        public AddCloudinaryResponse Post(AddCloudinaryRequest request)
        {
            AddCloudinaryResponse res = new AddCloudinaryResponse();
            try
            {
                request.Config.PersistIntegrationConf(request.SolnId, this.InfraConnectionFactory, request.UserId);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }

        public AddGoogleMapResponse Post(AddGoogleMapRequest request)
        {
            AddGoogleMapResponse res = new AddGoogleMapResponse();
            try
            {
                request.Config.PersistIntegrationConf(request.SolnId, this.InfraConnectionFactory, request.UserId);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }
        public AddSendGridResponse Post(AddSendGridRequest request)
        {
            AddSendGridResponse res = new AddSendGridResponse();
            try
            {
                request.Config.PersistIntegrationConf(request.SolnId, this.InfraConnectionFactory, request.UserId);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }
        
        public GetIntegrationConfigsResponse Get(GetIntegrationConfigsRequest request)
        {
            GetIntegrationConfigsResponse res = new GetIntegrationConfigsResponse();
            try
            {
                string sql = "SELECT * FROM eb_integration_configs where solution_id = @solution_id";
                DbParameter[] parameters = new DbParameter[] { this.InfraConnectionFactory.DataDB.GetNewParameter("solution_id", EbDbTypes.String, request.SolnId) };
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }

        public GetIntegrationsResponse Get(GetIntegrationsRequest request)
        {
            GetIntegrationsResponse res = new GetIntegrationsResponse();
            try
            {
                string sql = "SELECT * FROM eb_integrations where solution_id = @solution_id";
                DbParameter[] parameters = new DbParameter[] { this.InfraConnectionFactory.DataDB.GetNewParameter("solution_id", EbDbTypes.String, request.SolnId) };
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }

        public EbIntegrationResponse Post(EbIntegrationRequest request)
        {
            EbIntegrationResponse res = new EbIntegrationResponse();
            try
            {
                int flag = 0;
                if (request.IntegrationO.Type.ToString() == "EbDATA" || request.IntegrationO.Type.ToString()== "EbOBJECTS")
                {
                    string sql = "SELECT * FROM eb_integrations WHERE type = @type AND eb_del ='F' AND solution_id = @soluid;";
                    DbParameter[] parameters = {
                                                this.EbConnectionFactory.DataDB.GetNewParameter("type", EbDbTypes.String, request.IntegrationO.Type.ToString()),
                                                this.EbConnectionFactory.DataDB.GetNewParameter("soluid", EbDbTypes.String, request.SolnId.ToString())
                                           };
                    EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
                    if (dt.Rows.Count() > 0)
                        flag = 1;
                }
                
                if (flag == 0)
                {
                    request.IntegrationO.PersistIntegration(request.SolnId, this.InfraConnectionFactory, request.UserId);
                    if (request.IntegrationO.Type == EbConnectionTypes.EbDATA && request.deploy == true)
                    {
                        InitializeDataDb(request.IntegrationO.ConfigId, request.SolnId, request.UserId);
                    }
                    else
                    {
                        RefreshSolutionConnectionsAsyncResponse resp = this.MQClient.Post<RefreshSolutionConnectionsAsyncResponse>(new RefreshSolutionConnectionsBySolutionIdAsyncRequest()
                        {
                            SolutionId = request.SolnId
                        });
                    }
                }               
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }

        public EbIntegrationConfDeleteResponse Post(EbIntergationConfDeleteRequest request)
        {
            EbIntegrationConfDeleteResponse res = new EbIntegrationConfDeleteResponse();
            try
            {
                request.IntegrationConfdelete.PersistConfDeleteIntegration(request.SolnId, this.InfraConnectionFactory, request.UserId);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }

        public EbIntegrationDeleteResponse Post(EbIntergationDeleteRequest request)
        {
            EbIntegrationDeleteResponse res = new EbIntegrationDeleteResponse();
            try
            {
                request.Integrationdelete.PersistDeleteIntegration(request.SolnId, this.InfraConnectionFactory, request.UserId);
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }

        public EbIntegrationSwitchResponse Post(EbIntergationSwitchRequest request)
        {
            EbIntegrationSwitchResponse res = new EbIntegrationSwitchResponse();
            try
            {
                foreach (var ob in request.Integrations)
                {
                    EbIntegration obj = new EbIntegration
                    {
                        Id = Convert.ToInt32(ob.Id),
                        ConfigId = Convert.ToInt32(ob.ConfigId),
                        Preference = Enum.Parse<ConPreferences>(ob.Preference.ToString()),
                        Type = Enum.Parse<EbConnectionTypes>(ob.Type.ToString())
                    };
                    EbIntegrationRequest _obj = new EbIntegrationRequest { IntegrationO = obj };
                    _obj.IntegrationO.PersistIntegration(request.SolnId, this.InfraConnectionFactory, request.UserId);
                    RefreshSolutionConnectionsAsyncResponse resp = this.MQClient.Post<RefreshSolutionConnectionsAsyncResponse>(new RefreshSolutionConnectionsBySolutionIdAsyncRequest()
                    {
                        SolutionId = request.SolnId
                    });

                }
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
            }
            return res;
        }





        public void InitializeDataDb(int confid, string solid, int uid)
        {
            try
            {
                EbDbCreateServices _dbService = base.ResolveService<EbDbCreateServices>();
                TenantUserServices _tenantUserService = base.ResolveService<TenantUserServices>();

                string query = string.Format("SELECT * FROM eb_integration_configs where id ={0};", confid);
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(query);

                EbIntegrationConf conf = EbSerializers.Json_Deserialize(dt.Rows[0][4].ToString());

                EbDbCreateResponse response = _dbService.Post(new EbDbCreateRequest { DataDBConfig = conf as EbDbConfig, SolnId = solid, UserId = uid, IsChange = true });
                if (response.Resp)
                {
                    //Post(new InitialSolutionConnectionsRequest { NewSolnId = DbName, SolnId = request.SolnId, UserId = request.UserId, DbUsers = response.dbusers });
                    _tenantUserService.Post(new UpdateSolutionRequest() { UserId = uid, SolnId = solid, });
                    RefreshSolutionConnectionsAsyncResponse res = this.MQClient.Post<RefreshSolutionConnectionsAsyncResponse>(new RefreshSolutionConnectionsBySolutionIdAsyncRequest()
                    {
                        SolutionId = solid
                    });
                }
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
        }

        public CredientialBotResponse Get(CredientialBotRequest request)
        {
            CredientialBotResponse response = new CredientialBotResponse();
            string sql = string.Format(@"SELECT con_obj from eb_integration_configs WHERE id = {0} AND solution_id = '{1}' AND eb_del = 'F';
                                         SELECT pricing_tier FROM eb_solutions WHERE isolution_id='{1}'  ", request.ConfId, request.SolnId);
            try
            {
                EbDataSet dt = this.InfraConnectionFactory.DataDB.DoQueries(sql);
                EbDataTable _temp = dt.Tables[0];
                string ConnObj = _temp.Rows[0][0].ToString();
                _temp = dt.Tables[1];
                int pricing_tier = Convert.ToInt32(_temp.Rows[0][0]);
                EbIntegrationConf conobject = JsonConvert.DeserializeObject<EbIntegrationConf>(ConnObj);
                if (conobject.IsDefault == true && pricing_tier == 0 && conobject.Type == EbIntegrations.PGSQL)
                {
                    response.ResponseStatus = new ResponseStatus { Message = "Its a free account." };
                }
                else
                {
                    response.ConnObj = ConnObj;
                }
            }
            catch(Exception e)
             {
                Console.WriteLine(e.Message);
                response.ResponseStatus = new ResponseStatus { Message = e.Message };
            }
            return response;
        }

        public GetSolutioInfoResponses Get(GetSolutioInfoRequests request)
        {
            GetSolutioInfoResponses resp = new GetSolutioInfoResponses();

            string sql = string.Format(@"SELECT * FROM eb_solutions WHERE isolution_id='{0}';
                SELECT * FROM eb_integration_configs WHERE solution_id = '{0}' AND eb_del = 'F';
                SELECT * FROM
                    eb_integration_configs EC,
                    eb_integrations EI 
                where 
                     EC.id = EI.eb_integration_conf_id AND 
                     EC.solution_id = '{0}' AND
                     EI.solution_id = '{0}' 
                    AND EI.eb_del = 'F' AND EC.eb_del = 'F';", request.IsolutionId);
            try
            {
                EbDataSet dt = this.InfraConnectionFactory.DataDB.DoQueries(sql);
                EbDataTable _temp = dt.Tables[0];
                if (_temp != null)
                {
                    resp.SolutionInfo = new EbSolutionsWrapper
                    {
                        SolutionName = _temp.Rows[0]["solution_name"].ToString(),
                        Description = _temp.Rows[0]["description"].ToString(),
                        DateCreated = _temp.Rows[0]["date_created"].ToString(),
                        EsolutionId = _temp.Rows[0]["esolution_id"].ToString(),
                        PricingTier = Enum.Parse <PricingTiers>(_temp.Rows[0]["pricing_tier"].ToString()),
                        IsVersioningEnabled = Convert.ToBoolean(_temp.Rows[0]["versioning"])
                    };

                    _temp = dt.Tables[1];
                    Dictionary<string, List<EbIntegrationConfData>> _conf = new Dictionary<string, List<EbIntegrationConfData>>();
                    foreach (var _row in _temp.Rows)
                    {
                        string type = _row[3].ToString();
                        if (!_conf.ContainsKey(type))
                            _conf.Add(type, new List<EbIntegrationConfData>());
                        _conf[type].Add(new EbIntegrationConfData(_row));
                    }
                    resp.IntegrationsConfig = _conf;
                    _temp = dt.Tables[2];
                    Dictionary<string, List<EbIntegrationData>> _intgre = new Dictionary<string, List<EbIntegrationData>>();
                    foreach (var _row in _temp.Rows)
                    {
                        string Type = _row[12].ToString();
                        if (!_intgre.ContainsKey(Type))
                            _intgre.Add(Type, new List<EbIntegrationData>());
                        _intgre[Type].Add(new EbIntegrationData(_row));
                    }
                    resp.Integrations = _intgre;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return resp;
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------
        //--------------------------------------------------------------------------------------------------------------------------------------------------------
        //public _GetConectionsResponse Get(_GetConectionsRequest request)
        //{
        //    _GetConectionsResponse res = new _GetConectionsResponse();
        //    int migrated = 0;
        //    try
        //    {
        //        string sql = @"SELECT * FROM eb_connections WHERE eb_del='F' AND
        //                        con_type  in ( 'Cloudinary','EbDATA' ,'SMTP');";
        //        EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);
        //        int return_id = -1;
        //        if (dt.Rows.Count > 0)
        //        {
        //            foreach (EbDataRow dr in dt.Rows)
        //            {
        //                migrated++;
        //                if (!string.IsNullOrEmpty(dr["con_obj"].ToString()))
        //                {
        //                    Console.Write(dr["id"]);
        //                    IEbConnection con = EbSerializers.Json_Deserialize(dr["con_obj"].ToString());
        //                    Console.WriteLine("- " + dr["id"]);
        //                    if (con.EbConnectionType == EbConnectionTypes.EbDATA)
        //                    {
        //                        EbDataDbConnection _connection = (con as EbDataDbConnection);
        //                        if (Convert.ToInt32(_connection.DatabaseVendor) == 0)
        //                        {
        //                            PostgresConfig c = new PostgresConfig
        //                            {
        //                                DatabaseName = _connection.DatabaseName,
        //                                Id = _connection.Id,
        //                                IsSSL = _connection.IsSSL,
        //                                NickName = _connection.NickName,
        //                                Password = _connection.Password,
        //                                Port = _connection.Port,
        //                                ReadOnlyPassword = _connection.ReadOnlyPassword,
        //                                ReadOnlyUserName = _connection.ReadOnlyUserName,
        //                                ReadWritePassword = _connection.ReadWritePassword,
        //                                ReadWriteUserName = _connection.ReadWriteUserName,
        //                                Server = _connection.Server,
        //                                Timeout = _connection.Timeout,
        //                                UserName = _connection.UserName
        //                            };
        //                            return_id = c.PersistConfForHelper(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]), Convert.ToDateTime(dr["date_created"]));
        //                        }
        //                        if (Convert.ToInt32(_connection.DatabaseVendor) == 3)
        //                        {
        //                            OracleConfig c = new OracleConfig
        //                            {
        //                                DatabaseName = _connection.DatabaseName,
        //                                IsSSL = _connection.IsSSL,
        //                                NickName = _connection.NickName,
        //                                Password = _connection.Password,
        //                                Port = _connection.Port,
        //                                ReadOnlyPassword = _connection.ReadOnlyPassword,
        //                                ReadOnlyUserName = _connection.ReadOnlyUserName,
        //                                ReadWritePassword = _connection.ReadWritePassword,
        //                                ReadWriteUserName = _connection.ReadWriteUserName,
        //                                Server = _connection.Server,
        //                                Timeout = _connection.Timeout,
        //                                UserName = _connection.UserName
        //                            };
        //                            return_id = (c as OracleConfig).PersistConfForHelper(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]), Convert.ToDateTime(dr["date_created"]));
        //                        }
        //                        if (Convert.ToInt32(_connection.DatabaseVendor) == 1)
        //                        {
        //                            MySqlConfig c = new MySqlConfig
        //                            {
        //                                DatabaseName = _connection.DatabaseName,
        //                                IsSSL = _connection.IsSSL,
        //                                NickName = _connection.NickName,
        //                                Password = _connection.Password,
        //                                Port = _connection.Port,
        //                                ReadOnlyPassword = _connection.ReadOnlyPassword,
        //                                ReadOnlyUserName = _connection.ReadOnlyUserName,
        //                                ReadWritePassword = _connection.ReadWritePassword,
        //                                ReadWriteUserName = _connection.ReadWriteUserName,
        //                                Server = _connection.Server,
        //                                Timeout = _connection.Timeout,
        //                                UserName = _connection.UserName
        //                            };
        //                            return_id = (c as MySqlConfig).PersistConfForHelper(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]), Convert.ToDateTime(dr["date_created"]));
        //                        }
        //                        EbIntegration _obj = new EbIntegration
        //                        {
        //                            ConfigId = return_id,
        //                            Preference = ConPreferences.PRIMARY,
        //                            Type = EbConnectionTypes.EbDATA
        //                        };
        //                        _obj.PersistIntegrationForHelper(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]), Convert.ToDateTime(dr["date_created"]));
        //                    }

        //                    else if (con.EbConnectionType == EbConnectionTypes.EbOBJECTS)
        //                    {
        //                        EbObjectsDbConnection _connection = (con as EbObjectsDbConnection);
        //                        if (_connection.DatabaseVendor == DatabaseVendors.PGSQL)
        //                        {
        //                            PostgresConfig c = new PostgresConfig
        //                            {
        //                                DatabaseName = _connection.DatabaseName,
        //                                IsSSL = _connection.IsSSL,
        //                                NickName = _connection.NickName,
        //                                Password = _connection.Password,
        //                                Port = _connection.Port,
        //                                ReadOnlyPassword = _connection.ReadOnlyPassword,
        //                                ReadOnlyUserName = _connection.ReadOnlyUserName,
        //                                ReadWritePassword = _connection.ReadWritePassword,
        //                                ReadWriteUserName = _connection.ReadWriteUserName,
        //                                Server = _connection.Server,
        //                                Timeout = _connection.Timeout,
        //                                UserName = _connection.UserName
        //                            };
        //                            return_id = c.PersistConfForHelper(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]), Convert.ToDateTime(dr["date_created"]));
        //                        }
        //                        if (Convert.ToInt32(_connection.DatabaseVendor) == 3)
        //                        {
        //                            OracleConfig c = new OracleConfig
        //                            {
        //                                DatabaseName = _connection.DatabaseName,
        //                                Id = _connection.Id,
        //                                IsSSL = _connection.IsSSL,
        //                                NickName = _connection.NickName,
        //                                Password = _connection.Password,
        //                                Port = _connection.Port,
        //                                ReadOnlyPassword = _connection.ReadOnlyPassword,
        //                                ReadOnlyUserName = _connection.ReadOnlyUserName,
        //                                ReadWritePassword = _connection.ReadWritePassword,
        //                                ReadWriteUserName = _connection.ReadWriteUserName,
        //                                Server = _connection.Server,
        //                                Timeout = _connection.Timeout,
        //                                UserName = _connection.UserName
        //                            };
        //                            return_id = (c as OracleConfig).PersistConfForHelper(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]), Convert.ToDateTime(dr["date_created"]));
        //                        }
        //                        if (Convert.ToInt32(_connection.DatabaseVendor) == 1)
        //                        {
        //                            MySqlConfig c = new MySqlConfig
        //                            {
        //                                DatabaseName = _connection.DatabaseName,
        //                                Id = _connection.Id,
        //                                IsSSL = _connection.IsSSL,
        //                                NickName = _connection.NickName,
        //                                Password = _connection.Password,
        //                                Port = _connection.Port,
        //                                ReadOnlyPassword = _connection.ReadOnlyPassword,
        //                                ReadOnlyUserName = _connection.ReadOnlyUserName,
        //                                ReadWritePassword = _connection.ReadWritePassword,
        //                                ReadWriteUserName = _connection.ReadWriteUserName,
        //                                Server = _connection.Server,
        //                                Timeout = _connection.Timeout,
        //                                UserName = _connection.UserName
        //                            };
        //                            return_id = (c as MySqlConfig).PersistConfForHelper(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]), Convert.ToDateTime(dr["date_created"]));
        //                        }
        //                        EbIntegration _obj = new EbIntegration
        //                        {
        //                            ConfigId = return_id,
        //                            Preference = ConPreferences.PRIMARY,
        //                            Type = EbConnectionTypes.EbOBJECTS
        //                        };
        //                        _obj.PersistIntegration(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]));
        //                    }

        //                    else if (con.EbConnectionType == EbConnectionTypes.Cloudinary)
        //                    {
        //                        EbCloudinaryConnection _connection = con as EbCloudinaryConnection;
        //                        EbCloudinaryConfig c = new EbCloudinaryConfig
        //                        {
        //                            ApiKey = _connection.Account.ApiKey,
        //                            ApiSecret = _connection.Account.ApiSecret,
        //                            Cloud = _connection.Account.Cloud,
        //                            NickName = _connection.NickName
        //                        };
        //                        return_id = c.PersistConfForHelper(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]), Convert.ToDateTime(dr["date_created"]));

        //                        EbIntegration _obj = new EbIntegration
        //                        {
        //                            ConfigId = return_id,
        //                            Preference = ConPreferences.PRIMARY,
        //                            Type = EbConnectionTypes.Cloudinary
        //                        };
        //                        _obj.PersistIntegration(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]));
        //                    }

        //                    else if (con.EbConnectionType == EbConnectionTypes.SMTP)
        //                    {
        //                        EbEmail _connection = con as EbEmail;
        //                        EbSmtpConfig c = new EbSmtpConfig
        //                        {
        //                            EmailAddress = _connection.EmailAddress,
        //                            EnableSsl = _connection.EnableSsl,
        //                            Host = _connection.Host,
        //                            NickName = _connection.NickName,
        //                            Password = _connection.Password,
        //                            Port = _connection.Port,
        //                            //ProviderName = SmtpProviders.Gmail
        //                            ProviderName = (SmtpProviders)Enum.Parse(typeof(SmtpProviders), _connection.ProviderName)
        //                        };
        //                        return_id = c.PersistConfForHelper(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]), Convert.ToDateTime(dr["date_created"]));
        //                        EbIntegration _obj = new EbIntegration
        //                        {
        //                            ConfigId = return_id,
        //                            Preference = ConPreferences.PRIMARY,
        //                            Type = EbConnectionTypes.SMTP
        //                        };
        //                        _obj.PersistIntegration(dr["solution_id"].ToString(), this.InfraConnectionFactory, Convert.ToInt32(dr["eb_user_id"]));
        //                    }

        //                }
        //            }

        //            Console.ForegroundColor = ConsoleColor.Red;
        //            Console.WriteLine("-----Migrated Integrations :  ------ " + migrated);
        //            Console.ForegroundColor = ConsoleColor.White;

        //            //Cloudinary,EbDATA,EbFILES,EbOBJECTS,SMTP
        //            //Cloudinary,EbDATA,,EbFILES,EbImageManipulation,EbLOGS,EbOBJECTS,FTP,SMS,SMTP
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        Console.ForegroundColor = ConsoleColor.Red;
        //        Console.WriteLine("-----Error at Integrations :  ------ " + migrated);
        //        Console.ForegroundColor = ConsoleColor.White;
        //        res.ResponseStatus.Message = e.Message;
        //    }
        //    return res;
        //}
    }
}

