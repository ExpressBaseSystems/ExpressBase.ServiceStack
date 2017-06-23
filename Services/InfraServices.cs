using ExpressBase.Common;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data.Common;
using System.IO;
using System.Net;
using System.Reflection;
using System.Runtime.Loader;

namespace ExpressBase.ServiceStack.Services
{
    [ClientCanSwapTemplates]
    [EnableCors]
    public class InfraServices : EbBaseService
    {
        //[Authenticate]
        //public async System.Threading.Tasks.Task<InfraResponse> Any(InfraRequest request)
        //{
        //    base.ClientID = request.TenantAccountId;
        //    ILog log = LogManager.GetLogger(GetType());
        //    using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
        //    {
        //        con.Open();

        //      if (request.ltype == "G+")
        //        {

        //            var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "INSERT INTO eb_tenants (cname,firstname,gender,socialid,profileimg)VALUES(@cname, @firstname,@gender,@socialid,@profileimg)ON CONFLICT(socialid) DO UPDATE SET cname=@cname RETURNING id");
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("cname", System.Data.DbType.String, request.Colvalues["email"]));
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("firstname", System.Data.DbType.String, request.Colvalues["name"]));
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("gender", System.Data.DbType.String, request.Colvalues["gender"]));
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("socialid", System.Data.DbType.String, request.Colvalues["id"]));
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("profileimg", System.Data.DbType.String, string.Format("<img src='{0}' class='img-circle navbar-right img-cir'>", request.Colvalues["picture"])));


        //            InfraResponse res = new InfraResponse
        //            {
        //                id = Convert.ToInt32(cmd.ExecuteScalar())
        //            };
        //            return res;

        //        }

        //        else
        //        {

        //            var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "INSERT INTO eb_tenants (cname,password,u_token) VALUES ( @cname,@password,md5( @cname || now())) RETURNING u_token;");
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("cname", System.Data.DbType.String, request.Colvalues["email"]));
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("password", System.Data.DbType.String, request.Colvalues["password"]));

        //            InfraResponse res = new InfraResponse
        //            {
        //                u_token = cmd.ExecuteScalar().ToString()
        //            };
        //            if (!string.IsNullOrEmpty(res.u_token))
        //            {
        //                await base.ResolveService<EmailServices>().Any(new EmailServicesRequest { To = request.Colvalues["email"].ToString(), Message = "XXXX", Subject = "YYYY" });

        //            }
        //            return res;

        //        }
        //    }
        //}

        [Authenticate]
        public TokenRequiredUploadResponse Any(TokenRequiredUploadRequest request)
        {
            TokenRequiredUploadResponse resp = null;

            ILog log = LogManager.GetLogger(GetType());

            if (request.TenantAccountId!="expressbase")
            {
                base.ClientID = request.TenantAccountId;
                using (var con = base.DatabaseFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    var cmd = base.DatabaseFactory.ObjectsDB.GetNewCommand(con, "UPDATE eb_users SET locale=@locale,timezone=@timezone,dateformat=@dateformat,numformat=@numformat,timezonefull=@timezonefull WHERE id=@id");
                    cmd.Parameters.Add(base.DatabaseFactory.ObjectsDB.GetNewParameter("locale", System.Data.DbType.String, request.Colvalues["locale"]));
                    cmd.Parameters.Add(base.DatabaseFactory.ObjectsDB.GetNewParameter("timezone", System.Data.DbType.String, request.Colvalues["timecode"]));
                    cmd.Parameters.Add(base.DatabaseFactory.ObjectsDB.GetNewParameter("dateformat", System.Data.DbType.String, request.Colvalues["dateformat"]));
                    cmd.Parameters.Add(base.DatabaseFactory.ObjectsDB.GetNewParameter("numformat", System.Data.DbType.String, request.Colvalues["numformat"]));
                    cmd.Parameters.Add(base.DatabaseFactory.ObjectsDB.GetNewParameter("timezonefull", System.Data.DbType.String, request.Colvalues["timezone"]));
                    cmd.Parameters.Add(base.DatabaseFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int64, request.Colvalues["uid"]));
                    resp = new TokenRequiredUploadResponse
                    {
                        id = Convert.ToInt32(cmd.ExecuteScalar())

                    };
                }
            }
            else
            {
                using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
                {
                    con.Open();
                    log.Info("#Eb account insert 1");
                    if (request.Colvalues.ContainsKey("op") && request.Colvalues["op"].ToString() == "insertaccount")
                    {
                        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "INSERT INTO eb_tenantaccount (accountname,cid,address,phone,email,website,tier,tenantname,createdat,validtill,profilelogo,tenantid)VALUES(@accountname,@cid,@address,@phone,@email,@website,@tier,@tenantname,now(),(now()+ interval '30' day),@profilelogo,@tenantid) ON CONFLICT(cid) DO UPDATE SET accountname=@accountname,address=@address,phone=@phone,email=@email,website=@website,tier=@tier,createdat=now(),validtill=(now()+ interval '30' day) RETURNING id ");
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("accountname", System.Data.DbType.String, request.Colvalues["accountname"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("cid", System.Data.DbType.String, request.Colvalues["cid"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("address", System.Data.DbType.String, request.Colvalues["address"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("phone", System.Data.DbType.String, request.Colvalues["phone"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("email", System.Data.DbType.String, request.Colvalues["email"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("website", System.Data.DbType.String, request.Colvalues["website"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("tier", System.Data.DbType.String, request.Colvalues["tier"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("tenantname", System.Data.DbType.String, request.Colvalues["tenantname"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("profilelogo", System.Data.DbType.String, string.Format("<img src='{0}' class='prologo img-circle'/>", request.Colvalues["imgpro"])));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("tenantid", System.Data.DbType.Int64, request.Colvalues["tenantid"]));
                        resp = new TokenRequiredUploadResponse
                        {
                            id = Convert.ToInt32(cmd.ExecuteScalar())
                        };
                        base.Redis.Set<string>(string.Format("cid_{0}_uid_{1}_pimg", base.ClientID, resp.id), string.Format("<img src='{0}'class='img-circle img-cir'/>", request.Colvalues["imgpro"]));

                    }
                    else if (request.Colvalues.ContainsKey("dbcheck") && request.Colvalues["dbcheck"].ToString() == "dbconfig")
                    {
                        resp = new TokenRequiredUploadResponse();
                        int uid = 0;
                        string sql = string.Format("SELECT cid,accountname,tenantid FROM eb_tenantaccount WHERE id={0}", request.Colvalues["acid"]);
                        var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);


                        //CREATE CLIENTDB CONN
                        EbClientConf e = new EbClientConf()
                        {
                            ClientID = dt.Rows[0][0].ToString(),
                            ClientName = dt.Rows[0][1].ToString(),
                            EbClientTier = EbClientTiers.Unlimited
                        };

                        if (request.Colvalues.ContainsKey("dbtype") && request.Colvalues["dbtype"].ToString() == "2")
                        {
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbOBJECTS, new EbDatabaseConfiguration(EbDatabaseTypes.EbOBJECTS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_objrw"])), request.Colvalues["dbname_objrw"].ToString(), request.Colvalues["sip_objrw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_objrw"]), request.Colvalues["duname_objrw"].ToString(), request.Colvalues["pwd_objrw"].ToString(), Convert.ToInt32(request.Colvalues["tout_objrw"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbDATA, new EbDatabaseConfiguration(EbDatabaseTypes.EbDATA, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_datarw"])), request.Colvalues["dbname_datarw"].ToString(), request.Colvalues["sip_datarw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_datarw"]), request.Colvalues["duname_datarw"].ToString(), request.Colvalues["pwd_datarw"].ToString(), Convert.ToInt32(request.Colvalues["tout_datarw"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbFILES, new EbDatabaseConfiguration(EbDatabaseTypes.EbFILES, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_filerw"])), request.Colvalues["dbname_filerw"].ToString(), request.Colvalues["sip_filerw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_filerw"]), request.Colvalues["duname_filerw"].ToString(), request.Colvalues["pwd_filerw"].ToString(), Convert.ToInt32(request.Colvalues["tout_filerw"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbLOGS, new EbDatabaseConfiguration(EbDatabaseTypes.EbLOGS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_logrw"])), request.Colvalues["dbname_logrw"].ToString(), request.Colvalues["sip_logrw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_logrw"]), request.Colvalues["duname_logrw"].ToString(), request.Colvalues["pwd_logrw"].ToString(), Convert.ToInt32(request.Colvalues["tout_logrw"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbOBJECTS_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbOBJECTS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_objrw"])), request.Colvalues["dbname_objro"].ToString(), request.Colvalues["sip_objro"].ToString(), Convert.ToInt32(request.Colvalues["pnum_objro"]), request.Colvalues["duname_objro"].ToString(), request.Colvalues["pwd_objro"].ToString(), Convert.ToInt32(request.Colvalues["tout_objro"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbDATA_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbDATA_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_datarw"])), request.Colvalues["dbname_dataro"].ToString(), request.Colvalues["sip_dataro"].ToString(), Convert.ToInt32(request.Colvalues["pnum_dataro"]), request.Colvalues["duname_dataro"].ToString(), request.Colvalues["pwd_dataro"].ToString(), Convert.ToInt32(request.Colvalues["tout_dataro"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbFILES_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbFILES_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_filerw"])), request.Colvalues["dbname_filero"].ToString(), request.Colvalues["sip_filero"].ToString(), Convert.ToInt32(request.Colvalues["pnum_filero"]), request.Colvalues["duname_filero"].ToString(), request.Colvalues["pwd_filero"].ToString(), Convert.ToInt32(request.Colvalues["tout_filero"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbLOGS_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbLOGS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_logrw"])), request.Colvalues["dbname_logro"].ToString(), request.Colvalues["sip_logro"].ToString(), Convert.ToInt32(request.Colvalues["pnum_logro"]), request.Colvalues["duname_logro"].ToString(), request.Colvalues["pwd_logro"].ToString(), Convert.ToInt32(request.Colvalues["tout_logro"])));
                        }
                        else
                        {
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbOBJECTS, new EbDatabaseConfiguration(EbDatabaseTypes.EbOBJECTS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbDATA, new EbDatabaseConfiguration(EbDatabaseTypes.EbDATA, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbFILES, new EbDatabaseConfiguration(EbDatabaseTypes.EbFILES, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbLOGS, new EbDatabaseConfiguration(EbDatabaseTypes.EbLOGS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbOBJECTS_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbOBJECTS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbDATA_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbDATA_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbFILES_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbFILES_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                            e.DatabaseConfigurations.Add(EbDatabaseTypes.EbLOGS_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbLOGS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                        }


                        byte[] bytea2 = EbSerializers.ProtoBuf_Serialize(e);
                        var dbconf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea2);
                        var dbf = new DatabaseFactory(dbconf);
                        DbTransaction _con_d1_trans = null;
                        DbTransaction _con_o1_trans = null;

                        var _con_d1 = dbf.DataDB.GetNewConnection();
                        var _con_d2 = dbf.DataDBRO.GetNewConnection();
                        var _con_o1 = dbf.ObjectsDB.GetNewConnection();
                        var _con_o2 = dbf.ObjectsDBRO.GetNewConnection();
                        var _con_l1 = dbf.LogsDB.GetNewConnection();
                        var _con_l2 = dbf.LogsDBRO.GetNewConnection();
                        var _con_f1 = dbf.FilesDB.GetNewConnection();
                        var _con_f2 = dbf.FilesDBRO.GetNewConnection();
                        int i = 0;
                        try
                        {
                            _con_d1.Open(); 
                            i++;
                            _con_d2.Open(); _con_d2.Close();
                            i++;
                            _con_o1.Open(); 
                            i++;
                            _con_o2.Open(); _con_o2.Close();
                            i++;
                            _con_l1.Open(); _con_l1.Close();
                            i++;
                            _con_l2.Open(); _con_l2.Close();
                            i++;
                            _con_f1.Open(); _con_f1.Close();
                            i++;
                            _con_f2.Open(); _con_f2.Close();

                            _con_d1_trans = _con_d1.BeginTransaction();
                            _con_o1_trans = _con_o1.BeginTransaction();
                            
                        }
                        catch (Exception ex)
                        {
                            if (i == 0)
                                throw HttpError.NotFound("Error in data");

                            else if (i == 1)
                                throw HttpError.NotFound("Error in data read only");

                            else if (i == 2)
                                throw HttpError.NotFound("Error in objects");

                            else if (i == 3)
                                throw HttpError.NotFound("Error in objects read only");

                            else if (i == 4)
                                throw HttpError.NotFound("Error in logs");

                            else if (i == 5)
                                throw HttpError.NotFound("Error in log read only");

                            else if (i == 6)
                                throw HttpError.NotFound("Error in files");

                            else if (i == 7)
                                throw HttpError.NotFound("Error in files reda only");

                            else
                                throw HttpError.NotFound("Success");
                        }

                        var tenantdt = InfraDatabaseFactory.InfraDB.DoQuery(string.Format("SELECT cname,firstname,phone,password FROM eb_tenants WHERE id={0}", dt.Rows[0][2]));
                        try
                        {
                            TableInsertsDataDB(dbf, tenantdt, _con_d1);
                            TableInsertObjectDB(dbf, _con_o1);
                            _con_d1_trans.Commit();
                            _con_o1_trans.Commit();
                            _con_d1.Close();
                            _con_o1.Close();

                        }
                        catch (Exception ex)
                        {
                            string error = null;

                            if (_con_d1.State == System.Data.ConnectionState.Open)// || _con_o1.State == System.Data.ConnectionState.Open)
                                error = "Database for data is already in use.Please connect a new database";
                            else if (_con_o1.State == System.Data.ConnectionState.Open)
                                error = "Database for objects is already in use.Please connect a new database";

                            _con_d1_trans.Rollback();
                            _con_o1_trans.Rollback();
                            throw HttpError.NotFound(error);
                        }



                        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenantaccount SET config=@config,dbconfigtype=@dbconfigtype WHERE id=@id RETURNING id");
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("config", System.Data.DbType.Binary, bytea2));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("dbconfigtype", System.Data.DbType.Int32, request.Colvalues["dbtype"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, Convert.ToInt32(request.Colvalues["acid"])));
                        uid = Convert.ToInt32(cmd.ExecuteScalar());
                        resp.id = uid;


                    }
                    else if (request.op == "updatetenant")
                    {
                        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenants SET company=@company,employees=@employees,country=@country,phone=@phone WHERE id=@id RETURNING id");

                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("company", System.Data.DbType.String, request.Colvalues["company"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("employees", System.Data.DbType.String, request.Colvalues["employees"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("country", System.Data.DbType.String, request.Colvalues["country"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("phone", System.Data.DbType.String, request.Colvalues["phone"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, request.Colvalues["id"]));
                        resp = new TokenRequiredUploadResponse
                        {
                            id = Convert.ToInt32(cmd.ExecuteScalar())
                        };
                    }
                    else if (request.op == "tenantimgupload")
                    {
                        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenants SET profileimg=@profileimg WHERE id=@id RETURNING id");
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("profileimg", System.Data.DbType.String, string.Format("<img src='{0}'class='img-circle img-cir'/>", request.Colvalues["proimg"])));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, request.Colvalues["id"]));
                        resp = new TokenRequiredUploadResponse
                        {
                            id = Convert.ToInt32(cmd.ExecuteScalar())
                        };
                        base.Redis.Set<string>(string.Format("uid_{0}_pimg", resp.id), string.Format("<img src='{0}'class='img-circle img-cir'/>", request.Colvalues["proimg"]));
                    }
                    else if (request.Colvalues.ContainsKey("edit") && request.Colvalues["edit"].ToString() == "edit")
                    {
                        Dictionary<string, object> dict = new Dictionary<string, object>();
                        string sql = string.Format("SELECT * FROM eb_tenantaccount WHERE id={0}", request.Colvalues["id"]);
                        var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);
                        foreach (EbDataRow dr in dt.Rows)
                        {
                            foreach (EbDataColumn dc in dt.Columns)
                            {
                                dict.Add(dc.ColumnName, dr[dc.ColumnName]);
                            }
                        }
                        resp = new TokenRequiredUploadResponse()
                        {
                            Data = dict
                        };
                    }
                    else if (request.Colvalues.ContainsKey("dbedit") && request.Colvalues["dbedit"].ToString() == "dbedit")
                    {
                        Dictionary<string, object> dbresults = new Dictionary<string, object>();
                        var dt = InfraDatabaseFactory.InfraDB_RO.DoQuery(string.Format("SELECT dbconfigtype,config FROM eb_tenantaccount WHERE id={0}", request.Colvalues["id"]));
                        int db_conf_type = 0;

                        db_conf_type = Convert.ToInt32(dt.Rows[0][0]);

                        byte[] bytea = (dt.Rows[0][1] != DBNull.Value) ? (byte[])dt.Rows[0][1] : null;

                        if (bytea != null)
                        {
                            EbClientConf conf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea);
                            dbresults[Constants.CONF_DBTYPE] = db_conf_type;
                            if (db_conf_type == 1)
                            {

                                dbresults[Constants.CONF_VENDOR] = (int)conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].DatabaseVendor;
                                dbresults[Constants.CONF_DBNAME] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].DatabaseName;
                                dbresults[Constants.CONF_SIP] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].Server;
                                dbresults[Constants.CONF_PORT] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].Port;
                                dbresults[Constants.CONF_UNAME] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].UserName;
                                dbresults[Constants.CONF_TOUT] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].Timeout;
                                dbresults[Constants.CONF_PWD] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].Password;

                            }
                            else
                            {

                                dbresults[Constants.CONF_VENDOR_DATA] = (int)conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].DatabaseVendor;
                                dbresults[Constants.CONF_VENDOR_OBJ] = (int)conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS].DatabaseVendor;
                                dbresults[Constants.CONF_VENDOR_FILES] = (int)conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES].DatabaseVendor;
                                dbresults[Constants.CONF_VENDOR_LOGS] = (int)conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS].DatabaseVendor;

                                dbresults[Constants.CONF_DBNAME_DATA_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].DatabaseName;
                                dbresults[Constants.CONF_DBNAME_OBJ_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS].DatabaseName;
                                dbresults[Constants.CONF_DBNAME_FILES_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES].DatabaseName;
                                dbresults[Constants.CONF_DBNAME_LOGS_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS].DatabaseName;
                                dbresults[Constants.CONF_DBNAME_DATA_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA_RO].DatabaseName;
                                dbresults[Constants.CONF_DBNAME_OBJ_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS_RO].DatabaseName;
                                dbresults[Constants.CONF_DBNAME_FILES_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES_RO].DatabaseName;
                                dbresults[Constants.CONF_DBNAME_LOGS_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS_RO].DatabaseName;

                                dbresults[Constants.CONF_SIP_DATA_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].Server;
                                dbresults[Constants.CONF_SIP_OBJ_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS].Server;
                                dbresults[Constants.CONF_SIP_FILES_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES].Server;
                                dbresults[Constants.CONF_SIP_LOGS_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS].Server;
                                dbresults[Constants.CONF_SIP_DATA_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA_RO].Server;
                                dbresults[Constants.CONF_SIP_OBJ_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS_RO].Server;
                                dbresults[Constants.CONF_SIP_FILES_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES_RO].Server;
                                dbresults[Constants.CONF_SIP_LOGS_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS_RO].Server;

                                dbresults[Constants.CONF_PORT_DATA_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].Port;
                                dbresults[Constants.CONF_PORT_OBJ_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS].Port;
                                dbresults[Constants.CONF_PORT_FILES_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES].Port;
                                dbresults[Constants.CONF_PORT_LOGS_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS].Port;
                                dbresults[Constants.CONF_PORT_DATA_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA_RO].Port;
                                dbresults[Constants.CONF_PORT_OBJ_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS_RO].Port;
                                dbresults[Constants.CONF_PORT_FILES_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES_RO].Port;
                                dbresults[Constants.CONF_PORT_LOGS_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS_RO].Port;

                                dbresults[Constants.CONF_UNAME_DATA_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].UserName;
                                dbresults[Constants.CONF_UNAME_OBJ_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS].UserName;
                                dbresults[Constants.CONF_UNAME_FILES_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES].UserName;
                                dbresults[Constants.CONF_UNAME_LOGS_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS].UserName;
                                dbresults[Constants.CONF_UNAME_DATA_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA_RO].UserName;
                                dbresults[Constants.CONF_UNAME_OBJ_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS_RO].UserName;
                                dbresults[Constants.CONF_UNAME_FILES_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES_RO].UserName;
                                dbresults[Constants.CONF_UNAME_LOGS_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS_RO].UserName;

                                dbresults[Constants.CONF_TOUT_DATA_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].Timeout;
                                dbresults[Constants.CONF_TOUT_OBJ_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS].Timeout;
                                dbresults[Constants.CONF_TOUT_FILES_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES].Timeout;
                                dbresults[Constants.CONF_TOUT_LOGS_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS].Timeout;
                                dbresults[Constants.CONF_TOUT_DATA_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA_RO].Timeout;
                                dbresults[Constants.CONF_TOUT_OBJ_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS_RO].Timeout;
                                dbresults[Constants.CONF_TOUT_FILES_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES_RO].Timeout;
                                dbresults[Constants.CONF_TOUT_LOGS_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS_RO].Timeout;

                                dbresults[Constants.CONF_PWD_DATA_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA].Password;
                                dbresults[Constants.CONF_PWD_OBJ_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS].Password;
                                dbresults[Constants.CONF_PWD_FILES_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES].Password;
                                dbresults[Constants.CONF_PWD_LOGS_RW] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS].Password;
                                dbresults[Constants.CONF_PWD_DATA_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbDATA_RO].Password;
                                dbresults[Constants.CONF_PWD_OBJ_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbOBJECTS_RO].Password;
                                dbresults[Constants.CONF_PWD_FILES_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbFILES_RO].Password;
                                dbresults[Constants.CONF_PWD_LOGS_RO] = conf.DatabaseConfigurations[EbDatabaseTypes.EbLOGS_RO].Password;
                            }
                            resp = new TokenRequiredUploadResponse
                            {
                                Data = dbresults
                            };
                        }
                        else
                        {
                            resp = new TokenRequiredUploadResponse
                            {
                                Data = dbresults
                            };
                        }
                    }
                    else
                    {
                        resp = new TokenRequiredUploadResponse
                        {
                            id = 0
                        };
                    }
                }
            }
            return resp;
        }

        [Authenticate]
        public TokenRequiredSelectResponse Any(TokenRequiredSelectRequest request)
        {
            base.ClientID = request.TenantAccountId;

            using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
            {
                con.Open();
                if (request.restype == "img")
                {
                    string sql = string.Format("SELECT id,profileimg FROM eb_tenants WHERE id={0}", request.Uid);
                    var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);
                    // Dictionary<int, string> list = new Dictionary<int, string>();
                    List<List<object>> list = new List<List<object>>();
                    foreach (EbDataRow dr in dt.Rows)
                    {
                        list.Add(new List<object> { Convert.ToInt32(dr[0]), dr[1].ToString() });
                    }
                    TokenRequiredSelectResponse resp = new TokenRequiredSelectResponse()
                    {
                        returnlist = list
                    };
                    return resp;
                }
                else if (request.restype == "homeimg")
                {
                    string sql = string.Format("SELECT id,profileimg FROM eb_tenants WHERE cname={0}", request.Uname);
                    var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);
                    List<List<object>> list = new List<List<object>>();
                    foreach (EbDataRow dr in dt.Rows)
                    {
                        list.Add(new List<object> { Convert.ToInt32(dr[0]), dr[1].ToString() });
                    }
                    TokenRequiredSelectResponse resp = new TokenRequiredSelectResponse()
                    {
                        returnlist = list
                    };
                    return resp;

                }

                else
                {
                    string sql = string.Format("SELECT id,accountname,profilelogo,cid,createdat FROM eb_tenantaccount WHERE tenantid={0}", request.Uid);
                    var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);
                    List<List<object>> list = new List<List<object>>();
                    foreach (EbDataRow dr in dt.Rows)
                    {
                        list.Add(new List<object> { Convert.ToInt32(dr[0]), dr[1].ToString(), dr[2].ToString(), dr[3].ToString(),dr[4] });
                    }
                    TokenRequiredSelectResponse resp = new TokenRequiredSelectResponse()
                    {
                        returnlist = list
                    };
                    return resp;
                }


            }
        }

        
        public void TableInsertsDataDB(DatabaseFactory dbf, EbDataTable dt, DbConnection _con_d1)
        {
            string result;
            var assembly = typeof(ExpressBase.Data.Resource).GetAssembly();
            using (Stream stream = assembly.GetManifestResourceStream("ExpressBase.Data.SqlScripts.PostGreSql.DataDb.postgres_eb_users.sql"))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    result = reader.ReadToEnd();
                }
             
                var datacmd = dbf.DataDB.GetNewCommand(_con_d1, result);
                datacmd.ExecuteNonQuery();
                var cmd = dbf.DataDB.GetNewCommand(_con_d1, "INSERT INTO eb_users(email,pwd,fullname,phnoprimary) VALUES(@email,@pwd,@fullname,@phnoprimary); INSERT INTO eb_role2user(user_id,role_id) VALUES(1,3)");
                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("email", System.Data.DbType.String, dt.Rows[0][0]));
                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("pwd", System.Data.DbType.String, dt.Rows[0][3]));
                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("fullname", System.Data.DbType.String, dt.Rows[0][1]));
                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("phnoprimary", System.Data.DbType.String, dt.Rows[0][2]));
                cmd.ExecuteScalar();
            }

        }

        public void TableInsertObjectDB(DatabaseFactory dbf,DbConnection _con_o1)
        {
            string result;
            var assembly = typeof(ExpressBase.Data.Resource).GetAssembly();
            using (Stream stream = assembly.GetManifestResourceStream("ExpressBase.Data.SqlScripts.PostGreSql.ObjectsDb.postgres_eb_objects.sql"))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    result = reader.ReadToEnd();
                }
                    var datacmd = dbf.ObjectsDB.GetNewCommand(_con_o1, result);
                    datacmd.ExecuteNonQuery();
            }          

        }

        //public InfraDb_GENERIC_SELECTResponse Any(InfraDb_GENERIC_SELECTRequest req)
        //{
        //    using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
        //    {
        //        var redisClient = this.Redis;
        //        EbTableCollection tcol = redisClient.Get<EbTableCollection>("EbInfraTableCollection");
        //        EbTableColumnCollection ccol = redisClient.Get<EbTableColumnCollection>("EbInfraTableColumnCollection");
        //        con.Open();
        //        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, InfraDbSqlQueries["KEY1"]);
        //        foreach (string key in req.Parameters.Keys)
        //        {
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter(
        //                string.Format("@{0}", key), ccol[key].Type, req.Parameters[key]));

        //            foreach (int colkey in ccol.Keys)
        //            {
        //                if (ccol[colkey].Name == key)
        //                {
        //                }
        //            }
        //        }

        //        var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);
        //        ListDictionary list = new ListDictionary();
        //        foreach (EbDataRow dr in dt.Rows)
        //        {
        //            list.Add(Convert.ToInt32(dr[0]), dr[1].ToString());
        //        }
        //        GetAccountResponse resp = new GetAccountResponse()
        //        {
        //            ldict = list
        //        };
        //        return resp;
        //    }
        //}
    }

}
