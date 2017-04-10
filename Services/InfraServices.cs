using ExpressBase.Common;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;

namespace ExpressBase.ServiceStack.Services
{
    [ClientCanSwapTemplates]
    public class InfraServices : EbBaseService
    {
        public InfraResponse Any(InfraRequest request)
        {
            base.ClientID = request.TenantAccountId;
            ILog log = LogManager.GetLogger(GetType());
            using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
            {
                con.Open();

                if (request.ltype == "fb")
                {

                    //DateTime date = DateTime.ParseExact(request.Colvalues["birthday"].ToString(), "MM/dd/yyyy", CultureInfo.InvariantCulture);
                    var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "INSERT INTO eb_tenants (cname,firstname,gender,socialid) VALUES(@cname, @firstname,@gender,@socialid)ON CONFLICT(socialid) DO UPDATE SET cname=@cname RETURNING id ");
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("cname", System.Data.DbType.String, request.Colvalues["email"]));
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("firstname", System.Data.DbType.String, request.Colvalues["name"]));
                    //cmd.Parameters.Add(df.InfraDB.GetNewParameter("birthday", System.Data.DbType.DateTime, date));
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("gender", System.Data.DbType.String, request.Colvalues["gender"]));
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("socialid", System.Data.DbType.String, request.Colvalues["id"]));

                    InfraResponse res = new InfraResponse
                    {
                        id = Convert.ToInt32(cmd.ExecuteScalar())
                    };
                    return res;
                }
                else if (request.ltype == "G+")
                {

                    var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "INSERT INTO eb_tenants (cname,firstname,gender,socialid,profileimg)VALUES(@cname, @firstname,@gender,@socialid,@profileimg)ON CONFLICT(socialid) DO UPDATE SET cname=@cname RETURNING id");
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("cname", System.Data.DbType.String, request.Colvalues["email"]));
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("firstname", System.Data.DbType.String, request.Colvalues["name"]));
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("gender", System.Data.DbType.String, request.Colvalues["gender"]));
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("socialid", System.Data.DbType.String, request.Colvalues["id"]));
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("profileimg", System.Data.DbType.String, string.Format("<img src='{0}' class='img-circle navbar-right img-cir'>", request.Colvalues["picture"])));


                    InfraResponse res = new InfraResponse
                    {
                        id = Convert.ToInt32(cmd.ExecuteScalar())
                    };
                    return res;

                }

                else
                {

                    var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "INSERT INTO eb_tenants (cname,firstname,password) VALUES ( @cname, @firstname,@password) RETURNING id;");

                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("cname", System.Data.DbType.String, request.Colvalues["email"]));
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("firstname", System.Data.DbType.String, request.Colvalues["fullname"]));
                    cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("password", System.Data.DbType.String, request.Colvalues["password"]));

                    InfraResponse res = new InfraResponse
                    {
                        id = Convert.ToInt32(cmd.ExecuteScalar())
                    };
                    return res;

                }
            }
        }

        //public bool Any(UnRequest request)
        //{
        //    string path = Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName;
        //    var infraconf = EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(Path.Combine(path, "EbInfra.conn")));
        //    var df = new DatabaseFactory(infraconf);
        //    using (var con = df.InfraDB.GetNewConnection())
        //    {
        //        con.Open();

        //        foreach (string key in request.Colvalues.Keys)
        //        {
        //            string cf = request.Colvalues[key].ToString();
        //            var cmd = df.InfraDB.GetNewCommand(con, string.Format("SELECT COUNT(*) FROM eb_tenants where {0} = @{0}", key));
        //            cmd.Parameters.Add(df.InfraDB.GetNewParameter(string.Format("{0}", key), System.Data.DbType.String, request.Colvalues[key]));
        //            if (Convert.ToInt32(cmd.ExecuteScalar()) > 0)
        //                return false;

        //            else
        //                return true;
        //        }
        //        return false;
        //    }
        //}

        //public bool Any(DbCheckRequest request)
        //{

        //    string path = Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName;
        //    var infraconf = EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(Path.Combine(path, "EbInfra.conn")));
        //    var df = new DatabaseFactory(infraconf);
        //    using (var con = df.InfraDB.GetNewConnection())
        //    {
        //        con.Open();
        //        string sql = string.Format("SELECT cid,cname FROM eb_tenants WHERE id={0}", request.CId);
        //        var dt = df.InfraDB.DoQuery(sql);



        //        //CREATE CLIENTDB CONN
        //        EbClientConf e = new EbClientConf()
        //        {
        //            ClientID = dt.Rows[0][0].ToString(),
        //            ClientName = dt.Rows[0][1].ToString(),
        //            EbClientTier = EbClientTiers.Unlimited
        //        };

        //        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbOBJECTS, new EbDatabaseConfiguration(EbDatabaseTypes.EbOBJECTS, DatabaseVendors.PGSQL, request.DBColvalues["dbname"].ToString(), request.DBColvalues["sip"].ToString(), Convert.ToInt32(request.DBColvalues["pnum"]), request.DBColvalues["duname"].ToString(), request.DBColvalues["pwd"].ToString(), Convert.ToInt32(request.DBColvalues["tout"])));
        //        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbDATA, new EbDatabaseConfiguration(EbDatabaseTypes.EbDATA, DatabaseVendors.PGSQL, request.DBColvalues["dbname"].ToString(), request.DBColvalues["sip"].ToString(), Convert.ToInt32(request.DBColvalues["pnum"]), request.DBColvalues["duname"].ToString(), request.DBColvalues["pwd"].ToString(), Convert.ToInt32(request.DBColvalues["tout"])));
        //        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbFILES, new EbDatabaseConfiguration(EbDatabaseTypes.EbFILES, DatabaseVendors.PGSQL, request.DBColvalues["dbname"].ToString(), request.DBColvalues["sip"].ToString(), Convert.ToInt32(request.DBColvalues["pnum"]), request.DBColvalues["duname"].ToString(), request.DBColvalues["pwd"].ToString(), Convert.ToInt32(request.DBColvalues["tout"])));
        //        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbLOGS, new EbDatabaseConfiguration(EbDatabaseTypes.EbLOGS, DatabaseVendors.PGSQL, request.DBColvalues["dbname"].ToString(), request.DBColvalues["sip"].ToString(), Convert.ToInt32(request.DBColvalues["pnum"]), request.DBColvalues["duname"].ToString(), request.DBColvalues["pwd"].ToString(), Convert.ToInt32(request.DBColvalues["tout"])));
        //        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbOBJECTS_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbOBJECTS_RO, DatabaseVendors.PGSQL, request.DBColvalues["dbname"].ToString(), request.DBColvalues["sip"].ToString(), Convert.ToInt32(request.DBColvalues["pnum"]), request.DBColvalues["duname"].ToString(), request.DBColvalues["pwd"].ToString(), Convert.ToInt32(request.DBColvalues["tout"])));
        //        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbDATA_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbDATA_RO, DatabaseVendors.PGSQL, request.DBColvalues["dbname"].ToString(), request.DBColvalues["sip"].ToString(), Convert.ToInt32(request.DBColvalues["pnum"]), request.DBColvalues["duname"].ToString(), request.DBColvalues["pwd"].ToString(), Convert.ToInt32(request.DBColvalues["tout"])));
        //        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbFILES_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbFILES_RO, DatabaseVendors.PGSQL, request.DBColvalues["dbname"].ToString(), request.DBColvalues["sip"].ToString(), Convert.ToInt32(request.DBColvalues["pnum"]), request.DBColvalues["duname"].ToString(), request.DBColvalues["pwd"].ToString(), Convert.ToInt32(request.DBColvalues["tout"])));
        //        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbLOGS_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbLOGS_RO, DatabaseVendors.PGSQL, request.DBColvalues["dbname"].ToString(), request.DBColvalues["sip"].ToString(), Convert.ToInt32(request.DBColvalues["pnum"]), request.DBColvalues["duname"].ToString(), request.DBColvalues["pwd"].ToString(), Convert.ToInt32(request.DBColvalues["tout"])));

        //        byte[] bytea2 = EbSerializers.ProtoBuf_Serialize(e);
        //        var dbconf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea2);

        //        var dbf = new DatabaseFactory(dbconf);
        //        var _con = dbf.ObjectsDB.GetNewConnection();
        //        try
        //        {
        //            _con.Open();
        //        }
        //        catch (Exception ex) { return false; }
        //        var cmd = df.InfraDB.GetNewCommand(con, "UPDATE eb_tenants SET conf=@conf WHERE id=@id;");
        //        cmd.Parameters.Add(df.InfraDB.GetNewParameter("conf", System.Data.DbType.Binary, bytea2));
        //        cmd.Parameters.Add(df.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, Convert.ToInt32(request.DBColvalues["id"])));
        //        cmd.ExecuteNonQuery();
        //        return true;


        //    }
        //}

        public TokenRequiredUploadResponse Any(TokenRequiredUploadRequest request)
        {
          
            ILog log = LogManager.GetLogger(GetType());

            if (!string.IsNullOrEmpty(request.TenantAccountId))
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
                    TokenRequiredUploadResponse resp = new TokenRequiredUploadResponse
                    {
                        id = Convert.ToInt32(cmd.ExecuteScalar())
                    };
                    return resp;
                }
            }
            else
            {
                using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
                {
                    con.Open();
                    log.Info("#Eb account insert 1");
                    if (request.op == "insertaccount")
                    {
                        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenantaccount SET accountname=@accountname,cid=@cid,address=@address,phone=@phone,email=@email,website=@website,tier=@tier,tenantname=@tenantname WHERE id=@id RETURNING id");
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("accountname", System.Data.DbType.String, request.Colvalues["accountname"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("cid", System.Data.DbType.String, request.Colvalues["cid"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("address", System.Data.DbType.String, request.Colvalues["address"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("phone", System.Data.DbType.String, request.Colvalues["phone"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("email", System.Data.DbType.String, request.Colvalues["email"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("website", System.Data.DbType.String, request.Colvalues["website"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("tier", System.Data.DbType.String, request.Colvalues["tier"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("tenantname", System.Data.DbType.String, request.Colvalues["tenantname"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, request.Colvalues["tenantuserid"]));
                        TokenRequiredUploadResponse resp = new TokenRequiredUploadResponse
                        {
                            id = Convert.ToInt32(cmd.ExecuteScalar())
                        };
                        return resp;

                    }
                    else if (request.op == "Dbcheck")
                    {
                        int uid = 0;
                        string sql = string.Format("SELECT cid,accountname FROM eb_tenantaccount WHERE id={0}", request.Colvalues["acid"]);
                        var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);
                        //CREATE CLIENTDB CONN
                        EbClientConf e = new EbClientConf()
                        {
                            ClientID = dt.Rows[0][0].ToString(),
                            ClientName = dt.Rows[0][1].ToString(),
                            EbClientTier = EbClientTiers.Unlimited
                        };

                        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbOBJECTS, new EbDatabaseConfiguration(EbDatabaseTypes.EbOBJECTS, DatabaseVendors.PGSQL, request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbDATA, new EbDatabaseConfiguration(EbDatabaseTypes.EbDATA, DatabaseVendors.PGSQL, request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbFILES, new EbDatabaseConfiguration(EbDatabaseTypes.EbFILES, DatabaseVendors.PGSQL, request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbLOGS, new EbDatabaseConfiguration(EbDatabaseTypes.EbLOGS, DatabaseVendors.PGSQL, request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbOBJECTS_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbOBJECTS_RO, DatabaseVendors.PGSQL, request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbDATA_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbDATA_RO, DatabaseVendors.PGSQL, request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbFILES_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbFILES_RO, DatabaseVendors.PGSQL, request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
                        e.DatabaseConfigurations.Add(EbDatabaseTypes.EbLOGS_RO, new EbDatabaseConfiguration(EbDatabaseTypes.EbLOGS_RO, DatabaseVendors.PGSQL, request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));

                        byte[] bytea2 = EbSerializers.ProtoBuf_Serialize(e);
                        var dbconf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea2);
                        var dbf = new DatabaseFactory(dbconf);
                        var _con = dbf.ObjectsDB.GetNewConnection();
                        try
                        {
                            _con.Open();
                            var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenantaccount SET config=@config WHERE id=@id RETURNING id");
                            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("config", System.Data.DbType.Binary, bytea2));
                            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, Convert.ToInt32(request.Colvalues["acid"])));
                            uid = Convert.ToInt32(cmd.ExecuteScalar());
                        }
                        catch (Exception ex)
                        {

                        }
                        TokenRequiredUploadResponse resp = new TokenRequiredUploadResponse
                        {
                            id = uid
                        };
                        return resp;
                    }
                    else if (request.op == "updatetenant")
                    {
                        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenants SET company=@company,employees=@employees,country=@country,phone=@phone WHERE id=@id RETURNING id");


                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("company", System.Data.DbType.String, request.Colvalues["company"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("employees", System.Data.DbType.String, request.Colvalues["employees"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("country", System.Data.DbType.String, request.Colvalues["country"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("phone", System.Data.DbType.String, request.Colvalues["phone"]));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, request.Colvalues["id"]));
                        TokenRequiredUploadResponse res = new TokenRequiredUploadResponse
                        {
                            id = Convert.ToInt32(cmd.ExecuteScalar())
                        };
                        return res;
                    }
                    else if (request.op == "tenantimgupload")
                    {
                        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenants SET profileimg=@profileimg WHERE id=@id RETURNING id");
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("profileimg", System.Data.DbType.String, string.Format("<img src='data:image/png;base64,{0}'class='img-circle navbar-right img-cir'/>", request.Colvalues["profileimg"])));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, request.Colvalues["id"]));
                        TokenRequiredUploadResponse res = new TokenRequiredUploadResponse
                        {
                            id = Convert.ToInt32(cmd.ExecuteScalar())
                        };
                        return res;
                    }
                    else if (request.op == "tenantaccountimg")
                    {
                        log.Info("#Eb account image insert 1");
                        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "INSERT INTO eb_tenantaccount (profilelogo,tenantid) VALUES(@profilelogo,@tenantid) RETURNING id");
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("profilelogo", System.Data.DbType.String, string.Format("<img src='{0}' class='prologo img-circle'/>", request.Colvalues["proimg"])));
                        cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("tenantid", System.Data.DbType.Int64, request.Colvalues["id"]));
                        TokenRequiredUploadResponse res = new TokenRequiredUploadResponse
                        {
                            id = Convert.ToInt32(cmd.ExecuteScalar())
                        };
                        return res;
                    }
                    else
                    {
                        TokenRequiredUploadResponse resp = new TokenRequiredUploadResponse
                        {
                            id = 0
                        };
                        return resp;
                    }

                }
            }
        }

        //public bool Any(SendMail request)
        //{
        //    string path = Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).FullName;
        //    var infraconf = EbSerializers.ProtoBuf_DeSerialize<EbInfraDBConf>(EbFile.Bytea_FromFile(Path.Combine(path, "EbInfra.conn")));
        //    var df = new DatabaseFactory(infraconf);
        //    using (var con = df.InfraDB.GetNewConnection())
        //    {
        //        con.Open();
        //        foreach (string key in request.Emailvals.Keys)
        //        {

        //            var cmd = df.InfraDB.GetNewCommand(con, string.Format("SELECT COUNT(*) FROM eb_tenants where cname = @{0}", key));
        //            cmd.Parameters.Add(df.InfraDB.GetNewParameter(string.Format("{0}", key), System.Data.DbType.String, request.Emailvals[key]));
        //            int i = (Convert.ToInt32(cmd.ExecuteScalar()));
        //            if (i > 0)
        //            {
        //                StringBuilder strBody = new StringBuilder();
        //                //Passing emailid,username and generated unique code via querystring. For testing pass your localhost number and while making online pass your domain name instead of localhost path.
        //                strBody.Append("<a href=http://localhost:53125/Tenant/ResetPassword.aspx?emailId=" + request.Emailvals["email"]+">Click here to change your password</a>");
        //                // sbody.Append("&uCode=" + uniqueCode + "&uName=" + txtUserName.Text + ">Click here to change your password</a>");

        //                var message = new MimeMessage();
        //                message.From.Add(new MailboxAddress("ExpressBase Systems", "shasisoman785@gmail.com"));
        //                message.To.Add(new MailboxAddress("", request.Emailvals["email"].ToString()));
        //                message.Subject = "EB Account Created";
        //                message.Body = new TextPart("plain")
        //                {
        //                    Text = strBody.ToString()
        //                };

        //                using (var client = new MailKit.Net.Smtp.SmtpClient())
        //                {
        //                    //client.Connect("smtp.gmail.com", 587, false);
        //                    client.Connect("smtp.gmail.com", 587, false);
        //                    client.AuthenticationMechanisms.Remove("XOAUTH2");

        //                    // Note: since we don't have an OAuth2 token, disable 	// the XOAUTH2 authentication mechanism.     client.Authenticate("anuraj.p@example.com", "password");
        //                    client.Send(message);
        //                    client.Disconnect(true);
        //                }
        //            }

        //            //return false;

        //            else
        //                return true;
        //        }
        //        return false;

        //    }

        //    }

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
                    string sql = string.Format("SELECT id,accountname,profilelogo FROM eb_tenantaccount WHERE tenantid={0}", request.Uid);
                    var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);
                    List<List<object>> list = new List<List<object>>();
                    foreach (EbDataRow dr in dt.Rows)
                    {
                        list.Add(new List<object> { Convert.ToInt32(dr[0]), dr[1].ToString(), dr[2].ToString() });
                    }
                    TokenRequiredSelectResponse resp = new TokenRequiredSelectResponse()
                    {
                        returnlist = list
                    };
                    return resp;
                }


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
