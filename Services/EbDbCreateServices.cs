using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Security;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Data;
using System.Data.Common;
using System.IO;

namespace ExpressBase.ServiceStack.Services
{
    public class EbDbCreateServices : EbBaseService
    {
        private EbConnectionFactory _dcConnectionFactory = null;

        private EbConnectionFactory DCConnectionFactory
        {
            get
            {
                if (_dcConnectionFactory == null)
                    _dcConnectionFactory = new EbConnectionFactory(EbConnectionsConfigProvider.GetDataCenterConnections(), CoreConstants.EXPRESSBASE);

                return _dcConnectionFactory;
            }
        }

        public EbDbCreateServices(IEbConnectionFactory _idbf) : base(_idbf) { }

        public EbDbCreateResponse Post(EbDbCreateRequest request)
        {
            EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();

            _solutionConnections.ObjectsDbConnection.DatabaseName = request.dbName;

            _solutionConnections.DataDbConnection.DatabaseName = request.dbName;

            EbConnectionFactory NewCon = new EbConnectionFactory(_solutionConnections, request.dbName);

            IDatabase DataDB = null;

            if (!request.ischange)
            {
                DataDB = NewCon.DataDB;
            }
            else
            {
                if (request.DataDBConnection.DatabaseVendor == DatabaseVendors.PGSQL)
                    DataDB = new PGSQLDatabase(request.DataDBConnection);
                else if (request.DataDBConnection.DatabaseVendor == DatabaseVendors.ORACLE)
                    DataDB = new OracleDB(request.DataDBConnection);
                else if (request.DataDBConnection.DatabaseVendor == DatabaseVendors.MYSQL)
                    DataDB = new MySqlDB(request.DataDBConnection);
            }
            using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
            {
                try
                {
                    if (DataDB.Vendor == DatabaseVendors.PGSQL && !request.ischange)
                    {
                        con.Open();
                        var cmd = this.EbConnectionFactory.DataDB.GetNewCommand(con, string.Format("CREATE DATABASE {0};", request.dbName));
                        cmd.ExecuteNonQuery();
                    }
                    return DbOperations(request, DataDB);
                }

                catch (Exception e)
                {
                    Console.WriteLine("Exception: " + e.ToString());
                    //if (e.Data["Code"].ToString() == "42P04")
                    //    return DbOperations(request, DataDB);
                    //else
                    return new EbDbCreateResponse { ResponseStatus = new ResponseStatus { Message = "Database Already exists" } };
                }
            }


        }

        public EbDbCreateResponse DbOperations(EbDbCreateRequest request, IDatabase DataDB)
        {

            using (var con = DataDB.GetNewConnection())
            {
                con.Open();
                var con_trans = con.BeginTransaction();
                string vendor = DataDB.Vendor.ToString();
                try
                {
                    //.............DataDb Tables
                    string path = "ExpressBase.Common.sqlscripts.@vendor.eb_extras.sql".Replace("@vendor", vendor.ToLower());
                    bool b1 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_users.sql".Replace("@vendor", vendor.ToLower());
                    bool b2 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_usergroup.sql".Replace("@vendor", vendor.ToLower());
                    bool b3 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_roles.sql".Replace("@vendor", vendor.ToLower());
                    bool b4 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_userstatus.sql".Replace("@vendor", vendor.ToLower());
                    bool b5 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_useranonymous.sql".Replace("@vendor", vendor.ToLower());
                    bool b6 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_role2user.sql".Replace("@vendor", vendor.ToLower());
                    bool b7 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_role2role.sql".Replace("@vendor", vendor.ToLower());
                    bool b8 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_role2permission.sql".Replace("@vendor", vendor.ToLower());
                    bool b9 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_user2usergroup.sql".Replace("@vendor", vendor.ToLower());
                    bool b10 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_files.sql".Replace("@vendor", vendor.ToLower());
                    bool b23 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_role2location.sql".Replace("@vendor", vendor.ToLower());
                    bool b48 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_query_choices.sql".Replace("@vendor", vendor.ToLower());
                    bool b52 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_survey_lines.sql".Replace("@vendor", vendor.ToLower());
                    bool b53 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_survey_master.sql".Replace("@vendor", vendor.ToLower());
                    bool b54 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_survey_queries.sql".Replace("@vendor", vendor.ToLower());
                    bool b55 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_surveys.sql".Replace("@vendor", vendor.ToLower());
                    bool b56 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_audit_lines.sql".Replace("@vendor", vendor.ToLower());
                    bool b57 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_audit_master.sql".Replace("@vendor", vendor.ToLower());
                    bool b58 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_keys.sql".Replace("@vendor", vendor.ToLower());
                    bool b59 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_keyvalue.sql".Replace("@vendor", vendor.ToLower());
                    bool b60 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_languages.sql".Replace("@vendor", vendor.ToLower());
                    bool b61 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_constraints_datetime.sql".Replace("@vendor", vendor.ToLower());
                    bool b63 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.tablecreate.eb_constraints_ip.sql".Replace("@vendor", vendor.ToLower());
                    bool b64 = CreateOrAlter_Structure(con, path, DataDB);

                    //.............DataDb Functions
                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_authenticate_unified.sql".Replace("@vendor", vendor.ToLower());
                    bool b11 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_authenticate_anonymous.sql".Replace("@vendor", vendor.ToLower());
                    bool b12 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_create_or_update_role.sql".Replace("@vendor", vendor.ToLower());
                    bool b13 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_create_or_update_rbac_roles.sql".Replace("@vendor", vendor.ToLower());
                    bool b14 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_create_or_update_role2role.sql".Replace("@vendor", vendor.ToLower());
                    bool b15 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_create_or_update_role2user.sql".Replace("@vendor", vendor.ToLower());
                    bool b16 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_createormodifyuserandroles.sql".Replace("@vendor", vendor.ToLower());
                    bool b17 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_createormodifyusergroup.sql".Replace("@vendor", vendor.ToLower());
                    bool b18 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_getpermissions.sql".Replace("@vendor", vendor.ToLower());
                    bool b19 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_getroles.sql".Replace("@vendor", vendor.ToLower());
                    bool b20 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_create_or_update_role2loc.sql".Replace("@vendor", vendor.ToLower());
                    bool b49 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.datadb.functioncreate.eb_getconstraintstatus.sql".Replace("@vendor", vendor.ToLower());
                    bool b62 = CreateOrAlter_Structure(con, path, DataDB);

                    //.............ObjectsDb Tables

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_applications.sql".Replace("@vendor", vendor.ToLower());
                    bool b21 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_bots.sql".Replace("@vendor", vendor.ToLower());
                    bool b22 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_objects.sql".Replace("@vendor", vendor.ToLower());
                    bool b24 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_objects_relations.sql".Replace("@vendor", vendor.ToLower());
                    bool b25 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_objects_status.sql".Replace("@vendor", vendor.ToLower());
                    bool b26 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_objects_ver.sql".Replace("@vendor", vendor.ToLower());
                    bool b27 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_objects2application.sql".Replace("@vendor", vendor.ToLower());
                    bool b28 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_google_map.sql".Replace("@vendor", vendor.ToLower());
                    bool b29 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_locations.sql".Replace("@vendor", vendor.ToLower());
                    bool b46 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.tablecreate.eb_location_config.sql".Replace("@vendor", vendor.ToLower());
                    bool b47 = CreateOrAlter_Structure(con, path, DataDB);


                    //.............ObjectsDb Functions

                    //path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_botdetails.sql".Replace("@vendor", vendor);
                    //bool b29 = CreateOrAlter_Structure(con, path);

                    //path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_createbot.sql".Replace("@vendor", vendor);
                    //bool b30 = CreateOrAlter_Structure(con, path);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_objects_change_status.sql".Replace("@vendor", vendor.ToLower());
                    bool b31 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_objects_commit.sql".Replace("@vendor", vendor.ToLower());
                    bool b32 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_object_create_major_version.sql".Replace("@vendor", vendor.ToLower());
                    bool b33 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_object_create_minor_version.sql".Replace("@vendor", vendor.ToLower());
                    bool b34 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_objects_create_new_object.sql".Replace("@vendor", vendor.ToLower());
                    bool b35 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_object_create_patch_version.sql".Replace("@vendor", vendor.ToLower());
                    bool b36 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_objects_exploreobject.sql".Replace("@vendor", vendor.ToLower());
                    bool b37 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_objects_getversiontoopen.sql".Replace("@vendor", vendor.ToLower());
                    bool b38 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_objects_save.sql".Replace("@vendor", vendor.ToLower());
                    bool b39 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_objects_update_dashboard.sql".Replace("@vendor", vendor.ToLower());
                    bool b40 = CreateOrAlter_Structure(con, path, DataDB);

                    //path = "ExpressBase.Common.SqlScripts.@vendor.ObjectsDb.FunctionCreate.eb_update_rel.sql".Replace("@vendor", vendor);
                    //  bool b43 = CreateOrAlter_Structure(con, path, con_trans);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_get_tagged_object.sql".Replace("@vendor", vendor.ToLower());
                    bool b44 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.eb_compilefunctions.sql".Replace("@vendor", vendor.ToLower());
                    bool b45 = CreateOrAlter_Structure(con, path, DataDB);

                    path = "ExpressBase.Common.sqlscripts.@vendor.objectsdb.functioncreate.eb_currval.sql".Replace("@vendor", vendor.ToLower());
                    bool b51 = CreateOrAlter_Structure(con, path, DataDB);

                    //..........files db tables.......................

                    path = "ExpressBase.Common.sqlscripts.@vendor.filesdb.tablecreate.eb_files_bytea.sql".Replace("@vendor", vendor.ToLower());
                    bool b50 = CreateOrAlter_Structure(con, path, DataDB);

                    //.....insert into user tables.........
                    bool b41 = InsertIntoTables(request, con, DataDB);

                    var b42 = request.ischange ? null : CreateUsers4DataBase(con, request, DataDB);

                    if (b1 & b2 & b3 & b4 & b5 & b6 & b7 & b8 & b9 & b10 & b11 & b12 & b13 & b14 & b15 & b16 & b17 & b18 & b19 &
                        b20 & b21 & b22 & b23 & b24 & b25 & b26 & b27 & b28 & b29 & b31 & b32 & b33 & b34 & b35 & b36 & b37 & b38 & b39 & b40 & b41 & b44 & b45 & b46 & b47 &
                        b48 & b49 & b50 & b51 & b52 & b53 & b54 & b55 & b56 & b57 & b58 & b59 & b60 & b61 & b62 & b63 & b64)
                    {
                        Console.WriteLine(".............Reached Commit");
                        con_trans.Commit();
                        var success = request.ischange ? new EbDbCreateResponse() { resp = true } : b42;
                        return success;
                    }
                    else
                        con_trans.Rollback();
                }
                catch (Exception e)
                {
                    con_trans.Rollback();
                    throw new Exception(e.Message);
                }

            }

            return null;
        }

        public EbDbCreateResponse CreateUsers4DataBase(DbConnection con, EbDbCreateRequest request, IDatabase DataDB)
        {
            try
            {

                string usersql = "SELECT * FROM eb_assignprivileges('@unameadmin','@unameROUser','@unameRWUser');".Replace("@unameadmin", request.dbName + "_admin").Replace("@unameROUser", request.dbName + "_ro").Replace("@unameRWUser", request.dbName + "_rw");

                var dt = this.InfraConnectionFactory.DataDB.DoQuery(usersql);


                string sql = @"REVOKE connect ON DATABASE ""@dbname"" FROM PUBLIC;
                               GRANT ALL PRIVILEGES ON DATABASE ""@dbname"" TO @ebadmin;                   
                               GRANT ALL PRIVILEGES ON DATABASE ""@dbname"" TO @unameadmin;      
                               GRANT CONNECT ON DATABASE ""@dbname"" TO @unameROUser;     
                               GRANT CONNECT ON DATABASE ""@dbname"" TO @unameRWUser;
                              ".Replace("@unameadmin", request.dbName + "_admin").Replace("@unameROUser", request.dbName + "_ro")
                               .Replace("@unameRWUser", request.dbName + "_rw").Replace("@dbname", request.dbName).Replace("@ebadmin", Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER));

                var grnt = this.InfraConnectionFactory.DataDB.DoNonQuery(sql);

                string sql2 = @"GRANT USAGE ON SCHEMA public TO @ebadmin;
                            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO @ebadmin;
                            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO @ebadmin;
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @ebadmin;
                            GRANT USAGE ON SCHEMA public TO @unameadmin;   
                            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO @unameadmin;
                            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO @unameadmin;
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameadmin;
                            REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM @unameROUser;
                            GRANT USAGE ON SCHEMA public TO @unameROUser;
                            GRANT SELECT ON ALL TABLES IN SCHEMA public TO @unameROUser;
                            GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO @unameROUser;
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameROUser;
                            GRANT USAGE ON SCHEMA public TO @unameRWUser;
                            GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO @unameRWUser;
                            GRANT SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA public TO @unameRWUser;
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameRWUser;"
                                    .Replace("@unameadmin", request.dbName + "_admin").Replace("@unameROUser", request.dbName + "_ro")
                                    .Replace("@unameRWUser", request.dbName + "_rw").Replace("@ebadmin", Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER));

                var cmdtxt = DataDB.GetNewCommand(con, sql2);
                cmdtxt.ExecuteNonQuery();

                //return new EbDbCreateResponse
                //{
                //    resp = true,
                //    AdminUserName = request.dbName + "_admin",
                //    AdminPassword = dt.Rows[0][0].ToString(),
                //    ReadOnlyUserName = request.dbName + "_ro",
                //    ReadOnlyPassword = dt.Rows[0][1].ToString(),
                //    ReadWriteUserName = request.dbName + "_rw",
                //    ReadWritePassword = dt.Rows[0][2].ToString(),
                //    dbname = request.dbName
                //};
                var ebdbusers = new EbDbUsers
                {
                    AdminUserName = request.dbName + "_admin",
                    AdminPassword = dt.Rows[0][0].ToString(),
                    ReadOnlyUserName = request.dbName + "_ro",
                    ReadOnlyPassword = dt.Rows[0][1].ToString(),
                    ReadWriteUserName = request.dbName + "_rw",
                    ReadWritePassword = dt.Rows[0][2].ToString(),
                };
                return new EbDbCreateResponse
                {
                    resp = true,
                    dbname = request.dbName,
                    dbusers = ebdbusers
                };
            }
            catch (Exception e)
            {
                Console.WriteLine(".............problem in CreateUsers4DataBase: " + e.ToString());
                return null;
            }
        }

        public bool CreateOrAlter_Structure(DbConnection con, string path, IDatabase DataDB)
        {
            try
            {

                string result = null;

                var assembly = typeof(sqlscripts).Assembly;

                //.....................create tbls........
                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    using (StreamReader reader = new StreamReader(stream))
                        result = reader.ReadToEnd();

                    var cmdtxt1 = DataDB.GetNewCommand(con, result);
                    cmdtxt1.ExecuteNonQuery();

                }
            }
            catch (Exception e)
            {
                //return false;
                Console.WriteLine("Exception: " + e.ToString());
                throw new Exception("Already Exists");
            }

            return true;
        }

        public bool InsertIntoTables(EbDbCreateRequest request, DbConnection con, IDatabase DataDB)
        {
            try
            {
                //.......select details from server tbl eb_usres......... from INFRA
                string sql1 = "SELECT email, pwd, fullname,fb_id FROM eb_tenants WHERE id=:uid";
                DbParameter[] parameter = { this.InfraConnectionFactory.DataDB.GetNewParameter("uid", EbDbTypes.Int32, request.UserId) };
                var rslt = this.InfraConnectionFactory.DataDB.DoQuery(sql1, parameter);

                //..............insert into client tbl eb_users............ to SOLUTION
                string sql2 = @"INSERT INTO eb_users(email,pwd) VALUES ('anonymous@anonym.com','294de3557d9d00b3d2d8a1e6aab028cf'); 
                                INSERT INTO eb_locations(shortname,longname) VALUES ('default','default');";

                string sql3 = string.Empty;
                foreach (var role in Enum.GetValues(typeof(SystemRoles)))
                {
                    int rid = (int)role;
                    sql3 += DataDB.EB_INITROLE2USER.Replace("@role_id", rid.ToString()).Replace("@user_id", "2");
                }

                if (DataDB.Vendor == DatabaseVendors.PGSQL)
                {
                    sql2 += "INSERT INTO eb_users(email, pwd, fullname,fbid) VALUES (:email, :pwd, :fullname, :socialid)";
                    var cmdtxt3 = DataDB.GetNewCommand(con, sql2);
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("fullname", EbDbTypes.String, rslt.Rows[0][2]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3]));
                    cmdtxt3.ExecuteScalar();
                    var cmdtxt5 = DataDB.GetNewCommand(con, sql3);
                    cmdtxt5.ExecuteNonQuery();
                }
                else if (DataDB.Vendor == DatabaseVendors.ORACLE)
                {
                    sql2 += "INSERT INTO eb_users(email, pwd, fullname,fbid) VALUES (:email, :pwd, :fullname, :socialid)";
                    DbParameter[] parameters = { DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]),
                                                 DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]),
                                                 DataDB.GetNewParameter("fullname", EbDbTypes.String, rslt.Rows[0][2]),
                                                 DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3])
                                                  };
                    var result1 = DataDB.DoNonQuery(sql2, parameters);
                    var result2 = DataDB.DoNonQuery(sql3);

                }
                if (DataDB.Vendor == DatabaseVendors.MYSQL)
                {
                    sql2 += "INSERT INTO eb_users(email, pwd, fullname,fbid) VALUES (@email, @pwd, @fullname, @socialid);";
                    var cmdtxt3 = DataDB.GetNewCommand(con, sql2);
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("fullname", EbDbTypes.String, rslt.Rows[0][2]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3]));
                    cmdtxt3.ExecuteScalar();
                    var cmdtxt5 = DataDB.GetNewCommand(con, sql3);
                    cmdtxt5.ExecuteNonQuery();
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
                Console.WriteLine(".............problem in InsertIntoTables");
                return false;
            }
            return true;
        }
    }
}
