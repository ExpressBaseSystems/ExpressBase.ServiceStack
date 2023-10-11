using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Security;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.MQServices;
using ServiceStack;
using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
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
            IDatabase DataDB = null;

            try
            {
                EbDbUsers ebdbusers = null;
                if (request.IsChange)
                {
                    if (request.DataDBConfig.DatabaseVendor == DatabaseVendors.PGSQL)
                        DataDB = new PGSQLDatabase(request.DataDBConfig);
                    else if (request.DataDBConfig.DatabaseVendor == DatabaseVendors.ORACLE)
                        DataDB = new OracleDB(request.DataDBConfig);
                    else if (request.DataDBConfig.DatabaseVendor == DatabaseVendors.MYSQL)
                        DataDB = new MySqlDB(request.DataDBConfig);
                }
                else
                {
                    EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();

                    DataDB = new EbConnectionFactory(_solutionConnections, request.DBName).DataDB;
                    using (DbConnection con = DataDB.GetNewConnection())
                    {
                        con.Open();
                        using (DbCommand cmd = DataDB.GetNewCommand(con, string.Format("CREATE DATABASE {0};", request.DBName)))
                        {
                            int id = cmd.ExecuteNonQuery();
                            if (id > 0)
                            {
                                Console.WriteLine("...........Created Database " + request.DBName);
                            }
                            //_solutionConnections.DataDbConfig.DatabaseName = request.DBName;
                            //DataDB = new EbConnectionFactory(_solutionConnections, request.DBName).DataDB;
                            string usersql = string.Format("SELECT * FROM eb_assignprivileges('{0}_admin','{0}_ro','{0}_rw');", request.DBName);
                            // EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(usersql);
                            EbDataTable dt = DataDB.DoQuery(usersql);
                            string extension = "";
                            if (_solutionConnections.DataDbConfig.UserName.Split('@').Length > 1)
                                extension = "@" + _solutionConnections.DataDbConfig.UserName.Split('@')[1];
                            ebdbusers = new EbDbUsers
                            {
                                AdminUserName = request.DBName + "_admin" + extension,
                                AdminPassword = dt.Rows[0][0].ToString(),
                                ReadOnlyUserName = request.DBName + "_ro" + extension,
                                ReadOnlyPassword = dt.Rows[0][1].ToString(),
                                ReadWriteUserName = request.DBName + "_rw" + extension,
                                ReadWritePassword = dt.Rows[0][2].ToString(),
                            };
                            EbConnectionsConfig _dcConnections = EbConnectionsConfigProvider.GetDataCenterConnections();
                            _dcConnections.DataDbConfig.DatabaseName = request.DBName;
                            _dcConnections.DataDbConfig.UserName = ebdbusers.AdminUserName;
                            _dcConnections.DataDbConfig.Password = ebdbusers.AdminPassword;
                            DataDB = new EbConnectionFactory(_dcConnections, request.DBName).DataDB;
                        }
                    }
                }

                return DbOperations(request, ebdbusers, DataDB);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
                return new EbDbCreateResponse { ResponseStatus = new ResponseStatus { Message = ErrorTexConstants.DB_ALREADY_EXISTS } };
            }
        }

        public EbDbCreateResponse DbOperations(EbDbCreateRequest request, EbDbUsers ebDbUsers, IDatabase DataDB)
        {
            Console.WriteLine("Reached DbOperations");

            using (DbConnection con = DataDB.GetNewConnection())
            {
                con.Open();

                DbTransaction con_trans = con.BeginTransaction();

                string vendor = DataDB.Vendor.ToString();
                bool IsCreateComplete = false;
                bool IsInsertComplete = false;
                try
                {
                    string[] _filepath = SqlFiles.SQLSCRIPTS;
                    Console.WriteLine(".............Reached CreateOrAlter_Structure. Total Files: " + _filepath.Length);

                    int counter = 0;


                    string Urlstart = string.Format("ExpressBase.Common.sqlscripts.{0}.", vendor.ToLower());
                    foreach (string path in _filepath)
                    {
                        counter++;
                        Console.WriteLine(counter);

                        IsCreateComplete = CreateOrAlter_Structure(con, Urlstart + path, DataDB);
                        if (!IsCreateComplete)
                            break;
                    }
                    if (IsCreateComplete)
                    {
                        IsInsertComplete = InsertIntoTables(request, con, DataDB, request.SolutionType);
                    }

                    EbDbCreateResponse _res = request.IsChange ? null : AssignDBUserPrivileges(con, request.DBName, DataDB);

                    if (IsCreateComplete & IsInsertComplete)
                    {
                        Console.WriteLine(".............Reached Transaction Commit");
                        con_trans.Commit();
                        EbDbCreateResponse success = request.IsChange ? new EbDbCreateResponse() { DeploymentCompled = true } : _res;
                        success.DbUsers = ebDbUsers;
                        if (!request.IsChange && !request.IsFurther)
                        {   //run northwind
                            RunNorthWindScript(request.DBName, ebDbUsers);
                            //import the application 129
                        }
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

        public EbDbCreateResponse AssignDBUserPrivileges(DbConnection con, string _dbname, IDatabase DataDB)
        {
            try
            {
                EbConnectionsConfig _dcConnections = EbConnectionsConfigProvider.GetDataCenterConnections();
                _dcConnections.DataDbConfig.DatabaseName = _dbname;
                IDatabase DataCenterDataDB = new EbConnectionFactory(_dcConnections, _dbname).DataDB;

                using (DbConnection con_p = DataCenterDataDB.GetNewConnection())
                {

                    con_p.Open();
                    string sql = string.Format(@"REVOKE connect ON DATABASE ""{0}"" FROM PUBLIC;
                               GRANT ALL PRIVILEGES ON DATABASE ""{0}"" TO {1};                   
                               GRANT ALL PRIVILEGES ON DATABASE ""{0}"" TO {0}_admin;
                               GRANT CONNECT ON DATABASE ""{0}"" TO {1};
                               GRANT CONNECT ON DATABASE ""{0}"" TO {0}_admin;
                               GRANT CONNECT ON DATABASE ""{0}"" TO {0}_ro;     
                               GRANT CONNECT ON DATABASE ""{0}"" TO {0}_rw;", _dbname,
                                   Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER).Split('@')[0]);

                    int grnt = DataCenterDataDB.DoNonQuery(sql);

                    string sql2 = string.Format(@"GRANT ALL PRIVILEGES ON SCHEMA public TO {1};                           
                            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {1};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO {1};
                            ALTER DEFAULT PRIVILEGES FOR ROLE {1} IN SCHEMA public GRANT ALL ON TABLES TO {0}_admin;
                            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {1};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO {1};
                            ALTER DEFAULT PRIVILEGES FOR ROLE {1} IN SCHEMA public GRANT ALL ON SEQUENCES TO {0}_admin;
                            GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO {1};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON FUNCTIONS TO {1};
                            ALTER DEFAULT PRIVILEGES FOR ROLE {1} IN SCHEMA public GRANT ALL ON FUNCTIONS TO {0}_admin;
                      GRANT ALL PRIVILEGES ON SCHEMA public TO {0}_admin; 
                            GRANT {0}_admin to {1};
                            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {0}_admin;                            
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO {0}_admin;                            
                            ALTER DEFAULT PRIVILEGES FOR ROLE {0}_admin IN SCHEMA public GRANT ALL ON TABLES TO {1};
                            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {0}_admin;
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO {0}_admin;
                            ALTER DEFAULT PRIVILEGES FOR ROLE {0}_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO {1};
                            GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO {0}_admin;
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON FUNCTIONS TO {0}_admin; 
                            ALTER DEFAULT PRIVILEGES FOR ROLE {0}_admin IN SCHEMA public GRANT ALL ON FUNCTIONS TO {1};                            
                            REVOKE ALL ON SCHEMA public FROM public;
                            REVOKE ALL ON DATABASE {0} FROM PUBLIC;
                      REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {0}_ro;
                            REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {0}_ro; 
                            REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM {0}_ro; 
                            GRANT USAGE ON SCHEMA public TO {0}_ro;
                            GRANT {0}_ro to {0}_admin;                            
                            GRANT SELECT ON ALL TABLES IN SCHEMA public TO {0}_ro;
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {0}_ro;
                            ALTER DEFAULT PRIVILEGES FOR ROLE {0}_admin IN SCHEMA public GRANT SELECT ON TABLES TO {0}_ro;
                            GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {0}_ro;
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO {0}_ro;
                            ALTER DEFAULT PRIVILEGES FOR ROLE {0}_admin IN SCHEMA public GRANT USAGE ON SEQUENCES TO {0}_ro;
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {0}_ro;
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO {0}_ro; 
                            ALTER DEFAULT PRIVILEGES FOR ROLE {0}_admin IN SCHEMA public GRANT USAGE ON SEQUENCES TO {0}_ro;
                       REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {0}_rw;
                            REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {0}_rw;
                            REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM {0}_rw;
                            GRANT USAGE ON SCHEMA public TO {0}_rw;
                            GRANT {0}_rw to {0}_admin;                            
                            GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO {0}_rw;
                            ALTER DEFAULT PRIVILEGES FOR ROLE {0}_admin IN SCHEMA public GRANT SELECT,INSERT,UPDATE ON TABLES TO {0}_rw;
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,UPDATE ON TABLES TO {0}_rw;
                            GRANT SELECT,UPDATE,USAGE ON ALL SEQUENCES IN SCHEMA public TO {0}_rw;
                            ALTER DEFAULT PRIVILEGES FOR ROLE {0}_admin IN SCHEMA public GRANT SELECT,UPDATE,USAGE ON SEQUENCES TO {0}_rw;
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,UPDATE,USAGE ON SEQUENCES TO {0}_rw;                            
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {0}_rw;
                            ALTER DEFAULT PRIVILEGES FOR ROLE {0}_admin IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO {0}_rw;
                            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO {0}_rw;", _dbname,
                                Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER).Split('@')[0]
                                );

                    using (DbCommand cmdtxt = DataCenterDataDB.GetNewCommand(con_p, sql2))
                    {
                        cmdtxt.ExecuteNonQuery();
                    }
                }
                return new EbDbCreateResponse
                {
                    DeploymentCompled = true,
                    DbName = _dbname
                };
            }
            catch (Exception e)
            {
                Console.WriteLine(".............problem in AssignDBUserPrivileges: " + e.ToString());
                throw e;
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
                    if (stream != null)
                        using (StreamReader reader = new StreamReader(stream))
                            result = reader.ReadToEnd();
                    else
                    {
                        Console.WriteLine(" Reading reference - stream is null -" + path);
                        return true;
                    }
                    using (var cmdtxt1 = DataDB.GetNewCommand(con, result))
                    {
                        cmdtxt1.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception e)
            {
                //return false;
                Console.WriteLine("Exception: " + path + e.Message + e.StackTrace);
                throw new Exception("Already Exists");
            }

            return true;
        }

        public bool InsertIntoTables(EbDbCreateRequest request, DbConnection con, IDatabase DataDB, SolutionType SolutionType)
        {
            try
            {
                //.......select details from server tbl eb_usres......... from INFRA
                string sql1 = "SELECT email, pwd, fullname,fb_id FROM eb_tenants WHERE id=:uid";
                DbParameter[] parameter = { this.InfraConnectionFactory.DataDB.GetNewParameter("uid", EbDbTypes.Int32, request.UserId) };
                EbDataTable rslt = this.InfraConnectionFactory.DataDB.DoQuery(sql1, parameter);

                //..............insert into client tbl eb_users............ to SOLUTION
                string sql2 = @"INSERT INTO eb_users(email,pwd,statusid) VALUES ('anonymous@anonym.com','294de3557d9d00b3d2d8a1e6aab028cf',0); 
                                INSERT INTO eb_locations(shortname, longname, eb_location_types_id, parent_id, is_group) VALUES ('default', 'default', 1, 0, 'T');
                                INSERT INTO eb_languages(code, name, display_name, eb_row_num, eb_del) VALUES ('en', 'English', 'ENG', 1, 'F');";
                if (SolutionType != SolutionType.REPLICA)
                    sql2 += @" INSERT INTO eb_user_types(name,eb_del ) VALUES ('default','F');
                           INSERT INTO eb_location_types(type) VALUES ('default');";

                string sql3 = string.Empty;
                foreach (var role in Enum.GetValues(typeof(SystemRoles)))
                {
                    int rid = (int)role;
                    sql3 += DataDB.EB_INITROLE2USER.Replace("@role_id", rid.ToString()).Replace("@user_id", "2");
                }

                if (DataDB.Vendor == DatabaseVendors.PGSQL)
                {
                    sql2 += "INSERT INTO eb_users(email, pwd, fullname,fbid,statusid) VALUES (:email, :pwd, :fullname, :socialid, 0)";
                    using (DbCommand cmdtxt3 = DataDB.GetNewCommand(con, sql2))
                    {
                        cmdtxt3.Parameters.Add(DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]));
                        cmdtxt3.Parameters.Add(DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]));
                        cmdtxt3.Parameters.Add(DataDB.GetNewParameter("fullname", EbDbTypes.String, rslt.Rows[0][2]));
                        cmdtxt3.Parameters.Add(DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3]));
                        cmdtxt3.ExecuteScalar();
                    }
                    using (DbCommand cmdtxt5 = DataDB.GetNewCommand(con, sql3))
                    {
                        cmdtxt5.ExecuteNonQuery();
                    }
                }
                else if (DataDB.Vendor == DatabaseVendors.ORACLE)
                {
                    sql2 += "INSERT INTO eb_users(email, pwd, fullname,fbid,statusid) VALUES (:email, :pwd, :fullname, :socialid, 0)";
                    DbParameter[] parameters = { DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]),
                                                 DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]),
                                                 DataDB.GetNewParameter("fullname", EbDbTypes.String, rslt.Rows[0][2]),
                                                 DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3])
                                                  };
                    int result1 = DataDB.DoNonQuery(sql2, parameters);
                    int result2 = DataDB.DoNonQuery(sql3);

                }
                if (DataDB.Vendor == DatabaseVendors.MYSQL)
                {
                    sql2 += "INSERT INTO eb_users(email, pwd, fullname,fbid,statusid) VALUES (@email, @pwd, @fullname, @socialid, 0);";
                    using (DbCommand cmdtxt3 = DataDB.GetNewCommand(con, sql2))
                    {
                        cmdtxt3.Parameters.Add(DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]));
                        cmdtxt3.Parameters.Add(DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]));
                        cmdtxt3.Parameters.Add(DataDB.GetNewParameter("fullname", EbDbTypes.String, rslt.Rows[0][2]));
                        cmdtxt3.Parameters.Add(DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3]));
                        cmdtxt3.ExecuteScalar();
                    }
                    using (DbCommand cmdtxt5 = DataDB.GetNewCommand(con, sql3))
                    {
                        cmdtxt5.ExecuteNonQuery();
                    }
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

        public void RunNorthWindScript(string DBName, EbDbUsers dbusers)
        {
            Console.WriteLine("Executing northwind_script");
            EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.GetDataCenterConnections();
            _solutionConnections.DataDbConfig.DatabaseName = DBName;
            _solutionConnections.DataDbConfig.Password = dbusers.AdminPassword;
            _solutionConnections.DataDbConfig.UserName = dbusers.AdminUserName;

            IDatabase DataDB = new EbConnectionFactory(_solutionConnections, DBName).DataDB;
            using (DbConnection con = DataDB.GetNewConnection())
            {
                con.Open();
                CreateOrAlter_Structure(con, "ExpressBase.Common.sqlscripts.pgsql.eb_northwind_script.sql", DataDB);
            }
        }
    }
}
