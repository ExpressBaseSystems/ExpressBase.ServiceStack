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
            IDatabase DataDB = null;

            try
            {
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
                    DbConnection con = DataDB.GetNewConnection();
                    con.Open();
                    DbCommand cmd = DataDB.GetNewCommand(con, string.Format("CREATE DATABASE {0};", request.DBName));
                    cmd.ExecuteNonQuery();

                    _solutionConnections.DataDbConfig.DatabaseName = request.DBName;
                    DataDB = new EbConnectionFactory(_solutionConnections, request.DBName).DataDB;
                }

                return DbOperations(request, DataDB);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
                return new EbDbCreateResponse { ResponseStatus = new ResponseStatus { Message = "Database Already exists" } };
            }
        }

        public EbDbCreateResponse DbOperations(EbDbCreateRequest request, IDatabase DataDB)
        {

            using (DbConnection con = DataDB.GetNewConnection())
            {
                con.Open();
                DbTransaction con_trans = con.BeginTransaction();
                string vendor = DataDB.Vendor.ToString();
                bool IsCreateComplete = false;
                bool IsInsertComplete = false;
                try
                {
                    string[] filePaths = Directory.GetFiles(string.Format("../ExpressBase.Common/sqlscripts/{0}", vendor.ToLower()),
                        "*.sql",
                        SearchOption.AllDirectories);
                    foreach (string path in filePaths)
                    {
                        IsCreateComplete = CreateOrAlter_Structure(con, path, DataDB);
                        if (!IsCreateComplete)
                            break;
                    }
                    if (IsCreateComplete)
                    {
                        IsInsertComplete = InsertIntoTables(request, con, DataDB);
                    }
                    EbDbCreateResponse _res = request.IsChange ? null : CreateUsers4DataBase(con, request.DBName, DataDB);

                    if (IsCreateComplete & IsInsertComplete)
                    {
                        Console.WriteLine(".............Reached Commit");
                        con_trans.Commit();
                        EbDbCreateResponse success = request.IsChange ? new EbDbCreateResponse() { Resp = true } : _res;
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

        public EbDbCreateResponse CreateUsers4DataBase(DbConnection con, string _dbname, IDatabase DataDB)
        {
            try
            {
                //string usersql = "SELECT * FROM eb_assignprivileges('@unameadmin','@unameROUser','@unameRWUser');".Replace("@unameadmin", _dbname + "_admin").Replace("@unameROUser", _dbname + "_ro").Replace("@unameRWUser", _dbname + "_rw");
                string usersql = string.Format("SELECT * FROM eb_assignprivileges('{0}_admin','{0}_ro','{0}_rw');", _dbname);
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(usersql);

                string sql = string.Format(@"REVOKE connect ON DATABASE ""{0}"" FROM PUBLIC;
                               GRANT ALL PRIVILEGES ON DATABASE ""{0}"" TO {1};                   
                               GRANT ALL PRIVILEGES ON DATABASE ""{0}"" TO {0}_admin;      
                               GRANT CONNECT ON DATABASE ""{0}"" TO {0}_ro;     
                               GRANT CONNECT ON DATABASE ""{0}"" TO {0}_rw;", _dbname,
                               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER));

                //@"REVOKE connect ON DATABASE ""@dbname"" FROM PUBLIC;
                //               GRANT ALL PRIVILEGES ON DATABASE ""@dbname"" TO @ebadmin;                   
                //               GRANT ALL PRIVILEGES ON DATABASE ""@dbname"" TO @unameadmin;      
                //               GRANT CONNECT ON DATABASE ""@dbname"" TO @unameROUser;     
                //               GRANT CONNECT ON DATABASE ""@dbname"" TO @unameRWUser;
                //              ".Replace("@unameadmin", _dbname + "_admin").Replace("@unameROUser", _dbname + "_ro")
                //               .Replace("@unameRWUser", _dbname + "_rw").Replace("@dbname", _dbname).Replace("@ebadmin", Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER));

                int grnt = this.InfraConnectionFactory.DataDB.DoNonQuery(sql);

                string sql2 = string.Format(@"GRANT USAGE ON SCHEMA public TO {1};
                            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {1};
                            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {1};
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {1};
                            GRANT USAGE ON SCHEMA public TO {0}_admin;   
                            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {0}_admin;
                            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {0}_admin;
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {0}_admin;
                            REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {0}_ro;
                            GRANT USAGE ON SCHEMA public TO {0}_ro;
                            GRANT SELECT ON ALL TABLES IN SCHEMA public TO {0}_ro;
                            GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {0}_ro;
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {0}_ro;
                            GRANT USAGE ON SCHEMA public TO {0}_rw;
                            GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO {0}_rw;
                            GRANT SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA public TO {0}_rw;
                            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {0}_rw;", _dbname,
                            Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER)
                            );

                //@"GRANT USAGE ON SCHEMA public TO @ebadmin;
                //            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO @ebadmin;
                //            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO @ebadmin;
                //            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @ebadmin;
                //            GRANT USAGE ON SCHEMA public TO @unameadmin;   
                //            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO @unameadmin;
                //            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO @unameadmin;
                //            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameadmin;
                //            REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM @unameROUser;
                //            GRANT USAGE ON SCHEMA public TO @unameROUser;
                //            GRANT SELECT ON ALL TABLES IN SCHEMA public TO @unameROUser;
                //            GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO @unameROUser;
                //            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameROUser;
                //            GRANT USAGE ON SCHEMA public TO @unameRWUser;
                //            GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO @unameRWUser;
                //            GRANT SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA public TO @unameRWUser;
                //            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameRWUser;"
                //                    .Replace("@unameadmin", _dbname + "_admin").Replace("@unameROUser", _dbname + "_ro")
                //                    .Replace("@unameRWUser", _dbname + "_rw").Replace("@ebadmin", Environment.GetEnvironmentVariable(EnvironmentConstants.EB_DATACENTRE_ADMIN_USER));

                DbCommand cmdtxt = DataDB.GetNewCommand(con, sql2);
                cmdtxt.ExecuteNonQuery();

                EbDbUsers ebdbusers = new EbDbUsers
                {
                    AdminUserName = _dbname + "_admin",
                    AdminPassword = dt.Rows[0][0].ToString(),
                    ReadOnlyUserName = _dbname + "_ro",
                    ReadOnlyPassword = dt.Rows[0][1].ToString(),
                    ReadWriteUserName = _dbname + "_rw",
                    ReadWritePassword = dt.Rows[0][2].ToString(),
                };
                return new EbDbCreateResponse
                {
                    Resp = true,
                    DbName = _dbname,
                    DbUsers = ebdbusers
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
                path = path.Replace("../", "").Replace("/", ".").Replace("\\", ".");
                var assembly = typeof(sqlscripts).Assembly;

                //.....................create tbls........
                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    if (stream != null)
                        using (StreamReader reader = new StreamReader(stream))
                            result = reader.ReadToEnd();
                    else
                    {
                        Console.WriteLine("Exception: " + " Reading reference - stream is null");
                        return false;
                    }
                    var cmdtxt1 = DataDB.GetNewCommand(con, result);
                    cmdtxt1.ExecuteNonQuery();

                }
            }
            catch (Exception e)
            {
                //return false;
                Console.WriteLine("Exception: " + path + e.ToString());
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
                EbDataTable rslt = this.InfraConnectionFactory.DataDB.DoQuery(sql1, parameter);

                //..............insert into client tbl eb_users............ to SOLUTION
                string sql2 = @"INSERT INTO eb_users(email,pwd,statusid) VALUES ('anonymous@anonym.com','294de3557d9d00b3d2d8a1e6aab028cf',0); 
                                INSERT INTO eb_locations(shortname,longname) VALUES ('default','default');";

                string sql3 = string.Empty;
                foreach (var role in Enum.GetValues(typeof(SystemRoles)))
                {
                    int rid = (int)role;
                    sql3 += DataDB.EB_INITROLE2USER.Replace("@role_id", rid.ToString()).Replace("@user_id", "2");
                }

                if (DataDB.Vendor == DatabaseVendors.PGSQL)
                {
                    sql2 += "INSERT INTO eb_users(email, pwd, fullname,fbid,statusid) VALUES (:email, :pwd, :fullname, :socialid, 0)";
                    DbCommand cmdtxt3 = DataDB.GetNewCommand(con, sql2);
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("fullname", EbDbTypes.String, rslt.Rows[0][2]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3]));
                    cmdtxt3.ExecuteScalar();
                    DbCommand cmdtxt5 = DataDB.GetNewCommand(con, sql3);
                    cmdtxt5.ExecuteNonQuery();
                }
                else if (DataDB.Vendor == DatabaseVendors.ORACLE)
                {
                    sql2 += "INSERT INTO eb_users(email, pwd, fullname,fbid,statusid) VALUES (:email, :pwd, :fullname, :socialid,0)";
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
                    sql2 += "INSERT INTO eb_users(email, pwd, fullname,fbid,statusid) VALUES (@email, @pwd, @fullname, @socialid,0);";
                    DbCommand cmdtxt3 = DataDB.GetNewCommand(con, sql2);
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("fullname", EbDbTypes.String, rslt.Rows[0][2]));
                    cmdtxt3.Parameters.Add(DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3]));
                    cmdtxt3.ExecuteScalar();
                    DbCommand cmdtxt5 = DataDB.GetNewCommand(con, sql3);
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
