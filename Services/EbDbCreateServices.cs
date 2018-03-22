using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Security;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
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
                if(_dcConnectionFactory == null)
                    _dcConnectionFactory = new EbConnectionFactory(EbConnectionsConfigProvider.DataCenterConnections, CoreConstants.EXPRESSBASE);

                return _dcConnectionFactory;
            }
        }

        public EbDbCreateServices(IEbConnectionFactory _idbf) : base(_idbf) { }

        public EbDbCreateResponse Any(EbDbCreateRequest request)
        {
           
            using (var con = this.DCConnectionFactory.DataDB.GetNewConnection())
            {
                try
                {
                    con.Open();
                    var cmd = this.DCConnectionFactory.DataDB.GetNewCommand(con, string.Format("CREATE DATABASE {0};", request.dbName));
                    cmd.ExecuteNonQuery();

                    return DbOperations(request);
                }

                catch (Exception e)
                {
                    if (e.Data["Code"].ToString() == "42P04")
                        return DbOperations(request);
                }
            }
            return null;
           
        }

        public EbDbCreateResponse DbOperations(EbDbCreateRequest request)
        {

            EbConnectionsConfig _solutionConnections = EbConnectionsConfigProvider.DataCenterConnections;

            _solutionConnections.ObjectsDbConnection.DatabaseName = request.dbName.ToLower();
            _solutionConnections.DataDbConnection.DatabaseName = request.dbName.ToLower();

            using (var con = (new EbConnectionFactory(_solutionConnections, request.TenantAccountId)).DataDB.GetNewConnection())
            {
                con.Open();
                var con_trans = con.BeginTransaction();

                //.............DataDb Tables

                string path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.TableCreate.eb_users.sql";
                bool b1 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.TableCreate.eb_usergroup.sql";
                bool b2 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.TableCreate.eb_roles.sql";
                bool b3 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.TableCreate.eb_userstatus.sql";
                bool b4 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.TableCreate.eb_useranonymous.sql";
                bool b5 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.TableCreate.eb_role2user.sql";
                bool b6 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.TableCreate.eb_role2role.sql";
                bool b7 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.TableCreate.eb_role2permission.sql";
                bool b8 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.TableCreate.eb_user2usergroup.sql";
                bool b9 = CreateOrAlter_Structure(con, path);

                //.............DataDb Functions

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_authenticate_anonymous.sql";
                bool b10 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_authenticate_unified.sql";
                bool b11 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_create_or_update_rbac_manageroles.sql";
                bool b12 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_create_or_update_role.sql";
                bool b13 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_create_or_update_role2role.sql";
                bool b14 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_create_or_update_role2user.sql";
                bool b15 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_createormodifyuserandroles.sql";
                bool b16 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_createormodifyusergroup.sql";
                bool b17 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_getpermissions.sql";
                bool b18 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_getroles.sql";
                bool b19 = CreateOrAlter_Structure(con, path);

                //.............ObjectsDb Tables

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.TableCreate.eb_applications.sql";
                bool b20 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.TableCreate.eb_bots.sql";
                bool b21 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.TableCreate.eb_files.sql";
                bool b22 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.TableCreate.eb_objects.sql";
                bool b23 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.TableCreate.eb_objects_relations.sql";
                bool b24 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.TableCreate.eb_objects_status.sql";
                bool b25 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.TableCreate.eb_objects_ver.sql";
                bool b26 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.TableCreate.eb_objects2application.sql";
                bool b27 = CreateOrAlter_Structure(con, path);

                //.............ObjectsDb Functions

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_botdetails.sql";
                bool b28 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_createbot.sql";
                bool b29 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_change_status.sql";
                bool b30 = CreateOrAlter_Structure( con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_commit.sql";
                bool b31 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_create_major_version.sql";
                bool b32 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_create_minor_version.sql";
                bool b33 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_create_new_object.sql";
                bool b34 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_create_patch_version.sql";
                bool b35 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_exploreobject.sql";
                bool b36 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_getversiontoopen.sql";
                bool b37 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_save.sql";
                bool b38 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_objects_update_dashboard.sql";
                bool b39 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.ObjectsDb.FunctionCreate.eb_update_rel.sql";
                bool b40 = CreateOrAlter_Structure(con, path);

                path = "ExpressBase.Common.SqlScripts.PostGreSql.DataDb.FunctionCreate.eb_assignprivileges.sql";
                bool b43 = CreateOrAlter_Structure(con, path);


                //.....insert into user tables.........
                bool b41 = InsertIntoTables(request, con);

                var b42 = CreateUsers4DataBase(con, request);

                if (b1 & b2 & b3 & b4 & b5 & b6 & b7 & b8 & b9 & b10 & b11 & b12 & b13 & b14 & b15 & b16 & b17 & b18 & b19 & 
                    b20 & b21 & b22 & b23 & b24 & b25 & b26 & b27 & b28 & b29 & b30 & b31 & b32 & b33 & b34 & b35 & b36 & b37 & b38 & b39 & b40 & b41 & b42.resp & b43)
                {
                    con_trans.Commit();
                    return b42;
                }
                else
                    con_trans.Rollback();
            }

            return null;
        }

        public EbDbCreateResponse CreateUsers4DataBase(DbConnection con, EbDbCreateRequest request)
        {
            try
             {
                string AdminPass = HelperFunction.GeneratePassword();
                string ROUserPass = HelperFunction.GeneratePassword();
                string RWUserPass = HelperFunction.GeneratePassword();

                string usersql = "SELECT * FROM eb_assignprivileges('@unameadmin','@adminpass','@unameROUser','@ROpass','@unameRWUser','@RWpass');".Replace("@unameadmin", request.dbName + "_Admin").Replace("@adminpass", AdminPass).Replace("@unameROUser", request.dbName + "_RO").Replace("@ROpass", ROUserPass).Replace("@unameRWUser", request.dbName + "_RW").Replace("@RWpass",RWUserPass);
                var cmd = EbConnectionFactory.DataDB.GetNewCommand(con, usersql);
                //cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("unameadmin", EbDbTypes.String, request.dbName + "_Admin"));
                //cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("adminpass", EbDbTypes.String, AdminPass));
                //cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("unameROUser", EbDbTypes.String, request.dbName + "_RO"));
                //cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("ROpass", EbDbTypes.String, ROUserPass));
                //cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("unameRWUser", EbDbTypes.String, request.dbName + "_RW"));
                //cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("RWpass", EbDbTypes.String, RWUserPass));
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch(Exception e)
                {

                }


                //string sql = @"GRANT ALL PRIVILEGES ON DATABASE ""@dbname"" TO @unameadmin;
                //               GRANT USAGE ON SCHEMA public TO @unameadmin;
                //               GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO @unameadmin;
                //               GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO @unameadmin;
                //               GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameadmin;
                //               GRANT CONNECT ON DATABASE ""@dbname"" TO @unameROUser;
                //               GRANT USAGE ON SCHEMA public TO @unameROUser;
                //               GRANT SELECT ON ALL TABLES IN SCHEMA public TO @unameROUser;
                //               GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO @unameROUser;
                //               GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameROUser;
                //               GRANT CONNECT ON DATABASE ""@dbname"" TO @unameRWUser;
                //               GRANT USAGE ON SCHEMA public TO @unameRWUser;
                //               GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO @unameRWUser;
                //               GRANT SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA public TO @unameRWUser;
                //               GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO @unameRWUser;"
                //                        .Replace("@unameadmin", request.dbName + "_Admin").Replace("@unameROUser", request.dbName + "_RO")
                //                        .Replace("@unameRWUser", request.dbName + "_RW").Replace("@dbname", request.dbName);

                //var cmd1 = EbConnectionFactory.DataDB.GetNewCommand(con, sql);
                //cmd1.ExecuteNonQuery();
                var con1 = new Npgsql.NpgsqlConnection(string.Format("Host=35.200.147.143; Port=5432; Database={0}; Username={1}; Password={2}; SSL Mode=Require; Use SSL Stream=true; Trust Server Certificate=true; Pooling=true; CommandTimeout=500;", request.dbName, request.dbName + "_Admin", AdminPass));
                con1.Open();

                return new EbDbCreateResponse { resp = true,
                    AdminUserName = request.dbName + "_Admin",
                    AdminPassword = AdminPass,
                    ReadOnlyUserName = request.dbName + "_RO",
                    ReadOnlyPassword = ROUserPass,
                    ReadWriteUserName = request.dbName + "_RW",
                    ReadWritePassword = RWUserPass,
                    dbname = request.dbName
                };

            }
            catch (Exception e)
            {
                return null;
            }
            
        }

        public bool CreateOrAlter_Structure(DbConnection con, string path)
        {
            try
            {
                string result = null;

                var assembly = typeof(ExpressBase.Common.Resource).Assembly;

                //.....................create tbls........
                using (Stream stream = assembly.GetManifestResourceStream(path))
                {
                    using (StreamReader reader = new StreamReader(stream))
                        result = reader.ReadToEnd();

                    var cmdtxt1 = EbConnectionFactory.DataDB.GetNewCommand(con, result);
                    cmdtxt1.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                return false;
            }

            return true;
        }

        public bool InsertIntoTables(EbDbCreateRequest request, DbConnection con)
        {
            try
            {
                //.......select details from server tbl eb_usres......... from INFRA
                string sql1 = "SELECT email, pwd, firstname, socialid FROM eb_users WHERE id=@uid";
                DbParameter[] parameter = { this.InfraConnectionFactory.DataDB.GetNewParameter("@uid", EbDbTypes.Int32, request.UserId) };
				var rslt = this.InfraConnectionFactory.DataDB.DoQuery(sql1, parameter);

                //..............insert into client tbl eb_users............ to SOLUTION
                string sql2 = "INSERT INTO eb_users(email, pwd, fullname, socialid) VALUES (@email, @pwd, @firstname, @socialid) RETURNING id;";
                var cmdtxt3 = EbConnectionFactory.DataDB.GetNewCommand(con, sql2);
                cmdtxt3.Parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, rslt.Rows[0][0]));
                cmdtxt3.Parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("pwd", EbDbTypes.String, rslt.Rows[0][1]));
                cmdtxt3.Parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("firstname", EbDbTypes.String, rslt.Rows[0][2]));
                cmdtxt3.Parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("socialid", EbDbTypes.String, rslt.Rows[0][3]));
				var id = Convert.ToInt32(cmdtxt3.ExecuteScalar());

                //.......add role to tenant as a/c owner
                string sql4 = string.Empty;
                foreach (var role in Enum.GetValues(typeof(SystemRoles)))
                {
                    sql4 += string.Format("INSERT INTO eb_role2user(role_id, user_id, createdat) VALUES ({0}, {1}, now());", (int)role, id);
                }

                var cmdtxt5 = EbConnectionFactory.DataDB.GetNewCommand(con, sql4);
                cmdtxt5.ExecuteNonQuery();
            }
            catch (Exception e) { return false; }
            return true;
        }
    }
}
